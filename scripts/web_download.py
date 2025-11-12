"""
Step 4：Playwright 网页自动化
- 打开 https://li.feishu.cn/next/messenger
- 扫码登录（若已登录则直接进入）
- 进入“飞书合同”机器人会话
- 解析最新消息中的“协商数据（共计：XXX）”
- 点击“下载文件”，保存到 download_dir
- 返回 (保存路径, declared_count)
"""
from typing import Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import os
import re
import time
import logging
import hashlib


MESSENGER_URL = "https://li.feishu.cn/next/messenger"
logger = logging.getLogger(__name__)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    try:
        logger.debug("ensure_dir: %s", path)
    except Exception:
        pass


def _parse_declared_count(text: str) -> Optional[int]:
    # 示例文案：协商数据（共计：1000）已导出至...
    m = re.search(r"共计\D*(\d+)", text)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


class BrowserSession:
    def __init__(self, cfg: dict) -> None:
        dcfg = cfg.get("download", {})
        self.user_data_dir = dcfg.get("user_data_dir", "./.browser_profile")
        self.download_dir = dcfg.get("download_dir", "./output/raw")
        self.bot_chat_name = dcfg.get("bot_chat_name", "飞书合同")
        self.max_wait_seconds = int(dcfg.get("max_wait_seconds", 90))
        _ensure_dir(self.download_dir)
        self._p = sync_playwright().start()
        self._context = self._p.chromium.launch_persistent_context(
            user_data_dir=self.user_data_dir,
            headless=False,
            accept_downloads=True,
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(30_000)
        self.ensure_chat_open()

    def ensure_chat_open(self) -> None:
        page = self._page
        page.goto(MESSENGER_URL)
        time.sleep(2)
        try:
            locator = page.get_by_role("link", name=self.bot_chat_name)
            if locator.count() == 0:
                locator = page.get_by_text(self.bot_chat_name)
            locator.first.click()
        except Exception:
            pass

    def snapshot_download_button_count(self) -> int:
        try:
            return self._page.get_by_text("下载文件").count()
        except Exception:
            return 0

    def _stable_count(self, samples: int = 3, interval: float = 0.4) -> int:
        last = -1
        stable_times = 0
        for _ in range(max(2, samples)):
            try:
                cur = self._page.get_by_text("下载文件").count()
            except Exception:
                cur = 0
            if cur == last:
                stable_times += 1
                if stable_times >= 1:
                    return cur
            else:
                stable_times = 0
                last = cur
            time.sleep(max(0.05, interval))
        return max(0, last)

    def _text_near(self, btn_locator) -> str:
        txt = ""
        try:
            txt = btn_locator.evaluate(
                "el => { let n=el; for(let i=0;i<5 && n; i++){ if(n.innerText && n.innerText.includes('共计')) return n.innerText; n=n.parentElement;} return ''; }"
            ) or ""
        except Exception:
            txt = ""
        if not txt:
            try:
                txt = self._page.locator("xpath=(//*[contains(text(),'共计')])[last()]").inner_text()
            except Exception:
                txt = ""
        return txt or ""

    def _tail_signature(self) -> str:
        try:
            button = self._page.get_by_text("下载文件")
            cnt = button.count()
        except Exception:
            return ""
        if cnt <= 0:
            return ""
        btn_last = button.nth(cnt - 1)
        txt = self._text_near(btn_last)
        norm = re.sub(r"\s+", " ", txt or "").strip()
        if not norm:
            return ""
        try:
            return hashlib.md5(norm.encode("utf-8")).hexdigest()[:10]
        except Exception:
            return norm[:16]

    def snapshot_state(self) -> Tuple[int, str]:
        cnt = self._stable_count()
        sig = self._tail_signature() if cnt > 0 else ""
        return cnt, sig

    def _declared_near(self, btn_locator) -> int:
        text_near = ""
        try:
            text_near = btn_locator.evaluate(
                "el => { let n=el; for(let i=0;i<5 && n; i++){ if(n.innerText && n.innerText.includes('共计')) return n.innerText; n=n.parentElement;} return ''; }"
            ) or ""
        except Exception:
            text_near = ""
        if not text_near:
            try:
                text_near = self._page.locator("xpath=(//*[contains(text(),'共计')])[last()]").inner_text()
            except Exception:
                text_near = ""
        return int(_parse_declared_count(text_near) or 0)

    def wait_and_download_new(self, pre_count: int, pre_sig: str = "") -> Optional[Tuple[str, int]]:
        page = self._page
        start_ts = time.time()
        while time.time() - start_ts < self.max_wait_seconds:
            try:
                cnt, sig = self.snapshot_state()
                if cnt > pre_count or (pre_sig and sig and sig != pre_sig):
                    break
            except PlaywrightTimeoutError:
                pass
            time.sleep(0.8)
        try:
            cnt_now, sig_now = self.snapshot_state()
        except Exception:
            cnt_now, sig_now = pre_count, ""
        should = (cnt_now > pre_count) or (pre_sig and sig_now and sig_now != pre_sig)
        if not should:
            return None
        try:
            button = page.get_by_text("下载文件")
        except Exception:
            return None
        if cnt_now <= 0:
            return None
        idx = max(0, cnt_now - 1)
        btn_last = button.nth(idx)
        save_path: Optional[str] = None
        try:
            with page.expect_download() as dl_info:
                btn_last.click()
            download = dl_info.value
            suggested = download.suggested_filename
            dest = os.path.join(self.download_dir, suggested)
            download.save_as(dest)
            save_path = dest
        except Exception:
            save_path = None
        if not save_path:
            return None
        declared = self._declared_near(btn_last)
        return (save_path, declared)

    def close(self) -> None:
        try:
            self._context.close()
        except Exception:
            pass
        try:
            self._p.stop()
        except Exception:
            pass


def login_and_download(cfg: dict) -> Optional[Tuple[str, int]]:
    dcfg = cfg.get("download", {})
    user_data_dir = dcfg.get("user_data_dir", "./.browser_profile")
    download_dir = dcfg.get("download_dir", "./output/raw")
    bot_chat_name = dcfg.get("bot_chat_name", "飞书合同")
    max_wait_seconds = int(dcfg.get("max_wait_seconds", 90))

    _ensure_dir(download_dir)

    with sync_playwright() as p:
        logger.debug(
            "playwright: launch context user_data_dir=%s headless=%s accept_downloads=%s",
            user_data_dir,
            False,
            True,
        )
        context = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=False,
            accept_downloads=True,
        )
        page = context.new_page()
        page.set_default_timeout(30_000)

        # 进入飞书网页版
        logger.debug("playwright: goto %s", MESSENGER_URL)
        page.goto(MESSENGER_URL)

        # 进入会话列表或等待手动扫码完成
        # 若需要登录，用户手动扫码；此处仅等待页面稳定
        time.sleep(2)

        # 定位机器人会话
        try:
            # 优先使用 role=link/name 匹配
            locator = page.get_by_role("link", name=bot_chat_name)
            if locator.count() == 0:
                locator = page.get_by_text(bot_chat_name)
            logger.debug("playwright: click chat name=%s count=%s", bot_chat_name, locator.count())
            locator.first.click()
        except Exception:
            # 若无法定位，提示用户手动点击到机器人会话，并继续后续步骤
            logger.warning("playwright: cannot auto-locate chat '%s', please click it manually", bot_chat_name)

        # 轮询等待“下载文件”按钮出现
        start_ts = time.time()
        declared_count: Optional[int] = None
        button = page.get_by_text("下载文件")
        while time.time() - start_ts < max_wait_seconds:
            try:
                if button.count() > 0:
                    logger.debug("playwright: found download button count=%s", button.count())
                    break
            except PlaywrightTimeoutError:
                pass
            time.sleep(1)

        if button.count() == 0:
            # 无消息，返回 None 供上层处理为 no_data
            context.close()
            logger.info("playwright: no download button within %ss, return None", max_wait_seconds)
            return None

        # 取最新一条“下载文件”按钮
        idx = button.count() - 1
        btn_last = button.nth(idx)

        # 尝试解析同一消息容器中的文案，提取共计数
        text_near = ""
        try:
            text_near = btn_last.evaluate(
                "el => { let n=el; for(let i=0;i<5 && n; i++){ if(n.innerText && n.innerText.includes('共计')) return n.innerText; n=n.parentElement;} return ''; }"
            ) or ""
        except Exception:
            text_near = ""

        if not text_near:
            # 兜底：全局找最新包含“共计”的元素文本（可能不精确，但可用于计数）
            try:
                text_near = page.locator("xpath=(//*[contains(text(),'共计')])[last()]").inner_text()
            except Exception:
                text_near = ""
        declared_count = _parse_declared_count(text_near) or 0
        logger.debug("playwright: parsed declared_count=%s text_len=%s", declared_count, len(text_near))

        # 捕获下载事件并保存到 download_dir
        save_path: Optional[str] = None
        try:
            with page.expect_download() as dl_info:
                btn_last.click()
            download = dl_info.value
            suggested = download.suggested_filename
            dest = os.path.join(download_dir, suggested)
            logger.debug("playwright: save download suggested=%s dest=%s", suggested, dest)
            download.save_as(dest)
            save_path = dest
        except Exception:
            logger.exception("playwright: download failed")
            save_path = None

        context.close()

        if save_path and declared_count is not None:
            logger.info("playwright: success path=%s declared=%s", save_path, int(declared_count))
            return (save_path, int(declared_count))
        logger.info("playwright: return None (save_path or declared_count missing)")
        return None
