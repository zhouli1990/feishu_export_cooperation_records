from __future__ import annotations
from typing import List, Tuple, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import hashlib
import logging

from .run_state import RunStateConfig, from_config as rs_from_cfg, ensure_csv_exists, read_completed_window_ids, append_record
from .window_gen import generate_initial_windows
from .http_export import submit_export
from .web_download import BrowserSession

logger = logging.getLogger(__name__)

def _window_id(fr: str, to: str) -> str:
    return f"{fr.replace('-', '')}-{to.replace('-', '')}"


def _window_days(fr: str, to: str) -> int:
    fmt = "%Y-%m-%d"
    d1 = datetime.strptime(fr, fmt)
    d2 = datetime.strptime(to, fmt)
    return (d2 - d1).days + 1


def _md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _record(
    fr: str,
    to: str,
    status: str,
    declared_count: int,
    split_level: int,
    retries: int,
    file_path: str = "",
    file_md5: str = "",
    skip_reason: str = "",
    exception: str = "",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    start_iso = start_time.isoformat() if start_time else ""
    end_iso = end_time.isoformat() if end_time else ""
    duration_ms = 0
    if start_time and end_time:
        duration_ms = int((end_time - start_time).total_seconds() * 1000)
    return {
        "window_id": _window_id(fr, to),
        "from_date": fr,
        "to_date": to,
        "window_days": _window_days(fr, to),
        "status": status,
        "declared_count": declared_count,
        "split_level": split_level,
        "retries": retries,
        "skip_reason": skip_reason,
        "exception": exception,
        "file_path": file_path,
        "file_md5": file_md5,
        "start_time": start_iso,
        "end_time": end_iso,
        "duration_ms": duration_ms,
    }


def _rename_to_standard(download_dir: str, fr: str, to: str, count: int, saved_path: str) -> str:
    # 统一命名：合同协同_YYYYMMDD-YYYYMMDD_共{COUNT}条.xlsx
    base = f"合同协同_{fr.replace('-', '')}-{to.replace('-', '')}_共{count}条.xlsx"
    dest = str(Path(download_dir) / base)
    try:
        if Path(saved_path).resolve() != Path(dest).resolve():
            logger.debug("rename: %s -> %s", saved_path, dest)
            Path(saved_path).rename(dest)
            return dest
    except Exception:
        # 保留原名
        return saved_path
    return dest


def _process_leaf_window(cfg: dict, fr: str, to: str, split_level: int, rs_cfg: RunStateConfig, session: BrowserSession) -> None:
    # 导出并下载
    start_ts = datetime.now()
    logger.debug("leaf_window: start fr=%s to=%s level=%s", fr, to, split_level)
    try:
        session.ensure_chat_open()
    except Exception:
        pass
    dcfg = cfg.get("download", {})
    clear_each = bool(dcfg.get("clear_chat_each_round", False))
    if clear_each:
        try:
            session.clear_chat_history()
        except Exception:
            pass
    try:
        pre_count, pre_sig = session.snapshot_state()
    except Exception:
        pre_count, pre_sig = 0, ""
    export_result = submit_export(cfg, fr, to)
    if not export_result.get("ok"):
        # 失败写入记录
        logger.warning("leaf_window: export failed fr=%s to=%s err=%s", fr, to, export_result.get("error") or export_result.get("status_code"))
        append_record(rs_cfg, _record(fr, to, "failed", 0, split_level, retries=int(cfg.get("retry", {}).get("max_attempts", 3)),
                                       exception=str(export_result.get("error") or export_result.get("status_code")),
                                       start_time=start_ts, end_time=datetime.now()))
        return

    dl = session.wait_and_download_new(pre_count, pre_sig)
    end_ts = datetime.now()
    if dl is None:
        # 无消息
        logger.info("leaf_window: no_data fr=%s to=%s", fr, to)
        append_record(rs_cfg, _record(fr, to, "no_data", 0, split_level, retries=0, start_time=start_ts, end_time=end_ts))
        return

    saved_path, declared = dl
    logger.debug("leaf_window: declared=%s path=%s", declared, saved_path)
    # 阈值判断
    max_count = int(cfg.get("max_count_per_file", 1000))
    if declared == max_count and _window_days(fr, to) == 1:
        logger.info("leaf_window: over_limit_1d fr=%s to=%s declared=%s", fr, to, declared)
        append_record(rs_cfg, _record(fr, to, "manual", declared, split_level, retries=0,
                                       exception="over_limit_1d", file_path=saved_path,
                                       file_md5=_md5_file(saved_path) if Path(saved_path).exists() else "",
                                       start_time=start_ts, end_time=end_ts))
        try:
            if bool(cfg.get("download", {}).get("clear_chat_each_round", False)):
                session.clear_chat_history()
        except Exception:
            pass
        return

    if declared < max_count:
        # 重命名并记录 with_data
        download_dir = cfg.get("download", {}).get("download_dir", "./output/raw")
        std_path = _rename_to_standard(download_dir, fr, to, declared, saved_path)
        logger.info("leaf_window: with_data fr=%s to=%s declared=%s saved=%s", fr, to, declared, std_path)
        append_record(rs_cfg, _record(fr, to, "with_data", declared, split_level, retries=0,
                                       file_path=std_path,
                                       file_md5=_md5_file(std_path) if Path(std_path).exists() else "",
                                       start_time=start_ts, end_time=end_ts))
        try:
            if bool(cfg.get("download", {}).get("clear_chat_each_round", False)):
                session.clear_chat_history()
        except Exception:
            pass
        return

    # declared == max_count 且 window_days>1 的情况在上层处理（触发细分），此处不记录父窗口
    return


def _split_and_process(cfg: dict, fr: str, to: str, seq: List[int], level: int, rs_cfg: RunStateConfig, session: BrowserSession) -> None:
    days = seq[level]
    max_count = int(cfg.get("max_count_per_file", 1000))

    # 先执行一次以判断是否需要细分
    start_ts = datetime.now()
    logger.debug("split_process: start fr=%s to=%s level=%s days=%s", fr, to, level, days)
    try:
        session.ensure_chat_open()
    except Exception:
        pass
    dcfg = cfg.get("download", {})
    clear_each = bool(dcfg.get("clear_chat_each_round", False))
    if clear_each:
        try:
            session.clear_chat_history()
        except Exception:
            pass
    try:
        pre_count, pre_sig = session.snapshot_state()
    except Exception:
        pre_count, pre_sig = 0, ""
    exp = submit_export(cfg, fr, to)
    if not exp.get("ok"):
        logger.warning("split_process: export failed fr=%s to=%s err=%s", fr, to, exp.get("error") or exp.get("status_code"))
        append_record(rs_cfg, _record(fr, to, "failed", 0, level, retries=int(cfg.get("retry", {}).get("max_attempts", 3)),
                                       exception=str(exp.get("error") or exp.get("status_code")),
                                       start_time=start_ts, end_time=datetime.now()))
        return

    dl = session.wait_and_download_new(pre_count, pre_sig)
    end_ts = datetime.now()
    if dl is None:
        logger.info("split_process: no_data fr=%s to=%s", fr, to)
        append_record(rs_cfg, _record(fr, to, "no_data", 0, level, retries=0, start_time=start_ts, end_time=end_ts))
        return

    saved_path, declared = dl
    logger.debug("split_process: declared=%s path=%s", declared, saved_path)

    if declared == max_count and days > 1:
        # 细分为下一个级别
        next_level = level + 1
        next_days = seq[next_level]
        logger.info("split_process: need split -> next_level=%s next_days=%s fr=%s to=%s", next_level, next_days, fr, to)
        # 父窗口已下载的文件不保留，避免混淆
        try:
            if saved_path and Path(saved_path).exists():
                Path(saved_path).unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
        try:
            if bool(cfg.get("download", {}).get("clear_chat_each_round", False)):
                session.clear_chat_history()
        except Exception:
            pass
        sub_windows = generate_initial_windows(fr, to, next_days)
        logger.debug("split_process: sub_windows=%s", sub_windows)
        for sub_fr, sub_to in sub_windows:
            _split_and_process(cfg, sub_fr, sub_to, seq, next_level, rs_cfg, session)
        return

    if declared == max_count and days == 1:
        # 1天仍超限 → manual
        logger.info("split_process: over_limit_1d fr=%s to=%s declared=%s", fr, to, declared)
        append_record(rs_cfg, _record(fr, to, "manual", declared, level, retries=0,
                                       exception="over_limit_1d", file_path=saved_path,
                                       file_md5=_md5_file(saved_path) if Path(saved_path).exists() else "",
                                       start_time=start_ts, end_time=end_ts))
        return

    # declared < max_count → with_data
    download_dir = cfg.get("download", {}).get("download_dir", "./output/raw")
    std_path = _rename_to_standard(download_dir, fr, to, declared, saved_path)
    logger.info("split_process: with_data fr=%s to=%s declared=%s saved=%s", fr, to, declared, std_path)
    append_record(rs_cfg, _record(fr, to, "with_data", declared, level, retries=0,
                                   file_path=std_path,
                                   file_md5=_md5_file(std_path) if Path(std_path).exists() else "",
                                   start_time=start_ts, end_time=end_ts))
    try:
        if bool(cfg.get("download", {}).get("clear_chat_each_round", False)):
            session.clear_chat_history()
    except Exception:
        pass


def run(cfg: dict) -> None:
    # 准备 run_state
    rs_cfg = rs_from_cfg(cfg)
    ensure_csv_exists(rs_cfg)

    # 断点续跑：跳过已完成窗口
    completed = read_completed_window_ids(rs_cfg, rs_cfg.completed_statuses or ["with_data", "no_data", "manual"])

    # 初始窗口
    seq: List[int] = list(cfg.get("split_days_sequence", [7, 3, 1]))
    initial_days = int(seq[0]) if seq else 7
    initial_windows = generate_initial_windows(cfg.get("start_date"), cfg.get("end_date"), initial_days)
    logger.info("run: initial_windows=%s", initial_windows)

    session = BrowserSession(cfg)
    try:
        # 逐窗口处理
        for fr, to in initial_windows:
            wid = _window_id(fr, to)
            if wid in completed and (rs_cfg.resume_mode or "resume") == "resume":
                logger.debug("run: skip completed window_id=%s fr=%s to=%s", wid, fr, to)
                continue
            _split_and_process(cfg, fr, to, seq, 0, rs_cfg, session)
    finally:
        try:
            session.close()
        except Exception:
            pass
