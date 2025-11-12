import argparse
import sys
from pathlib import Path
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

try:
    import yaml  # type: ignore
except Exception as e:  # pragma: no cover
    logging.error("[提示] 需要依赖 pyyaml。请先运行: pip install -r requirements.txt")
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parents[1]
# 确保可以以脚本方式运行并导入 scripts 包
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.orchestrator import run as orchestrator_run  # noqa: E402
from scripts.merge_and_validate import merge_run_state  # noqa: E402


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        logging.error(f"[错误] 未找到配置文件: {config_path}")
        sys.exit(1)
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def summarize(cfg: dict) -> str:
    rs = cfg.get("run_state", {})
    dl = cfg.get("download", {})
    time_range = f"{cfg.get('start_date')} → {cfg.get('end_date')}"
    summary = [
        f"时间范围: {time_range}",
        f"拆分序列: {cfg.get('split_days_sequence')}",
        f"阈值: max_count_per_file={cfg.get('max_count_per_file')}",
        f"断点续跑: {rs.get('resume_mode','resume')} (已完成态: {rs.get('completed_statuses')})",
        f"下载目录: {dl.get('download_dir')}  机器人: {dl.get('bot_chat_name')}  超时: {dl.get('max_wait_seconds')}s",
    ]
    return "\n".join(summary)


def setup_logging(cfg: dict) -> str:
    base_dir = Path(__file__).resolve().parents[1]
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    level_name = str(cfg.get("log", {}).get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(fmt)
    root.addHandler(ch)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"run_{ts}.log"
    fh = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)
    return str(log_file)


def main() -> None:
    parser = argparse.ArgumentParser(description="飞书合同协同记录批量拉取 - 运行入口")
    parser.add_argument(
        "--config",
        "-c",
        default=str(Path("config") / "config.yaml"),
        help="配置文件路径 (默认: config/config.yaml)",
    )
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parents[1]
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = base_dir / config_path

    cfg = load_config(config_path)

    # 确保必要目录存在
    (base_dir / Path(cfg.get("download", {}).get("download_dir", "./output/raw")).as_posix()).mkdir(parents=True, exist_ok=True)
    (base_dir / "output" / "merged").mkdir(parents=True, exist_ok=True)
    (base_dir / "state").mkdir(parents=True, exist_ok=True)
    (base_dir / "logs").mkdir(parents=True, exist_ok=True)

    log_file = setup_logging(cfg)
    logging.info("[启动] 飞书合同协同记录批量拉取")
    logging.info("日志文件: %s", log_file)
    logging.info("%s", summarize(cfg))

    # Step 5：编排执行（遍历窗口、细分、导出、下载、CSV记录）
    logging.info("[执行] 开始编排（Step 5）...")
    orchestrator_run(cfg)
    logging.info("[完成] 编排结束。状态已写入 state/run_windows.csv")

    # Step 6：合并与输出
    logging.info("[执行] 开始合并（Step 6）...")
    merged_path = merge_run_state(cfg)
    logging.info("[完成] 合并输出: %s", merged_path)


if __name__ == "__main__":
    main()
