from dataclasses import dataclass
from pathlib import Path
from typing import List, Set, Dict, Any
import csv

HEADERS: List[str] = [
    "window_id",
    "from_date",
    "to_date",
    "window_days",
    "status",
    "declared_count",
    "split_level",
    "retries",
    "skip_reason",
    "exception",
    "file_path",
    "file_md5",
    "start_time",
    "end_time",
    "duration_ms",
]


@dataclass
class RunStateConfig:
    csv_path: str
    encoding: str = "utf-8-sig"
    line_ending: str = "crlf"
    flush: bool = True
    resume_mode: str = "resume"
    completed_statuses: List[str] = None  # type: ignore


def from_config(root_cfg: dict) -> RunStateConfig:
    rs = root_cfg.get("run_state", {})
    return RunStateConfig(
        csv_path=rs.get("csv_path", "./state/run_windows.csv"),
        encoding=rs.get("encoding", "utf-8-sig"),
        line_ending=rs.get("line_ending", "crlf"),
        flush=rs.get("flush", True),
        resume_mode=rs.get("resume_mode", "resume"),
        completed_statuses=rs.get("completed_statuses", ["with_data", "no_data", "manual"]),
    )


def headers() -> List[str]:
    return list(HEADERS)


def _lineterminator(cfg: RunStateConfig) -> str:
    return "\r\n" if cfg.line_ending.lower() == "crlf" else "\n"


def ensure_csv_exists(cfg: RunStateConfig) -> None:
    p = Path(cfg.csv_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        with p.open("w", encoding=cfg.encoding, newline="") as f:
            writer = csv.writer(f, lineterminator=_lineterminator(cfg))
            writer.writerow(HEADERS)
            f.flush()


def read_completed_window_ids(cfg: RunStateConfig, completed_statuses: List[str]) -> Set[str]:
    p = Path(cfg.csv_path)
    if not p.exists():
        return set()
    result: Set[str] = set()
    with p.open("r", encoding=cfg.encoding, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = (row.get("status") or "").strip()
            wid = (row.get("window_id") or "").strip()
            if wid and status in set(completed_statuses):
                result.add(wid)
    return result


def append_record(cfg: RunStateConfig, row: Dict[str, Any]) -> None:
    p = Path(cfg.csv_path)
    ensure_csv_exists(cfg)
    values: List[Any] = [row.get(k, "") for k in HEADERS]
    enc = cfg.encoding
    with p.open("a", encoding=enc, newline="") as f:
        writer = csv.writer(f, lineterminator=_lineterminator(cfg))
        writer.writerow(values)
        if cfg.flush:
            f.flush()
