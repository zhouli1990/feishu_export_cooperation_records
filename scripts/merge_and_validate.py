from typing import List, Tuple
from pathlib import Path
import pandas as pd
import csv
import warnings


def _load_with_data(csv_path: Path, encoding: str = "utf-8-sig") -> Tuple[List[str], int]:
    files: List[str] = []
    total = 0
    if not csv_path.exists():
        return files, total
    with csv_path.open("r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("status") or "").strip() != "with_data":
                continue
            fp = (row.get("file_path") or "").strip()
            try:
                dc = int((row.get("declared_count") or "0").strip() or 0)
            except ValueError:
                dc = 0
            if fp:
                files.append(fp)
            total += dc
    return files, total


def _merge_files(files: List[str], out_path: Path) -> Path:
    if not files:
        return out_path
    frames: List[pd.DataFrame] = []
    for fp in files:
        p = Path(fp)
        if not p.exists():
            continue
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="Workbook contains no default style, apply openpyxl's default",
                    category=UserWarning,
                    module="openpyxl.styles.stylesheet",
                )
                df = pd.read_excel(p, engine="openpyxl")
            frames.append(df)
        except Exception:
            continue
    if not frames:
        return out_path
    # 对齐列并纵向合并
    all_cols = []
    for df in frames:
        for c in df.columns:
            if c not in all_cols:
                all_cols.append(c)
    frames = [df.reindex(columns=all_cols) for df in frames]
    merged = pd.concat(frames, ignore_index=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style, apply openpyxl's default",
            category=UserWarning,
            module="openpyxl.styles.stylesheet",
        )
        merged.to_excel(out_path, index=False)
    return out_path


def merge_run_state(cfg: dict) -> Path:
    rs = cfg.get("run_state", {})
    csv_path = Path(rs.get("csv_path", "./state/run_windows.csv"))
    encoding = rs.get("encoding", "utf-8-sig")
    files, total = _load_with_data(csv_path, encoding=encoding)

    merge_cfg = cfg.get("merge", {})
    pattern = merge_cfg.get("output_path_pattern", "./output/merged/合同协同_汇总.xlsx")
    out_path = Path(pattern.replace("{TOTAL}", str(total)))
    return _merge_files(files, out_path)
