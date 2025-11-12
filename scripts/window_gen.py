"""
时间窗口生成 - Step 2 实现
仅生成初始窗口列表（闭区间），细分与推进逻辑在后续步骤实现。
"""
from typing import List, Tuple
from datetime import datetime, timedelta


def _parse(d: str) -> datetime:
    return datetime.strptime(d, "%Y-%m-%d")


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def generate_initial_windows(start_date: str, end_date: str, days: int) -> List[Tuple[str, str]]:
    """生成[start_date, end_date]闭区间内、步长为days的窗口列表。

    Args:
        start_date: YYYY-MM-DD（含）
        end_date: YYYY-MM-DD（含）
        days: 每个窗口的天数（如7）
    Returns:
        List[Tuple[from_date, to_date]]，均为字符串YYYY-MM-DD，且闭区间
    """
    if days <= 0:
        raise ValueError("days 必须为正整数")

    start = _parse(start_date)
    end = _parse(end_date)
    if start > end:
        return []

    windows: List[Tuple[str, str]] = []
    cur = start
    step = timedelta(days=days)
    while cur <= end:
        win_start = cur
        win_end = cur + timedelta(days=days - 1)
        if win_end > end:
            win_end = end
        windows.append((_fmt(win_start), _fmt(win_end)))
        cur = win_end + timedelta(days=1)

    return windows
