"""Shared time-of-day window helpers (used by COD + coupon availability configs).

Windows are expressed as 24h ``"HH:MM"`` strings and evaluated against a
minute-of-day integer. Overnight windows (start > end, e.g. 21:00-06:00) are
supported.
"""
from typing import Optional


def parse_hhmm(value) -> Optional[int]:
    """Parse a 24h ``"HH:MM"`` string into minutes-since-midnight.

    Returns None for absent / malformed / out-of-range values.
    """
    if not isinstance(value, str):
        return None
    parts = value.strip().split(":")
    if len(parts) != 2:
        return None
    try:
        hours, minutes = int(parts[0]), int(parts[1])
    except (TypeError, ValueError):
        return None
    if not (0 <= hours <= 23 and 0 <= minutes <= 59):
        return None
    return hours * 60 + minutes


def within_window(start: Optional[int], end: Optional[int], now_minutes: Optional[int]) -> bool:
    """True if ``now_minutes`` falls inside [start, end).

    No/incomplete window (any bound None, or start == end) means "no restriction"
    and returns True. Supports overnight wrap when start > end.
    """
    if now_minutes is None or start is None or end is None or start == end:
        return True
    if start < end:
        return start <= now_minutes < end
    return now_minutes >= start or now_minutes < end
