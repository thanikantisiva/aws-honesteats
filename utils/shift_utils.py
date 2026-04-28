"""Shift timing utilities for restaurant and menu item availability evaluation."""
from datetime import datetime, time
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

# Ordered day abbreviations (match DynamoDB stored values)
_DAY_ORDER = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]


def _parse_hhmm(hhmm: str) -> time:
    """Parse 'HH:MM' string to a time object. Raises ValueError on bad input."""
    h, m = hhmm.split(":")
    return time(int(h), int(m))


def _current_hhmm_and_day(timezone: str) -> Tuple[str, str]:
    """Return (HH:MM string, 3-letter day abbreviation) in the given IANA timezone."""
    now = datetime.now(ZoneInfo(timezone))
    day = now.strftime("%A")[:3].upper()  # "MON", "TUE", etc.
    hhmm = now.strftime("%H:%M")
    return hhmm, day


def _hhmm_in_shift(current: str, start: str, end: str) -> bool:
    """
    Return True if current HH:MM falls in [start, end).

    Handles midnight-crossing shifts: if start > end (e.g. 22:00 – 02:00),
    the shift is split into two ranges: [start, 24:00) and [00:00, end).
    """
    try:
        t_now = _parse_hhmm(current)
        t_start = _parse_hhmm(start)
        t_end = _parse_hhmm(end)
    except (ValueError, TypeError):
        return False

    if t_start <= t_end:
        # Normal range, e.g. 11:00 – 15:00
        return t_start <= t_now < t_end
    else:
        # Midnight-crossing range, e.g. 22:00 – 02:00
        return t_now >= t_start or t_now < t_end


def is_in_shift(
    shift_timings: Optional[List[dict]],
    timezone: str = "Asia/Kolkata",
) -> bool:
    """
    Return True if the current time falls within any of the defined shifts.

    Args:
        shift_timings: List of shift schedule dicts with shape:
            {
              "days": ["MON", "TUE", ...],   # optional; absent = every day
              "shifts": [
                  {"label": "Lunch", "start": "11:00", "end": "15:00"},
                  ...
              ]
            }
        timezone: IANA timezone string (e.g. "Asia/Kolkata"). Always sourced
                  from the restaurant document — never from the menu item.

    Returns:
        True if there are no shift restrictions (shift_timings is None / empty),
        or if the current time matches at least one configured shift.
    """
    if not shift_timings:
        return True  # No restrictions — always available

    current_hhmm, current_day = _current_hhmm_and_day(timezone)

    for schedule in shift_timings:
        days: List[str] = schedule.get("days") or []
        # If days is absent or empty → applies to every day
        if days and current_day not in days:
            continue
        for shift in schedule.get("shifts", []):
            start = shift.get("start", "")
            end = shift.get("end", "")
            if _hhmm_in_shift(current_hhmm, start, end):
                return True

    return False


def get_shift_label(
    shift_timings: Optional[List[dict]],
    timezone: str = "Asia/Kolkata",
) -> Optional[str]:
    """Return the label of the currently active shift, or None if not in any shift."""
    if not shift_timings:
        return None

    current_hhmm, current_day = _current_hhmm_and_day(timezone)

    for schedule in shift_timings:
        days: List[str] = schedule.get("days") or []
        if days and current_day not in days:
            continue
        for shift in schedule.get("shifts", []):
            if _hhmm_in_shift(current_hhmm, shift.get("start", ""), shift.get("end", "")):
                return shift.get("label")

    return None


def get_next_shift_opens_at(
    shift_timings: Optional[List[dict]],
    timezone: str = "Asia/Kolkata",
) -> Optional[str]:
    """
    Return HH:MM of the next upcoming shift start time (same day, earliest future),
    or None if there are no upcoming shifts today / no restrictions defined.
    """
    if not shift_timings:
        return None

    current_hhmm, current_day = _current_hhmm_and_day(timezone)

    upcoming: List[str] = []

    for schedule in shift_timings:
        days: List[str] = schedule.get("days") or []
        if days and current_day not in days:
            continue
        for shift in schedule.get("shifts", []):
            start = shift.get("start", "")
            if start > current_hhmm:
                upcoming.append(start)

    return min(upcoming) if upcoming else None


def validate_shift_timings(shift_timings: list) -> Optional[str]:
    """
    Validate the shift_timings list structure.

    Returns an error message string if invalid, or None if valid.
    """
    if not isinstance(shift_timings, list):
        return "shiftTimings must be an array"

    valid_days = set(_DAY_ORDER)

    for idx, schedule in enumerate(shift_timings):
        if not isinstance(schedule, dict):
            return f"shiftTimings[{idx}] must be an object"

        days = schedule.get("days")
        if days is not None:
            if not isinstance(days, list):
                return f"shiftTimings[{idx}].days must be an array"
            invalid = [d for d in days if d not in valid_days]
            if invalid:
                return f"shiftTimings[{idx}].days contains invalid values: {invalid}. Use MON/TUE/WED/THU/FRI/SAT/SUN"

        shifts = schedule.get("shifts")
        if not isinstance(shifts, list) or not shifts:
            return f"shiftTimings[{idx}].shifts must be a non-empty array"

        for s_idx, shift in enumerate(shifts):
            if not isinstance(shift, dict):
                return f"shiftTimings[{idx}].shifts[{s_idx}] must be an object"
            for field in ("start", "end"):
                val = shift.get(field)
                if not isinstance(val, str):
                    return f"shiftTimings[{idx}].shifts[{s_idx}].{field} is required and must be a string"
                try:
                    _parse_hhmm(val)
                except (ValueError, TypeError):
                    return f"shiftTimings[{idx}].shifts[{s_idx}].{field} must be in HH:MM format (got '{val}')"

    return None
