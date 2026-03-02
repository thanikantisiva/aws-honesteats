"""IST (Indian Standard Time) helpers for storing dates in the database."""
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def now_ist_iso() -> str:
    """Current time in IST as ISO 8601 string (e.g. 2025-03-02T14:30:00+05:30)."""
    return datetime.now(IST).isoformat()


def now_ist_strftime(fmt: str) -> str:
    """Current time in IST formatted by fmt (e.g. %Y%m%d-%H%M%S for file keys)."""
    return datetime.now(IST).strftime(fmt)


def epoch_ms_to_ist_iso(ms: int) -> str:
    """Convert Unix milliseconds to IST ISO 8601 string."""
    return datetime.fromtimestamp(ms / 1000.0, tz=IST).isoformat()
