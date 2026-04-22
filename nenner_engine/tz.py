"""Single source of truth for Eastern Time handling.

Every time-sensitive code path in NennerEngine (schedulers, watchdogs,
Fischer expiry math, close-breach evaluation) is anchored to US Eastern
Time because that's the market the engine trades. Prior to this module,
ET was reconstructed inline in five different places with inconsistent
behavior — most notably a UTC-5 fallback in email_scheduler.py that
was silently wrong for 8 months a year during Daylight Saving Time.

Usage
-----
    from nenner_engine.tz import ET, now_et, today_et
    scheduled_at = datetime(2026, 4, 21, 16, 30, tzinfo=ET)
    if now_et().weekday() in (0, 2, 4): ...
    today_str = today_et().isoformat()
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def now_et() -> datetime:
    """Current wall-clock time in US Eastern (tz-aware)."""
    return datetime.now(ET)


def today_et() -> date:
    """Today's date as observed in US Eastern."""
    return now_et().date()
