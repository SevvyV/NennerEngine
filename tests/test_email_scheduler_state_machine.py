"""Tests for EmailScheduler state machine — pin one-fire-per-day dedup
and weekday/window gating without exercising the IMAP/Anthropic side
effects.

The scheduler doesn't take any tz-aware datetime object — internally it
calls _now_eastern() for some checks but the per-tick _check_* methods
take an explicit now_et arg. We feed naive datetimes covering the
boundary cases and watch the side-effect counters.

We monkeypatch the side-effecting helpers (_send_stock_report,
_run_auto_cancel) at module level so no email is sent and no DB write
happens.

Constants (from nenner_engine.config):
  STOCK_REPORT  → 8:30 ET (weekdays)
  AUTO_CANCEL   → 16:30 ET (every day)
  WATCHDOG      → 12:00 ET (Mon/Wed/Fri only)
"""

from datetime import datetime, timedelta

import pytest

from nenner_engine import email_scheduler as _es
from nenner_engine.email_scheduler import EmailScheduler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def call_counters(monkeypatch):
    """Replace side-effecting helpers with counters."""
    counters = {"stock_report": 0, "auto_cancel": 0, "watchdog_tg": 0}

    def fake_send_stock_report(db_path):
        counters["stock_report"] += 1

    def fake_run_auto_cancel(db_path, today_str):
        counters["auto_cancel"] += 1

    monkeypatch.setattr(_es, "_send_stock_report", fake_send_stock_report)
    monkeypatch.setattr(_es, "_run_auto_cancel", fake_run_auto_cancel)
    return counters


@pytest.fixture
def sched():
    """A fresh EmailScheduler, never started."""
    return EmailScheduler(
        db_path=":memory:", check_on_start=False, daily_check=True,
    )


# ---------------------------------------------------------------------------
# Stock report: weekdays only, in window, once per day
# ---------------------------------------------------------------------------

def test_stock_report_fires_on_wednesday_in_window(sched, call_counters):
    """Wed 2026-04-22 08:30 ET → fires."""
    wed_in_window = datetime(2026, 4, 22, 8, 30)  # weekday() == 2 (Wed)
    sched._check_stock_report(wed_in_window)
    assert call_counters["stock_report"] == 1
    assert sched._last_stock_report_date == "2026-04-22"


def test_stock_report_does_not_fire_outside_window(sched, call_counters):
    """Wed 09:00 ET — past 5-min window."""
    sched._check_stock_report(datetime(2026, 4, 22, 9, 0))
    assert call_counters["stock_report"] == 0
    assert sched._last_stock_report_date is None


def test_stock_report_skipped_on_saturday(sched, call_counters):
    """Sat 2026-04-25 08:30 ET — weekend skip."""
    saturday = datetime(2026, 4, 25, 8, 30)  # weekday() == 5
    sched._check_stock_report(saturday)
    assert call_counters["stock_report"] == 0


def test_stock_report_skipped_on_sunday(sched, call_counters):
    """Sun 2026-04-26 08:30 ET — weekend skip."""
    sched._check_stock_report(datetime(2026, 4, 26, 8, 30))
    assert call_counters["stock_report"] == 0


def test_stock_report_does_not_re_fire_same_day(sched, call_counters):
    """Two ticks within the same window on the same day → still 1 call."""
    sched._check_stock_report(datetime(2026, 4, 22, 8, 30))
    sched._check_stock_report(datetime(2026, 4, 22, 8, 31))
    sched._check_stock_report(datetime(2026, 4, 22, 8, 34, 30))
    assert call_counters["stock_report"] == 1


def test_stock_report_re_fires_next_day(sched, call_counters):
    """Different date string → fresh fire."""
    sched._check_stock_report(datetime(2026, 4, 22, 8, 30))  # Wed
    sched._check_stock_report(datetime(2026, 4, 23, 8, 30))  # Thu
    assert call_counters["stock_report"] == 2


# ---------------------------------------------------------------------------
# Auto-cancel: in window, once per day, every day
# ---------------------------------------------------------------------------

def test_auto_cancel_fires_at_window_start(sched, call_counters):
    sched._check_auto_cancel(datetime(2026, 4, 22, 16, 30))
    assert call_counters["auto_cancel"] == 1
    assert sched._last_auto_cancel_date == "2026-04-22"


def test_auto_cancel_runs_on_weekend_too(sched, call_counters):
    """Auto-cancel doesn't have the weekday gate — Saturdays still fire."""
    sched._check_auto_cancel(datetime(2026, 4, 25, 16, 30))  # Sat
    assert call_counters["auto_cancel"] == 1


def test_auto_cancel_does_not_re_fire_same_day(sched, call_counters):
    sched._check_auto_cancel(datetime(2026, 4, 22, 16, 30))
    sched._check_auto_cancel(datetime(2026, 4, 22, 16, 33))
    assert call_counters["auto_cancel"] == 1


def test_auto_cancel_outside_window_silent(sched, call_counters):
    sched._check_auto_cancel(datetime(2026, 4, 22, 16, 0))   # before
    sched._check_auto_cancel(datetime(2026, 4, 22, 16, 36))  # after
    assert call_counters["auto_cancel"] == 0


# ---------------------------------------------------------------------------
# Nenner watchdog: Mon/Wed/Fri at noon, once per day, no email = alert
# ---------------------------------------------------------------------------

def test_watchdog_skips_tuesday(sched, call_counters, monkeypatch):
    """Tuesday is not in NENNER_EXPECTED_DAYS — short-circuit before any work."""
    # Inject a tracker for the DB query so we can confirm we never reached it.
    db_open_count = {"n": 0}
    real_init_db = _es.__dict__.get("init_db")

    def fake_init_db(path):
        db_open_count["n"] += 1
        raise RuntimeError("should not be called on a non-Nenner day")

    # The function imports init_db lazily from .db — patch there.
    from nenner_engine import db as _db
    monkeypatch.setattr(_db, "init_db", fake_init_db)

    tuesday_noon = datetime(2026, 4, 21, 12, 0)  # weekday == 1
    sched._check_nenner_watchdog(tuesday_noon)

    assert db_open_count["n"] == 0, "Watchdog reached DB on a non-Nenner day"


def test_watchdog_outside_noon_window_does_not_fire(sched, monkeypatch):
    """Wed 11:30 — before window, no work."""
    from nenner_engine import db as _db
    sentinel = {"called": False}

    def trip(path):
        sentinel["called"] = True

    monkeypatch.setattr(_db, "init_db", trip)

    wed_early = datetime(2026, 4, 22, 11, 30)
    sched._check_nenner_watchdog(wed_early)
    assert not sentinel["called"]


def test_watchdog_does_not_re_fire_same_day(sched, monkeypatch, test_db):
    """Two noon ticks on Wed → DB queried once."""
    db_open_count = {"n": 0}
    from nenner_engine import db as _db

    def fake_init_db(path):
        db_open_count["n"] += 1
        return test_db  # has emails table from migrations

    monkeypatch.setattr(_db, "init_db", fake_init_db)

    sched._check_nenner_watchdog(datetime(2026, 4, 22, 12, 0))
    sched._check_nenner_watchdog(datetime(2026, 4, 22, 12, 3))
    assert db_open_count["n"] == 1
    assert sched._last_watchdog_date == "2026-04-22"


# ---------------------------------------------------------------------------
# Startup catch-up — fires only when past schedule on a weekday and unsent
# ---------------------------------------------------------------------------

def test_catchup_fires_when_past_window_on_weekday(sched, call_counters,
                                                   monkeypatch):
    """Launch at 09:00 on Wed → 30 min past 8:30 schedule, so catch-up fires."""
    monkeypatch.setattr(_es, "_now_eastern",
                        lambda: datetime(2026, 4, 22, 9, 0))
    sched._startup_stock_report_catchup()
    assert call_counters["stock_report"] == 1
    assert sched._last_stock_report_date == "2026-04-22"


def test_catchup_does_not_fire_before_window(sched, call_counters, monkeypatch):
    """Launch at 06:00 on Wed → before 8:30 schedule, catch-up skipped."""
    monkeypatch.setattr(_es, "_now_eastern",
                        lambda: datetime(2026, 4, 22, 6, 0))
    sched._startup_stock_report_catchup()
    assert call_counters["stock_report"] == 0


def test_catchup_skips_weekend(sched, call_counters, monkeypatch):
    """Sat 09:00 — weekend, never fires regardless of time."""
    monkeypatch.setattr(_es, "_now_eastern",
                        lambda: datetime(2026, 4, 25, 9, 0))
    sched._startup_stock_report_catchup()
    assert call_counters["stock_report"] == 0


def test_catchup_skips_if_already_sent(sched, call_counters, monkeypatch):
    """If _last_stock_report_date is already today, catch-up no-ops."""
    sched._last_stock_report_date = "2026-04-22"
    monkeypatch.setattr(_es, "_now_eastern",
                        lambda: datetime(2026, 4, 22, 9, 0))
    sched._startup_stock_report_catchup()
    assert call_counters["stock_report"] == 0


def test_catchup_5min_grace_buffer(sched, call_counters, monkeypatch):
    """At 8:32 on Wed — within the 5-min grace AFTER schedule, no catch-up
    (the regular scheduled tick still has time to fire). Catch-up only
    activates after schedule + 5 min."""
    # 8:30 + 5 min = 8:35; at 8:34, no catch-up
    monkeypatch.setattr(_es, "_now_eastern",
                        lambda: datetime(2026, 4, 22, 8, 34))
    sched._startup_stock_report_catchup()
    assert call_counters["stock_report"] == 0

    # At 8:36, past the grace, catch-up fires
    monkeypatch.setattr(_es, "_now_eastern",
                        lambda: datetime(2026, 4, 22, 8, 36))
    sched._startup_stock_report_catchup()
    assert call_counters["stock_report"] == 1
