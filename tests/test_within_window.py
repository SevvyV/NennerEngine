"""Tests for email_scheduler._within_window — pin the Risk #3 fix.

The helper replaces the buggy `MINUTE <= m < MINUTE + 5` form that
silently dropped the next-hour minutes (0-4) when MINUTE > 54. The
hour-boundary cases below would FAIL on the old form.
"""

from datetime import datetime

from nenner_engine.email_scheduler import _within_window


# ---------------------------------------------------------------------------
# Normal in-hour windows (the only cases the old form handled)
# ---------------------------------------------------------------------------

def test_at_window_start():
    assert _within_window(datetime(2026, 4, 22, 8, 30), 8, 30)


def test_just_before_window_end():
    assert _within_window(datetime(2026, 4, 22, 8, 34, 59), 8, 30)


def test_exactly_at_window_end_excluded():
    assert not _within_window(datetime(2026, 4, 22, 8, 35), 8, 30)


def test_just_before_window_start_excluded():
    assert not _within_window(datetime(2026, 4, 22, 8, 29, 59), 8, 30)


# ---------------------------------------------------------------------------
# Hour-boundary span — the regression the old form silently dropped
# ---------------------------------------------------------------------------

def test_window_starts_at_minute_58_includes_start():
    assert _within_window(datetime(2026, 4, 22, 8, 58), 8, 58)


def test_window_starts_at_minute_58_includes_next_hour_start():
    """At minute 9:00 with target 8:58, the old form would reject because
    `hour == 8` was False. The new form spans the boundary."""
    assert _within_window(datetime(2026, 4, 22, 9, 0), 8, 58)


def test_window_starts_at_minute_58_includes_next_hour_minute_2():
    """8:58 + 5min = 9:03 (exclusive), so 9:02:59 should be inside."""
    assert _within_window(datetime(2026, 4, 22, 9, 2, 59), 8, 58)


def test_window_starts_at_minute_58_excludes_minute_3_of_next_hour():
    assert not _within_window(datetime(2026, 4, 22, 9, 3), 8, 58)


# ---------------------------------------------------------------------------
# Wrong hour entirely
# ---------------------------------------------------------------------------

def test_one_hour_before_excluded():
    assert not _within_window(datetime(2026, 4, 22, 7, 30), 8, 30)


def test_one_hour_after_excluded():
    assert not _within_window(datetime(2026, 4, 22, 9, 30), 8, 30)


# ---------------------------------------------------------------------------
# Custom window length
# ---------------------------------------------------------------------------

def test_custom_window_length():
    """Override the 5-minute default."""
    assert _within_window(datetime(2026, 4, 22, 8, 32), 8, 30, window_minutes=3)
    assert not _within_window(datetime(2026, 4, 22, 8, 33), 8, 30, window_minutes=3)
