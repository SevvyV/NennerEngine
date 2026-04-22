"""Tests for nenner_engine.tz — locks in the DST behavior that the old
UTC-5 fallback in email_scheduler silently got wrong for 8 months a year.
"""

from datetime import datetime

from nenner_engine.tz import ET, now_et, today_et


class TestEasternTime:
    def test_now_et_returns_tz_aware_datetime(self):
        result = now_et()
        assert result.tzinfo is not None
        assert result.tzinfo is ET or str(result.tzinfo) == "America/New_York"

    def test_today_et_is_a_date(self):
        from datetime import date as _date
        assert isinstance(today_et(), _date)

    def test_dst_is_observed_in_april(self):
        """In April, US Eastern is EDT (UTC-4), not EST (UTC-5). The old
        `datetime.utcnow() - timedelta(hours=5)` fallback would be wrong
        by one hour during DST — a signal-check scheduled for 8:35 AM ET
        would fire at 7:35 AM ET instead."""
        apr = datetime(2026, 4, 15, 12, 0, tzinfo=ET)
        assert apr.utcoffset().total_seconds() == -4 * 3600

    def test_standard_time_in_january(self):
        """In January, US Eastern is EST (UTC-5)."""
        jan = datetime(2026, 1, 15, 12, 0, tzinfo=ET)
        assert jan.utcoffset().total_seconds() == -5 * 3600
