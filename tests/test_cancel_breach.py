"""Tests for the centralized is_cancel_breached() helper."""

from nenner_engine.db import is_cancel_breached


class TestIsCancelBreached:
    # ABOVE: SELL signal cancelled by a close strictly above the level
    def test_above_close_strictly_above_is_breach(self):
        assert is_cancel_breached("ABOVE", 100.0, 100.01) is True

    def test_above_close_equal_is_not_breach(self):
        assert is_cancel_breached("ABOVE", 100.0, 100.0) is False

    def test_above_close_below_is_not_breach(self):
        assert is_cancel_breached("ABOVE", 100.0, 99.99) is False

    # BELOW: BUY signal cancelled by a close strictly below the level
    def test_below_close_strictly_below_is_breach(self):
        assert is_cancel_breached("BELOW", 100.0, 99.99) is True

    def test_below_close_equal_is_not_breach(self):
        assert is_cancel_breached("BELOW", 100.0, 100.0) is False

    def test_below_close_above_is_not_breach(self):
        assert is_cancel_breached("BELOW", 100.0, 100.01) is False

    # Defensive: any None returns False rather than raising
    def test_none_direction_returns_false(self):
        assert is_cancel_breached(None, 100.0, 101.0) is False

    def test_none_level_returns_false(self):
        assert is_cancel_breached("ABOVE", None, 101.0) is False

    def test_none_close_returns_false(self):
        assert is_cancel_breached("ABOVE", 100.0, None) is False

    def test_unknown_direction_returns_false(self):
        assert is_cancel_breached("SIDEWAYS", 100.0, 101.0) is False
