"""Tests for alerts.evaluate_price_alerts.

Pure function — takes the rows output of get_prices_with_signal_context()
and emits alert dicts when a position's price is within a threshold of
its cancel level. No DB, no side effects, no mocks needed.

Thresholds (from alerts.py):
  PROXIMITY_DANGER_PCT  = 0.5   → DANGER below this
  PROXIMITY_WARNING_PCT = 1.0   → WATCH between DANGER and WARNING
                                 → no alert at or above WARNING
"""

import pytest

from nenner_engine.alerts import (
    evaluate_price_alerts,
    PROXIMITY_DANGER_PCT,
    PROXIMITY_WARNING_PCT,
)


# ---------------------------------------------------------------------------
# Fixture builder
# ---------------------------------------------------------------------------

def _row(ticker="GC", instrument="Gold", price=4400.0, signal="BUY",
         cancel_level=4350.0, cancel_dist_pct=1.14):
    """One row in the shape evaluate_price_alerts expects."""
    return {
        "ticker": ticker,
        "instrument": instrument,
        "price": price,
        "effective_signal": signal,
        "cancel_level": cancel_level,
        "cancel_dist_pct": cancel_dist_pct,
    }


# ---------------------------------------------------------------------------
# Threshold band behavior
# ---------------------------------------------------------------------------

def test_danger_band_fires_danger_alert():
    """abs(cancel_dist_pct) < 0.5% → DANGER."""
    alerts = evaluate_price_alerts([_row(cancel_dist_pct=0.3)])
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "DANGER"
    assert alerts[0]["alert_type"] == "CANCEL_DANGER"


def test_watch_band_fires_warning_alert():
    """0.5% <= abs(cancel_dist_pct) < 1.0% → WATCH."""
    alerts = evaluate_price_alerts([_row(cancel_dist_pct=0.7)])
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "WARNING"
    assert alerts[0]["alert_type"] == "CANCEL_WATCH"


def test_safe_band_emits_nothing():
    """abs(cancel_dist_pct) >= 1.0% → no alert."""
    assert evaluate_price_alerts([_row(cancel_dist_pct=1.5)]) == []
    assert evaluate_price_alerts([_row(cancel_dist_pct=5.0)]) == []


def test_danger_warning_boundary_is_inclusive_on_warning_side():
    """exactly DANGER_PCT (0.5) → falls into WATCH, not DANGER (strict <)."""
    alerts = evaluate_price_alerts([_row(cancel_dist_pct=PROXIMITY_DANGER_PCT)])
    assert alerts[0]["severity"] == "WARNING"


def test_warning_safe_boundary_excludes_warning():
    """exactly WARNING_PCT (1.0) → no alert at all (strict <)."""
    assert evaluate_price_alerts([_row(cancel_dist_pct=PROXIMITY_WARNING_PCT)]) == []


def test_uses_absolute_value_for_distance():
    """A SELL position has cancel above price → cancel_dist_pct positive;
    a BUY has cancel below price → cancel_dist_pct negative. Threshold
    check must use abs() so both directions trigger the same way."""
    pos = evaluate_price_alerts([_row(cancel_dist_pct=0.3)])
    neg = evaluate_price_alerts([_row(cancel_dist_pct=-0.3)])
    assert len(pos) == 1 and pos[0]["severity"] == "DANGER"
    assert len(neg) == 1 and neg[0]["severity"] == "DANGER"


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------

def test_no_price_skipped():
    """price=None → row is silently skipped (no alert, no crash)."""
    assert evaluate_price_alerts([_row(price=None, cancel_dist_pct=0.1)]) == []


def test_no_cancel_distance_skipped():
    """cancel_dist_pct=None → no alert (untracked instrument or fresh signal)."""
    assert evaluate_price_alerts([_row(cancel_dist_pct=None)]) == []


def test_empty_input_returns_empty_list():
    assert evaluate_price_alerts([]) == []


# ---------------------------------------------------------------------------
# Multi-row + alert payload structure
# ---------------------------------------------------------------------------

def test_multiple_rows_independent_thresholds():
    """Each row evaluated independently; mixed bands all reported."""
    rows = [
        _row(ticker="GC", cancel_dist_pct=0.2),     # DANGER
        _row(ticker="SI", cancel_dist_pct=0.8),     # WATCH
        _row(ticker="TSLA", cancel_dist_pct=3.0),   # safe — no alert
        _row(ticker="ES", cancel_dist_pct=-0.4),    # DANGER (uses abs)
    ]
    alerts = evaluate_price_alerts(rows)
    assert len(alerts) == 3
    by_ticker = {a["ticker"]: a for a in alerts}
    assert by_ticker["GC"]["severity"] == "DANGER"
    assert by_ticker["SI"]["severity"] == "WARNING"
    assert by_ticker["ES"]["severity"] == "DANGER"
    assert "TSLA" not in by_ticker


def test_alert_payload_carries_context():
    """The alert dict must surface the fields downstream channels need:
    price, cancel_dist_pct, effective_signal — without those the Telegram
    formatter and history view break."""
    rows = [_row(ticker="GC", instrument="Gold", price=4400.0,
                 signal="BUY", cancel_level=4380.0, cancel_dist_pct=0.45)]
    alert = evaluate_price_alerts(rows)[0]
    assert alert["ticker"] == "GC"
    assert alert["instrument"] == "Gold"
    assert alert["current_price"] == 4400.0
    assert alert["effective_signal"] == "BUY"
    assert alert["cancel_dist_pct"] == 0.45
    # Message should have the concrete numbers, not placeholders
    assert "4,400.00" in alert["message"]
    assert "4,380.00" in alert["message"]
    assert "0.45%" in alert["message"]


def test_instrument_falls_back_to_ticker_when_missing():
    """If get_prices_with_signal_context didn't supply instrument
    (older row shape), the alert uses the ticker — no KeyError."""
    row = {
        "ticker": "GC",
        "price": 4400.0,
        "effective_signal": "BUY",
        "cancel_level": 4380.0,
        "cancel_dist_pct": 0.3,
        # no "instrument" key
    }
    alert = evaluate_price_alerts([row])[0]
    assert alert["instrument"] == "GC"
