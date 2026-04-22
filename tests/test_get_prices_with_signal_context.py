"""Tests for prices.get_prices_with_signal_context.

This is the function the dashboard's refresh callback and the alert
evaluator both call on every tick. It joins current_state with whatever
price source is available (DataBento → yFinance → DB cache) and computes
pnl_pct, cancel_dist_pct, trigger_dist_pct, and matched targets.

We mock get_current_prices() so the tests run offline. The core math
and joining logic is what matters here — the price-source fallback is
exercised separately by integration paths.
"""

from datetime import date

import pytest

from conftest import seed_current_state
from nenner_engine import prices as _prices


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _patch_prices(monkeypatch, ticker_to_price):
    """Mock get_current_prices to return a fixed mapping."""
    def fake_get_current_prices(conn, tickers, try_t1=True):
        return {
            t: {"price": p, "source": "test", "as_of": "2026-04-22"}
            for t, p in ticker_to_price.items()
        }
    monkeypatch.setattr(_prices, "get_current_prices", fake_get_current_prices)


# ---------------------------------------------------------------------------
# Basic enrichment + filter
# ---------------------------------------------------------------------------

def test_enriches_buy_signal_with_full_pnl_and_cancel_dist(test_db, monkeypatch):
    """BUY at 4400 origin, current price 4500 → +2.27% pnl, cancel below."""
    seed_current_state(test_db, ticker="GC", signal="BUY",
                       origin_price=4400.0, cancel_level=4350.0,
                       cancel_direction="BELOW")
    _patch_prices(monkeypatch, {"GC": 4500.0})

    rows = _prices.get_prices_with_signal_context(test_db)
    assert len(rows) == 1
    r = rows[0]
    assert r["ticker"] == "GC"
    assert r["price"] == 4500.0
    assert r["price_source"] == "test"
    assert r["pnl"] == pytest.approx(100.0)              # 4500 - 4400
    assert r["pnl_pct"] == pytest.approx(100/4400 * 100, abs=0.01)  # ~2.27%
    assert r["cancel_dist_pct"] == pytest.approx(-150/4500 * 100, abs=0.01)
    # No trigger / no target seeded
    assert r["trigger_dist_pct"] is None
    assert r["target_price"] is None


def test_sell_signal_pnl_is_inverted(test_db, monkeypatch):
    """SELL profits when price drops below entry — pnl flipped."""
    seed_current_state(test_db, ticker="ES", instrument="S&P 500",
                       signal="SELL", origin_price=5400.0,
                       cancel_level=5450.0, cancel_direction="ABOVE")
    _patch_prices(monkeypatch, {"ES": 5350.0})  # price down 50

    r = _prices.get_prices_with_signal_context(test_db)[0]
    # Naive (price - origin) = -50, but SELL inverts → positive 50 pnl
    assert r["pnl"] == pytest.approx(50.0)
    assert r["pnl_pct"] > 0, "SELL with price down should show positive pnl"


def test_sell_signal_pnl_negative_when_price_rises(test_db, monkeypatch):
    """SELL loses when price rises — pnl negative after inversion."""
    seed_current_state(test_db, ticker="ES", instrument="S&P 500",
                       signal="SELL", origin_price=5400.0,
                       cancel_level=5450.0, cancel_direction="ABOVE")
    _patch_prices(monkeypatch, {"ES": 5430.0})  # price up 30

    r = _prices.get_prices_with_signal_context(test_db)[0]
    assert r["pnl"] == pytest.approx(-30.0)
    assert r["pnl_pct"] < 0


# ---------------------------------------------------------------------------
# Missing-data paths — every derived field must be None, no crash
# ---------------------------------------------------------------------------

def test_no_price_source_yields_all_None_derived_fields(test_db, monkeypatch):
    """When no price source returns a value, derived fields are None."""
    seed_current_state(test_db, ticker="GC", signal="BUY",
                       origin_price=4400.0, cancel_level=4350.0)
    _patch_prices(monkeypatch, {})  # no prices for any ticker

    r = _prices.get_prices_with_signal_context(test_db)[0]
    assert r["price"] is None
    assert r["price_source"] is None
    assert r["pnl"] is None
    assert r["pnl_pct"] is None
    assert r["cancel_dist_pct"] is None
    assert r["trigger_dist_pct"] is None
    assert r["target_price"] is None


def test_zero_origin_does_not_crash_pnl_calc(test_db, monkeypatch):
    """origin_price = 0 (corrupt data) must not divide-by-zero."""
    seed_current_state(test_db, ticker="GC", signal="BUY",
                       origin_price=0.0, cancel_level=4350.0)
    _patch_prices(monkeypatch, {"GC": 4500.0})

    r = _prices.get_prices_with_signal_context(test_db)[0]
    assert r["pnl"] is None
    assert r["pnl_pct"] is None
    # But cancel_dist_pct should still be computable (uses price, not origin)
    assert r["cancel_dist_pct"] is not None


def test_missing_cancel_level_yields_none_dist(test_db, monkeypatch):
    """current_state row with NULL cancel_level → cancel_dist_pct is None."""
    seed_current_state(test_db, ticker="GC", signal="BUY",
                       origin_price=4400.0, cancel_level=None,
                       cancel_direction=None)
    _patch_prices(monkeypatch, {"GC": 4500.0})

    r = _prices.get_prices_with_signal_context(test_db)[0]
    assert r["cancel_dist_pct"] is None
    # pnl still works
    assert r["pnl_pct"] is not None


# ---------------------------------------------------------------------------
# Tickers filter
# ---------------------------------------------------------------------------

def test_tickers_filter_restricts_results(test_db, monkeypatch):
    """Passing tickers=['GC'] restricts to that ticker only."""
    seed_current_state(test_db, ticker="GC", signal="BUY", origin_price=4400.0)
    seed_current_state(test_db, ticker="SI", instrument="Silver",
                       signal="BUY", origin_price=78.0)
    seed_current_state(test_db, ticker="ES", instrument="S&P 500",
                       signal="SELL", origin_price=5400.0)
    _patch_prices(monkeypatch, {"GC": 4500.0, "SI": 80.0, "ES": 5350.0})

    rows = _prices.get_prices_with_signal_context(test_db, tickers=["GC"])
    assert {r["ticker"] for r in rows} == {"GC"}


def test_no_filter_returns_all_signals(test_db, monkeypatch):
    seed_current_state(test_db, ticker="GC", signal="BUY", origin_price=4400.0)
    seed_current_state(test_db, ticker="SI", instrument="Silver",
                       signal="BUY", origin_price=78.0)
    _patch_prices(monkeypatch, {"GC": 4500.0, "SI": 80.0})

    rows = _prices.get_prices_with_signal_context(test_db)
    assert {r["ticker"] for r in rows} == {"GC", "SI"}


# ---------------------------------------------------------------------------
# Price target matching
# ---------------------------------------------------------------------------

def _seed_target(conn, ticker: str, target_price: float, direction: str,
                 reached: int = 0, target_date: str | None = None):
    """Seed a price_targets row with a backing email so the EXISTS subquery
    in get_prices_with_signal_context's join finds it."""
    if target_date is None:
        target_date = date.today().isoformat()
    cur = conn.execute(
        "INSERT INTO emails (message_id, subject, date_sent, date_parsed, "
        "email_type, raw_text) "
        "VALUES (?, ?, ?, datetime('now'), 'morning_update', 'test')",
        (f"target-{ticker}-{direction}-{target_date}",
         f"Test {ticker}", target_date)
    )
    email_id = cur.lastrowid
    conn.execute(
        "INSERT INTO price_targets (email_id, date, ticker, instrument, "
        "target_price, direction, condition, reached, raw_text) "
        "VALUES (?, ?, ?, ?, ?, ?, '', ?, 'test')",
        (email_id, target_date, ticker, ticker, target_price, direction, reached)
    )
    conn.commit()


def test_buy_signal_picks_upside_target(test_db, monkeypatch):
    """BUY → target_direction must be UPSIDE."""
    seed_current_state(test_db, ticker="GC", signal="BUY", origin_price=4400.0)
    _seed_target(test_db, "GC", 4800.0, "UPSIDE")
    _seed_target(test_db, "GC", 4100.0, "DOWNSIDE")  # opposite — should be ignored
    _patch_prices(monkeypatch, {"GC": 4500.0})

    r = _prices.get_prices_with_signal_context(test_db)[0]
    assert r["target_price"] == 4800.0
    assert r["target_direction"] == "UPSIDE"
    assert r["target_dist_pct"] == pytest.approx(300/4500 * 100, abs=0.01)


def test_sell_signal_picks_downside_target(test_db, monkeypatch):
    """SELL → target_direction must be DOWNSIDE."""
    seed_current_state(test_db, ticker="ES", instrument="S&P 500",
                       signal="SELL", origin_price=5400.0)
    _seed_target(test_db, "ES", 5800.0, "UPSIDE")
    _seed_target(test_db, "ES", 5100.0, "DOWNSIDE")
    _patch_prices(monkeypatch, {"ES": 5350.0})

    r = _prices.get_prices_with_signal_context(test_db)[0]
    assert r["target_price"] == 5100.0
    assert r["target_direction"] == "DOWNSIDE"


def test_reached_target_excluded(test_db, monkeypatch):
    """Targets with reached=1 are filtered out by the SQL WHERE clause."""
    seed_current_state(test_db, ticker="GC", signal="BUY", origin_price=4400.0)
    _seed_target(test_db, "GC", 4800.0, "UPSIDE", reached=1)  # reached
    _patch_prices(monkeypatch, {"GC": 4500.0})

    r = _prices.get_prices_with_signal_context(test_db)[0]
    assert r["target_price"] is None
    assert r["target_dist_pct"] is None
