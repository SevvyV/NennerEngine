"""Tests for nenner_engine.auto_cancel.check_auto_cancellations.

Focused on post-Phase-2.4 behavior: no synthetic emails, auto_cancel rows
are tagged via the signals.source column, and regenerate=True lets a
re-run overwrite a previously-written row (for yFinance close corrections).
"""

import sqlite3
from datetime import date, timedelta
from unittest.mock import patch

import pytest

from tests.conftest import make_test_db, seed_current_state, seed_price_history


def _count_emails(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]


def _count_signals_for(conn, ticker, price_date, source=None) -> int:
    q = "SELECT COUNT(*) FROM signals WHERE ticker = ? AND date = ?"
    args = [ticker, price_date]
    if source is not None:
        q += " AND source = ?"
        args.append(source)
    return conn.execute(q, args).fetchone()[0]


@pytest.fixture
def db():
    conn = make_test_db()
    yield conn
    conn.close()


@patch("nenner_engine.prices.fetch_yfinance_daily")
def test_breach_writes_cancelled_signal_without_touching_emails(
    mock_fetch, db
):
    """Previously auto-cancel created a fake `emails` row. Now it should
    write only a signals row tagged source='auto_cancel' and leave the
    emails table untouched."""
    mock_fetch.return_value = {}
    today = date.today().isoformat()

    seed_current_state(
        db, ticker="GC", signal="BUY",
        origin_price=2650.0, cancel_level=2580.0, cancel_direction="BELOW",
    )
    # Close strictly below cancel_level triggers a breach
    seed_price_history(db, ticker="GC", close=2570.0, source="yfinance")

    from nenner_engine.auto_cancel import check_auto_cancellations
    results = check_auto_cancellations(db, price_date=today)

    assert len(results) == 1
    assert results[0]["ticker"] == "GC"
    assert _count_emails(db) == 0, "auto_cancel must not write to emails table"
    assert _count_signals_for(db, "GC", today, source="auto_cancel") == 1


@patch("nenner_engine.prices.fetch_yfinance_daily")
def test_second_run_is_deduplicated_by_default(mock_fetch, db):
    mock_fetch.return_value = {}
    today = date.today().isoformat()

    seed_current_state(
        db, ticker="GC", signal="BUY",
        origin_price=2650.0, cancel_level=2580.0, cancel_direction="BELOW",
    )
    seed_price_history(db, ticker="GC", close=2570.0, source="yfinance")

    from nenner_engine.auto_cancel import check_auto_cancellations
    check_auto_cancellations(db, price_date=today)
    # Second call should skip — dedupe on (ticker, date, source)
    second = check_auto_cancellations(db, price_date=today)

    assert second == []
    assert _count_signals_for(db, "GC", today, source="auto_cancel") == 1


@patch("nenner_engine.prices.fetch_yfinance_daily")
def test_regenerate_overwrites_existing_row(mock_fetch, db):
    """When yFinance corrects a historical close, regenerate=True lets
    the admin re-run auto-cancel without a stale row blocking it."""
    mock_fetch.return_value = {}
    today = date.today().isoformat()

    seed_current_state(
        db, ticker="GC", signal="BUY",
        origin_price=2650.0, cancel_level=2580.0, cancel_direction="BELOW",
    )
    seed_price_history(db, ticker="GC", close=2570.0, source="yfinance")

    from nenner_engine.auto_cancel import check_auto_cancellations
    check_auto_cancellations(db, price_date=today)
    assert _count_signals_for(db, "GC", today, source="auto_cancel") == 1

    # Regenerate — previous row is replaced, not appended
    check_auto_cancellations(db, price_date=today, regenerate=True)
    assert _count_signals_for(db, "GC", today, source="auto_cancel") == 1


@patch("nenner_engine.prices.fetch_yfinance_daily")
def test_close_equal_to_cancel_is_not_a_breach(mock_fetch, db):
    """Regression for the centralized is_cancel_breached rule — a close
    exactly at the cancel level must NOT trigger auto-cancel."""
    mock_fetch.return_value = {}
    today = date.today().isoformat()

    seed_current_state(
        db, ticker="GC", signal="BUY",
        origin_price=2650.0, cancel_level=2580.0, cancel_direction="BELOW",
    )
    seed_price_history(db, ticker="GC", close=2580.0, source="yfinance")

    from nenner_engine.auto_cancel import check_auto_cancellations
    results = check_auto_cancellations(db, price_date=today)

    assert results == []
    assert _count_signals_for(db, "GC", today, source="auto_cancel") == 0
