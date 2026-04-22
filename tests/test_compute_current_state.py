"""Tests for db.compute_current_state — gaps not covered by test_nenner_engine.py.

The legacy test file (TestStateMachine in test_nenner_engine.py) covers the
happy paths: ACTIVE/CANCELLED transitions, multi-instrument independence,
latest-signal-wins, empty DB. This file adds three behaviors that have
no direct coverage:

  1. Atomicity — a mid-loop exception must roll back the DELETE so a
     subsequent commit on the same connection cannot expose a half-empty
     current_state. Pins the with-conn fix landed in a95d9ca (Risk #2).
  2. Already-breached cancel on a fresh ACTIVE signal flips to implied
     reversal at rebuild time, instead of waiting for the next auto-cancel
     pass.
  3. DATABENTO_EQUITY prices (intraday midpoint snapshots) are excluded
     from the breach check — Nenner's cancel rule requires a confirmed
     daily close.

Tickers used in tests must exist in INSTRUMENT_MAP — compute_current_state
filters out unmapped tickers.
"""

import sqlite3

import pytest

from conftest import seed_signal, seed_current_state, seed_price_history
from nenner_engine.db import compute_current_state


# ---------------------------------------------------------------------------
# Atomicity (Risk #2 fix verification)
# ---------------------------------------------------------------------------

def test_mid_loop_exception_rolls_back_delete(test_db, monkeypatch):
    """If compute_current_state crashes mid-loop, the prior current_state
    survives — the DELETE rolls back with the partial INSERTs.

    Without `with conn:`, the implicit transaction would stay open with
    the DELETE pending; any subsequent commit on the same connection
    (e.g. from auto_cancel writing a signal row) would commit just the
    DELETE and silently empty the table.
    """
    # Pre-existing state — represents the row(s) currently in the table
    seed_current_state(test_db, ticker="GC", signal="BUY",
                       origin_price=4380.0, cancel_level=4590.0)

    # Seed signals so the rebuild loop has work to do — three real tickers
    seed_signal(test_db, ticker="GC", signal_type="BUY", signal_status="ACTIVE",
                origin_price=4400.0, cancel_direction="BELOW", cancel_level=4350.0)
    seed_signal(test_db, ticker="SI", signal_type="SELL", signal_status="ACTIVE",
                origin_price=78.0, cancel_direction="ABOVE", cancel_level=80.0)
    seed_signal(test_db, ticker="TSLA", signal_type="BUY", signal_status="ACTIVE",
                origin_price=250.0, cancel_direction="BELOW", cancel_level=240.0)

    # Inject a fault: the second call to is_cancel_breached raises. The
    # first row INSERTs, then the loop crashes. With atomicity, the DELETE
    # AND that first INSERT both roll back; pre-existing GC row survives.
    import nenner_engine.db as db_module
    real = db_module.is_cancel_breached
    call_count = {"n": 0}

    def fault_injecting_breach_check(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise RuntimeError("simulated mid-loop fault")
        return real(*args, **kwargs)

    monkeypatch.setattr(db_module, "is_cancel_breached", fault_injecting_breach_check)

    with pytest.raises(RuntimeError, match="simulated mid-loop fault"):
        compute_current_state(test_db)

    # Pre-existing row must still be there — the rollback restored it.
    rows = test_db.execute(
        "SELECT ticker, effective_signal, origin_price, cancel_level "
        "FROM current_state"
    ).fetchall()
    assert len(rows) == 1, (
        f"Expected pre-rebuild state to be restored, got {len(rows)} rows: "
        f"{[dict(r) for r in rows]}"
    )
    assert rows[0]["ticker"] == "GC"
    assert rows[0]["effective_signal"] == "BUY"
    assert rows[0]["origin_price"] == 4380.0   # original, NOT 4400 from new signal
    assert rows[0]["cancel_level"] == 4590.0   # original, NOT 4350 from new signal


# ---------------------------------------------------------------------------
# Already-breached cancel → implied reversal at rebuild
# ---------------------------------------------------------------------------

def test_active_buy_with_breached_cancel_flips_to_implied_sell(test_db):
    """A fresh ACTIVE BUY whose cancel is already past the latest close
    becomes an implied SELL immediately on rebuild — no waiting for the
    next auto-cancel sweep."""
    # ACTIVE BUY: origin 4400, cancel BELOW 4350, with trigger ABOVE 4350
    seed_signal(test_db, ticker="GC", signal_type="BUY", signal_status="ACTIVE",
                origin_price=4400.0,
                cancel_direction="BELOW", cancel_level=4350.0,
                trigger_direction="ABOVE", trigger_level=4350.0)
    # Latest close is 4300 — already through the 4350 cancel level
    seed_price_history(test_db, ticker="GC", close=4300.0, source="yfinance")

    compute_current_state(test_db)

    row = test_db.execute(
        "SELECT effective_signal, implied_reversal, origin_price, cancel_level "
        "FROM current_state WHERE ticker='GC'"
    ).fetchone()
    assert row is not None
    assert row["effective_signal"] == "SELL"     # BUY cancelled → implied SELL
    assert row["implied_reversal"] == 1
    assert row["origin_price"] == 4350.0          # cancel level becomes new origin
    assert row["cancel_level"] == 4350.0          # trigger becomes new cancel


def test_active_buy_with_unbreached_cancel_stays_buy(test_db):
    """Sanity check the breach-flip: an ACTIVE BUY whose cancel is NOT
    breached stays a normal BUY (not implied)."""
    seed_signal(test_db, ticker="GC", signal_type="BUY", signal_status="ACTIVE",
                origin_price=4400.0,
                cancel_direction="BELOW", cancel_level=4350.0)
    # Close above the cancel level — no breach
    seed_price_history(test_db, ticker="GC", close=4380.0, source="yfinance")

    compute_current_state(test_db)

    row = test_db.execute(
        "SELECT effective_signal, implied_reversal FROM current_state WHERE ticker='GC'"
    ).fetchone()
    assert row["effective_signal"] == "BUY"
    assert row["implied_reversal"] == 0


# ---------------------------------------------------------------------------
# DATABENTO intraday prices excluded from breach check
# ---------------------------------------------------------------------------

def test_databento_intraday_price_does_not_trigger_breach(test_db):
    """DATABENTO_EQUITY prices are intraday midpoint snapshots, not
    confirmed daily closes. Cancel-breach detection at rebuild time must
    ignore them — only settled daily bars count.

    Setup: yfinance close of 4380 (above cancel 4350, no breach). Add
    a later DATABENTO_EQUITY snapshot at 4300 (would be a breach if it
    counted). Result must still be a normal BUY, not an implied reversal.
    """
    seed_signal(test_db, ticker="GC", signal_type="BUY", signal_status="ACTIVE",
                origin_price=4400.0,
                cancel_direction="BELOW", cancel_level=4350.0)
    # Yesterday's confirmed daily close — above cancel
    seed_price_history(test_db, ticker="GC", close=4380.0,
                       price_date="2026-04-21", source="yfinance")
    # Today's intraday snapshot — would be a breach if it counted
    seed_price_history(test_db, ticker="GC", close=4300.0,
                       price_date="2026-04-22", source="DATABENTO_EQUITY")

    compute_current_state(test_db)

    row = test_db.execute(
        "SELECT effective_signal, implied_reversal FROM current_state WHERE ticker='GC'"
    ).fetchone()
    assert row["effective_signal"] == "BUY", (
        "DATABENTO_EQUITY intraday price was incorrectly used for "
        "cancel-breach check — should only use settled daily closes"
    )
    assert row["implied_reversal"] == 0
