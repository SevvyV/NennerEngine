"""
Automatic Cancellation Detection
==================================
Checks daily closing prices against cancel levels in current_state.
When a cancel level is breached on the daily close, creates a synthetic
CANCELLED signal record and triggers state rebuild (implied reversal).

Usage:
    python -m nenner_engine --auto-cancel
"""

import logging
import sqlite3
from datetime import datetime, date

log = logging.getLogger(__name__)


def check_auto_cancellations(
    conn: sqlite3.Connection,
    price_date: str = None,
    regenerate: bool = False,
) -> list[dict]:
    """Check daily closing prices against cancel levels and auto-cancel breached signals.

    For each instrument in current_state with a cancel level:
    1. Get the daily closing price for the given date.
    2. If the close breaches the cancel level, write a CANCELLED signal
       row with ``source='auto_cancel'`` and ``email_id=NULL``. No fake
       email row is created (they previously polluted the `emails` table
       and blocked re-runs via message_id dedupe).
    3. After all cancellations, rebuild current_state.

    Args:
        conn: SQLite connection with row_factory = sqlite3.Row.
        price_date: Date to check (YYYY-MM-DD). Defaults to today.
        regenerate: If True, delete any existing source='auto_cancel' row
            for this (ticker, date) before re-writing. Use this to re-run
            auto-cancel after yFinance corrects a historical close.

    Returns:
        List of dicts describing each auto-cancellation that was triggered.
    """
    from .db import compute_current_state, is_cancel_breached
    from .prices import fetch_yfinance_daily

    if price_date is None:
        price_date = date.today().strftime("%Y-%m-%d")

    # Ensure we have fresh prices
    try:
        fetch_yfinance_daily(conn)
    except Exception as e:
        log.warning(f"Could not fetch fresh prices for auto-cancel: {e}")

    # Get all current signals with cancel levels
    rows = conn.execute("""
        SELECT cs.ticker, cs.instrument, cs.asset_class,
               cs.effective_signal, cs.cancel_direction, cs.cancel_level,
               cs.origin_price, cs.source_signal_id
        FROM current_state cs
        WHERE cs.cancel_level IS NOT NULL
    """).fetchall()

    if not rows:
        log.info("No instruments with cancel levels to check.")
        return []

    # Skip instruments that use hourly close (look up from source signal)
    hourly_signal_ids = set()
    source_ids = [r["source_signal_id"] for r in rows if r["source_signal_id"]]
    if source_ids:
        placeholders = ",".join("?" * len(source_ids))
        hourly_rows = conn.execute(f"""
            SELECT id FROM signals
            WHERE id IN ({placeholders}) AND uses_hourly_close = 1
        """, source_ids).fetchall()
        hourly_signal_ids = {r["id"] for r in hourly_rows}

    cancellations = []

    for row in rows:
        ticker = row["ticker"]
        instrument = row["instrument"]
        cancel_dir = row["cancel_direction"]
        cancel_level = row["cancel_level"]
        signal_type = row["effective_signal"]
        source_id = row["source_signal_id"]

        # Skip hourly-close instruments
        if source_id in hourly_signal_ids:
            log.debug(f"Skipping {ticker}: uses hourly close")
            continue

        if not cancel_dir or not cancel_level:
            continue

        # Get daily closing price for this date.
        # Exclude DATABENTO_EQUITY — those are intraday midpoint snapshots,
        # not settled daily closes.  Cancel levels require a confirmed close.
        price_row = conn.execute("""
            SELECT close FROM price_history
            WHERE ticker = ? AND date = ?
            AND source != 'DATABENTO_EQUITY'
            ORDER BY fetched_at DESC
            LIMIT 1
        """, (ticker, price_date)).fetchone()

        if price_row is None:
            log.debug(f"No price data for {ticker} on {price_date}")
            continue

        close_price = price_row["close"]
        if close_price is None:
            continue

        # Check if cancel level is breached (centralized rule in db.is_cancel_breached)
        if not is_cancel_breached(cancel_dir, cancel_level, close_price):
            continue

        # Dedupe: if we already wrote an auto_cancel row for this
        # (ticker, date), skip unless regenerate=True.
        existing = conn.execute(
            "SELECT id FROM signals "
            "WHERE ticker = ? AND date = ? AND source = 'auto_cancel' "
            "AND signal_status = 'CANCELLED' LIMIT 1",
            (ticker, price_date),
        ).fetchone()
        if existing:
            if regenerate:
                conn.execute(
                    "DELETE FROM signals WHERE id = ?", (existing["id"],)
                )
            else:
                log.debug(
                    f"Auto-cancel for {ticker} on {price_date} already "
                    f"recorded (signals.id={existing['id']})"
                )
                continue

        log.info(
            f"AUTO-CANCEL: {ticker} ({instrument}) {signal_type} cancelled. "
            f"Close={close_price:.2f} breached cancel {cancel_dir} {cancel_level:.2f}"
        )

        raw_text = (
            f"Automatic cancellation detected by NennerEngine. "
            f"{ticker} ({instrument}) daily close of {close_price:.2f} "
            f"breached cancel level {cancel_dir} {cancel_level:.2f}. "
            f"The {signal_type} signal is cancelled, implying reversal."
        )

        conn.execute(
            "INSERT INTO signals (email_id, date, instrument, ticker, "
            "asset_class, signal_type, signal_status, origin_price, "
            "cancel_direction, cancel_level, trigger_direction, trigger_level, "
            "price_target, target_direction, note_the_change, "
            "uses_hourly_close, raw_text, source) "
            "VALUES (NULL, ?, ?, ?, ?, ?, 'CANCELLED', ?, ?, ?, NULL, NULL, "
            "NULL, NULL, 0, 0, ?, 'auto_cancel')",
            (
                price_date, instrument, ticker,
                row["asset_class"] or "Unknown",
                signal_type, row["origin_price"],
                cancel_dir, cancel_level,
                raw_text[:500],
            ),
        )

        cancellations.append({
            "ticker": ticker,
            "instrument": instrument,
            "old_signal": signal_type,
            "new_signal": "BUY" if signal_type == "SELL" else "SELL",
            "cancel_level": cancel_level,
            "close_price": close_price,
            "date": price_date,
        })

    if cancellations:
        conn.commit()
        compute_current_state(conn)
        log.info(f"Auto-cancelled {len(cancellations)} signal(s)")
    else:
        log.info(f"No auto-cancellations for {price_date}")

    return cancellations
