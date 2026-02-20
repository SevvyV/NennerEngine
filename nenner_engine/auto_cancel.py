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

log = logging.getLogger("nenner")


def check_auto_cancellations(
    conn: sqlite3.Connection,
    price_date: str = None,
) -> list[dict]:
    """Check daily closing prices against cancel levels and auto-cancel breached signals.

    For each instrument in current_state with a cancel level:
    1. Get the daily closing price for the given date.
    2. If the close breaches the cancel level, create a CANCELLED signal record.
    3. After all cancellations, rebuild current_state (implied reversals happen automatically).

    Args:
        conn: SQLite connection with row_factory = sqlite3.Row.
        price_date: Date to check (YYYY-MM-DD). Defaults to today.

    Returns:
        List of dicts describing each auto-cancellation that was triggered.
    """
    from .db import store_email, store_parsed_results, compute_current_state
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

        # Get daily closing price for this date
        price_row = conn.execute("""
            SELECT close FROM price_history
            WHERE ticker = ? AND date = ?
            ORDER BY fetched_at DESC
            LIMIT 1
        """, (ticker, price_date)).fetchone()

        if price_row is None:
            log.debug(f"No price data for {ticker} on {price_date}")
            continue

        close_price = price_row["close"]
        if close_price is None:
            continue

        # Check if cancel level is breached
        # ABOVE: breached if close > cancel_level (strict inequality)
        # BELOW: breached if close < cancel_level (strict inequality)
        breached = False
        if cancel_dir == "ABOVE" and close_price > cancel_level:
            breached = True
        elif cancel_dir == "BELOW" and close_price < cancel_level:
            breached = True

        if not breached:
            continue

        log.info(
            f"AUTO-CANCEL: {ticker} ({instrument}) {signal_type} cancelled. "
            f"Close={close_price:.2f} breached cancel {cancel_dir} {cancel_level:.2f}"
        )

        # Create synthetic email record
        message_id = f"auto-cancel-{ticker}-{price_date}"
        subject = f"Auto-Cancel: {ticker} {signal_type} cancelled on {price_date}"
        raw_text = (
            f"Automatic cancellation detected by NennerEngine. "
            f"{ticker} ({instrument}) daily close of {close_price:.2f} "
            f"breached cancel level {cancel_dir} {cancel_level:.2f}. "
            f"The {signal_type} signal is cancelled, implying reversal."
        )

        email_id = store_email(conn, message_id, subject, price_date,
                               "auto_cancel", raw_text)

        if email_id is None:
            # Already processed this auto-cancel (duplicate message_id)
            log.debug(f"Auto-cancel for {ticker} on {price_date} already processed")
            continue

        # Build the cancelled signal record
        results = {
            "signals": [{
                "email_id": email_id,
                "date": price_date,
                "instrument": instrument,
                "ticker": ticker,
                "asset_class": row["asset_class"] or "Unknown",
                "signal_type": signal_type,
                "signal_status": "CANCELLED",
                "origin_price": row["origin_price"],
                "cancel_direction": cancel_dir,
                "cancel_level": cancel_level,
                "trigger_direction": None,
                "trigger_level": None,
                "price_target": None,
                "target_direction": None,
                "note_the_change": 0,
                "uses_hourly_close": 0,
                "raw_text": raw_text[:500],
            }],
            "cycles": [],
            "price_targets": [],
        }

        store_parsed_results(conn, results, email_id)

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
        # State is rebuilt by store_parsed_results via compute_current_state
        log.info(f"Auto-cancelled {len(cancellations)} signal(s)")
    else:
        log.info(f"No auto-cancellations for {price_date}")

    return cancellations
