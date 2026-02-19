"""
Reporting & Display
====================
Console output for signal status, history, and CSV export.
"""

import csv
import os
import logging
import sqlite3

log = logging.getLogger("nenner")


def show_status(conn: sqlite3.Connection):
    """Print current signal state for all instruments."""

    # ---- Effective State (from state machine) ----
    state_rows = conn.execute("""
        SELECT * FROM current_state ORDER BY asset_class, instrument, ticker
    """).fetchall()

    if state_rows:
        print("\n" + "=" * 110)
        print("  EFFECTIVE SIGNAL STATE  |  Nenner state machine (cancellation = reversal)")
        print("=" * 110)

        current_class = ""
        for row in state_rows:
            if row["asset_class"] != current_class:
                current_class = row["asset_class"]
                print(f"\n  [{current_class}]")
                print(f"  {'Instrument':<25} {'Ticker':<8} {'Signal':<6} "
                      f"{'From':>10} {'Cancel':>10} {'Flip':>10} {'Implied':<8} {'Date':<12}")
                print("  " + "-" * 100)

            signal = row["effective_signal"][:4] if row["effective_signal"] else "----"
            origin = f"{row['origin_price']:>10,.2f}" if row["origin_price"] else "      ----"
            cancel = f"{row['cancel_level']:>10,.2f}" if row["cancel_level"] else "      ----"
            trigger = f"{row['trigger_level']:>10,.2f}" if row["trigger_level"] else "      ----"
            implied = " (impl)" if row["implied_reversal"] else "       "

            print(f"  {row['instrument']:<25} {row['ticker']:<8} {signal:<6} "
                  f"{origin} {cancel} {trigger} {implied} {row['last_signal_date']:<12}")

    # ---- Raw Signal Log (original view for reference) ----
    print("\n" + "=" * 110)
    print("  RAW SIGNAL LOG  |  Latest parsed signals per instrument")
    print("=" * 110)

    rows = conn.execute("""
        SELECT s.instrument, s.ticker, s.asset_class, s.signal_type, s.signal_status,
               s.origin_price, s.cancel_direction, s.cancel_level,
               s.trigger_direction, s.trigger_level, s.note_the_change,
               s.date, s.uses_hourly_close
        FROM signals s
        INNER JOIN (
            SELECT instrument, ticker, MAX(date) as max_date
            FROM signals
            GROUP BY instrument, ticker
        ) latest ON s.instrument = latest.instrument
               AND s.ticker = latest.ticker
               AND s.date = latest.max_date
        ORDER BY s.asset_class, s.instrument, s.ticker
    """).fetchall()

    current_class = ""
    for row in rows:
        if row["asset_class"] != current_class:
            current_class = row["asset_class"]
            print(f"\n  [{current_class}]")
            print(f"  {'Instrument':<25} {'Ticker':<8} {'Signal':<6} {'Status':<10} "
                  f"{'From':>10} {'Cancel':>10} {'Trigger':>10} {'NTC':<4} {'Date':<12}")
            print("  " + "-" * 105)

        signal = row["signal_type"][:4] if row["signal_type"] else "----"
        status = row["signal_status"][:6] if row["signal_status"] else "------"
        ntc = " *" if row["note_the_change"] else "  "
        hourly = "(H)" if row["uses_hourly_close"] else "   "

        origin = f"{row['origin_price']:>10,.2f}" if row["origin_price"] else "      ----"
        cancel = f"{row['cancel_level']:>10,.2f}" if row["cancel_level"] else "      ----"
        trigger = f"{row['trigger_level']:>10,.2f}" if row["trigger_level"] else "      ----"

        print(f"  {row['instrument']:<25} {row['ticker']:<8} {signal:<6} {status:<10} "
              f"{origin} {cancel} {trigger} {ntc}{hourly} {row['date']:<12}")

    # Price targets
    print(f"\n{'=' * 90}")
    print("  ACTIVE PRICE TARGETS")
    print("=" * 90)

    targets = conn.execute("""
        SELECT instrument, ticker, target_price, direction, condition, date
        FROM price_targets
        WHERE reached = 0
        AND date = (SELECT MAX(date) FROM price_targets)
        ORDER BY instrument
    """).fetchall()

    print(f"  {'Instrument':<25} {'Ticker':<8} {'Target':>10} {'Direction':<10} {'Condition':<30} {'Date':<12}")
    print("  " + "-" * 95)
    for row in targets:
        target = f"{row['target_price']:>10,.2f}" if row["target_price"] else "      ----"
        print(f"  {row['instrument']:<25} {row['ticker']:<8} {target} {row['direction']:<10} "
              f"{row['condition']:<30} {row['date']:<12}")

    # Cycle summary
    print(f"\n{'=' * 90}")
    print("  CYCLE DIRECTIONS (Latest)")
    print("=" * 90)

    cycles = conn.execute("""
        SELECT instrument, ticker, timeframe, direction, until_description, date
        FROM cycles
        WHERE date = (SELECT MAX(date) FROM cycles)
        ORDER BY instrument, timeframe
    """).fetchall()

    print(f"  {'Instrument':<25} {'Ticker':<8} {'Timeframe':<15} {'Direction':<6} {'Until':<30} {'Date':<12}")
    print("  " + "-" * 95)
    for row in cycles:
        print(f"  {row['instrument']:<25} {row['ticker']:<8} {row['timeframe']:<15} "
              f"{row['direction']:<6} {row['until_description']:<30} {row['date']:<12}")

    # Database stats
    print(f"\n{'=' * 90}")
    print("  DATABASE STATS")
    print("=" * 90)
    email_count = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    signal_count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    cycle_count = conn.execute("SELECT COUNT(*) FROM cycles").fetchone()[0]
    target_count = conn.execute("SELECT COUNT(*) FROM price_targets").fetchone()[0]
    min_date = conn.execute("SELECT MIN(date_sent) FROM emails").fetchone()[0]
    max_date = conn.execute("SELECT MAX(date_sent) FROM emails").fetchone()[0]
    print(f"  Emails: {email_count}  |  Signals: {signal_count}  |  Cycles: {cycle_count}  |  Targets: {target_count}")
    print(f"  Date range: {min_date} to {max_date}")
    print()


def show_history(conn: sqlite3.Connection, instrument: str):
    """Show signal history for an instrument."""
    print(f"\n  Signal History: {instrument}")
    print("=" * 100)

    rows = conn.execute("""
        SELECT date, signal_type, signal_status, origin_price, cancel_direction,
               cancel_level, trigger_level, note_the_change
        FROM signals
        WHERE instrument LIKE ? OR ticker LIKE ?
        ORDER BY date DESC, id DESC
        LIMIT 50
    """, (f"%{instrument}%", f"%{instrument}%")).fetchall()

    print(f"  {'Date':<12} {'Signal':<6} {'Status':<10} {'From':>10} {'Cancel Dir':<10} "
          f"{'Cancel Lvl':>10} {'Trigger':>10} {'NTC':<4}")
    print("  " + "-" * 80)

    for row in rows:
        origin = f"{row['origin_price']:>10,.2f}" if row["origin_price"] else "      ----"
        cancel = f"{row['cancel_level']:>10,.2f}" if row["cancel_level"] else "      ----"
        trigger = f"{row['trigger_level']:>10,.2f}" if row["trigger_level"] else "      ----"
        ntc = " *" if row["note_the_change"] else "  "
        print(f"  {row['date']:<12} {row['signal_type']:<6} {row['signal_status']:<10} "
              f"{origin} {row['cancel_direction'] or '':>10} {cancel} {trigger} {ntc}")
    print()


def export_csv(conn: sqlite3.Connection, base_dir: str = None):
    """Export all tables to CSV files."""
    if base_dir is None:
        base_dir = os.getcwd()

    tables = ["emails", "signals", "cycles", "price_targets"]

    for table in tables:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            continue

        csv_path = os.path.join(base_dir, f"nenner_{table}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(rows[0].keys())
            for row in rows:
                writer.writerow(tuple(row))

        log.info(f"Exported {len(rows)} rows to {csv_path}")
