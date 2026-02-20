"""
CLI Entry Point
================
Argument parsing and command dispatch.
"""

import argparse
import os
import sys
import logging

from .db import init_db, migrate_db, compute_current_state
from .imap_client import backfill_imap, check_new_emails, import_eml_folder
from .reporting import show_status, show_history, export_csv
from .prices import fetch_yfinance_daily, get_prices_with_signal_context
from .alerts import run_monitor, show_alert_history


def _show_prices(conn):
    """Display current signal state enriched with live prices and P/L."""
    rows = get_prices_with_signal_context(conn)
    if not rows:
        print("No signal data available.")
        return

    # Table header
    header = (
        f"{'Ticker':<10} {'Signal':<6} {'Price':>12} {'Origin':>12} "
        f"{'P/L %':>8} {'Cancel':>12} {'CxDist%':>8} {'Source':<8}"
    )
    print("=" * len(header))
    print(header)
    print("=" * len(header))

    for r in rows:
        ticker = r["ticker"]
        signal = r.get("effective_signal", "")
        price = r.get("price")
        origin = r.get("origin_price")
        pnl_pct = r.get("pnl_pct")
        cancel = r.get("cancel_level")
        cancel_dist = r.get("cancel_dist_pct")
        source = r.get("price_source") or ""

        price_str = f"{price:,.2f}" if price else "—"
        origin_str = f"{origin:,.2f}" if origin else "—"
        pnl_str = f"{pnl_pct:+.1f}%" if pnl_pct is not None else "—"
        cancel_str = f"{cancel:,.2f}" if cancel else "—"
        cdist_str = f"{cancel_dist:+.1f}%" if cancel_dist is not None else "—"

        print(
            f"{ticker:<10} {signal:<6} {price_str:>12} {origin_str:>12} "
            f"{pnl_str:>8} {cancel_str:>12} {cdist_str:>8} {source:<8}"
        )

    print(f"\n{len(rows)} instruments")


def setup_logging(log_path: str):
    """Configure dual file + console logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(
                open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)
            ),
        ],
    )


def main():
    # Determine paths relative to project root (parent of this package)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_db = os.path.join(project_root, "nenner_signals.db")
    default_log = os.path.join(project_root, "nenner_engine.log")

    setup_logging(default_log)

    parser = argparse.ArgumentParser(
        description="Nenner Signal Engine - Parse and track Charles Nenner cycle signals",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  nenner-engine --backfill              Pull all historical emails from Gmail
  nenner-engine                         Check for new emails (incremental)
  nenner-engine --import-folder ./emls  Parse local .eml files
  nenner-engine --status                Show current signal state
  nenner-engine --history Gold          Show Gold signal history
  nenner-engine --export                Export database to CSV
  nenner-engine --rebuild-state         Rebuild current_state table
  nenner-engine --monitor               Start alert monitoring daemon
  nenner-engine --monitor --interval 30 Monitor with 30-second checks
  nenner-engine --alert-history         Show recent alert log
        """,
    )
    parser.add_argument("--backfill", action="store_true",
                        help="Pull all historical emails from Gmail via IMAP")
    parser.add_argument("--import-folder", type=str,
                        help="Import .eml files from a local folder")
    parser.add_argument("--status", action="store_true",
                        help="Show current signal state")
    parser.add_argument("--history", type=str,
                        help="Show signal history for an instrument (e.g., 'Gold', 'TSLA')")
    parser.add_argument("--export", action="store_true",
                        help="Export database tables to CSV files")
    parser.add_argument("--rebuild-state", action="store_true",
                        help="Rebuild current_state table from signal history")
    parser.add_argument("--fetch-prices", action="store_true",
                        help="Fetch daily closes from yFinance for all instruments")
    parser.add_argument("--prices", action="store_true",
                        help="Show current signal state with live prices and P/L")
    parser.add_argument("--monitor", action="store_true",
                        help="Start alert monitoring daemon (Ctrl+C to stop)")
    parser.add_argument("--interval", type=int, default=60,
                        help="Alert check interval in seconds (default: 60)")
    parser.add_argument("--alert-history", action="store_true",
                        help="Show recent alerts from alert_log")
    parser.add_argument("--db", type=str, default=default_db,
                        help=f"Database path (default: {default_db})")

    args = parser.parse_args()

    # Initialize database
    conn = init_db(args.db)
    migrate_db(conn)

    if args.monitor:
        run_monitor(conn, interval=args.interval)
    elif args.alert_history:
        show_alert_history(conn)
    elif args.rebuild_state:
        compute_current_state(conn)
        print("Current state rebuilt successfully.")
    elif args.fetch_prices:
        prices = fetch_yfinance_daily(conn)
        print(f"Fetched prices for {len(prices)} instruments.")
    elif args.prices:
        _show_prices(conn)
    elif args.status:
        show_status(conn)
    elif args.history:
        show_history(conn, args.history)
    elif args.export:
        export_csv(conn, base_dir=os.path.dirname(args.db))
    elif args.import_folder:
        import_eml_folder(conn, args.import_folder)
    elif args.backfill:
        backfill_imap(conn)
    else:
        # Default: incremental check
        check_new_emails(conn)

    conn.close()


if __name__ == "__main__":
    main()
