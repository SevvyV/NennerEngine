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
from .alerts import run_monitor, show_alert_history, AlertConfig
from .auto_cancel import check_auto_cancellations
from .positions import read_positions, get_positions_with_signal_context


def _show_positions(conn):
    """Display current trade positions with dollar P/L and Nenner signal context."""
    positions = read_positions()
    if not positions:
        print("No positions available (workbook may not be open).")
        return

    enriched = get_positions_with_signal_context(conn, positions)

    header = (
        f"{'Underlying':<10} {'Strategy':<14} {'Shares':>8} "
        f"{'Current':>12} {'Stock P/L':>12} {'Opt P/L':>12} "
        f"{'Total P/L':>12} {'Signal':<6} {'CxDist%':>8}"
    )
    print("=" * len(header))
    print(header)
    print("=" * len(header))

    grand_total = 0.0
    for p in enriched:
        underlying = p["underlying"]
        strategy = p["strategy"].replace("_", " ").title()
        # Sum shares across stock legs only
        stock_shares = sum(
            leg["shares"] for leg in p["legs"] if not leg["is_option"]
        )
        current = p.get("current_price")
        stock_pnl = p["stock_pnl_dollar"]
        opt_pnl = p["option_pnl_dollar"]
        total_pnl = p["total_pnl_dollar"]
        signal = p.get("nenner_signal") or "—"
        cdist = p.get("cancel_dist_pct")

        current_str = f"{current:,.2f}" if current else "—"
        shares_str = f"{stock_shares:,.0f}" if stock_shares else "—"
        stock_str = f"${stock_pnl:+,.0f}"
        opt_str = f"${opt_pnl:+,.0f}"
        total_str = f"${total_pnl:+,.0f}"
        cdist_str = f"{cdist:+.1f}%" if cdist is not None else "—"

        print(
            f"{underlying:<10} {strategy:<14} {shares_str:>8} "
            f"{current_str:>12} {stock_str:>12} {opt_str:>12} "
            f"{total_str:>12} {signal:<6} {cdist_str:>8}"
        )
        grand_total += total_pnl

    print("=" * len(header))
    print(f"{'TOTAL':>58} ${grand_total:+,.0f}")
    print(f"\n{len(enriched)} positions")


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
  nenner-engine --positions             Show trade positions with P/L
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
    parser.add_argument("--positions", action="store_true",
                        help="Show trade positions with dollar P/L")
    parser.add_argument("--auto-cancel", action="store_true",
                        help="Check daily closes against cancel levels and auto-cancel breached signals")
    parser.add_argument("--show-config", action="store_true",
                        help="Show current alert configuration")
    parser.add_argument("--stanley-teach", type=str, metavar="RULE",
                        help="Add a knowledge rule to Stanley's knowledge base")
    parser.add_argument("--stanley-knowledge", action="store_true",
                        help="List all rules in Stanley's knowledge base")
    parser.add_argument("--stanley-brief", action="store_true",
                        help="Generate a Stanley brief from the latest email (on-demand)")
    parser.add_argument("--stock-report", action="store_true",
                        help="Generate and email Stanley's Daily Stock Report")
    # --- Fischer subscription management ---
    parser.add_argument("--fischer-add-subscriber", nargs=4,
                        metavar=("EMAIL", "FIRST", "LAST", "PORTFOLIO"),
                        help="Add a Fischer subscriber: email first_name last_name portfolio_name")
    parser.add_argument("--fischer-remove-subscriber", type=str, metavar="EMAIL",
                        help="Deactivate a Fischer subscriber by email")
    parser.add_argument("--fischer-reactivate-subscriber", type=str, metavar="EMAIL",
                        help="Reactivate a deactivated Fischer subscriber by email")
    parser.add_argument("--fischer-list-subscribers", action="store_true",
                        help="List all Fischer subscribers")
    parser.add_argument("--fischer-add-portfolio", nargs=3,
                        metavar=("NAME", "LABEL", "TICKERS"),
                        help="Add a Fischer portfolio: name label 'AMZN,AVGO,IWM'")
    parser.add_argument("--fischer-list-portfolios", action="store_true",
                        help="List all Fischer portfolios")
    parser.add_argument("--fischer-refresh", type=str, metavar="EMAIL",
                        help="Manually trigger a Fischer refresh for a subscriber by email")

    parser.add_argument("--db", type=str, default=default_db,
                        help=f"Database path (default: {default_db})")

    args = parser.parse_args()

    # Initialize database
    conn = init_db(args.db)
    migrate_db(conn)

    if args.show_config:
        cfg = AlertConfig()
        print("\n  ALERT CONFIGURATION")
        print("  " + "=" * 50)
        print(f"  Windows Toast (audio):  {'ENABLED' if cfg.ENABLE_TOAST else 'DISABLED'}")
        print()
    elif args.stanley_teach:
        from .stanley import add_knowledge
        rule_text = args.stanley_teach
        category = "pattern"
        if ":" in rule_text and rule_text.split(":")[0] in (
            "pattern", "preference", "interpretation", "cross_instrument"
        ):
            category, rule_text = rule_text.split(":", 1)
            rule_text = rule_text.strip()
        rule_id = add_knowledge(conn, category, rule_text)
        print(f"Added knowledge rule #{rule_id}: [{category}] {rule_text}")
    elif args.stanley_knowledge:
        from .stanley import list_knowledge
        rules = list_knowledge(conn)
        if not rules:
            print("No knowledge rules. Add one with --stanley-teach \"rule text\"")
        else:
            print(f"\n  STANLEY KNOWLEDGE BASE ({len(rules)} rules)")
            print("  " + "=" * 60)
            for r in rules:
                status = "ACTIVE" if r["active"] else "inactive"
                instr = f" [{r['instrument']}]" if r["instrument"] else ""
                print(f"  #{r['id']:3d} [{r['category']}]{instr} ({status})")
                print(f"        {r['rule_text'][:80]}")
            print()
    elif args.stanley_brief:
        from .stanley import generate_brief_on_demand
        brief = generate_brief_on_demand(conn, args.db)
        # Handle Windows console encoding (cp1252 can't render all Unicode)
        sys.stdout.buffer.write(brief.encode("utf-8", errors="replace"))
        sys.stdout.buffer.write(b"\n")
    elif args.stock_report:
        from .stock_report import generate_stock_report_on_demand
        html = generate_stock_report_on_demand(conn, args.db)
        if html:
            print(f"Stock report generated and sent ({len(html)} chars)")
        else:
            print("Stock report generation failed — check logs")
    elif args.auto_cancel:
        results = check_auto_cancellations(conn)
        if results:
            print(f"\nAuto-cancelled {len(results)} signal(s):")
            for r in results:
                print(f"  {r['ticker']} ({r['instrument']}): "
                      f"{r['old_signal']} -> {r['new_signal']} "
                      f"(close={r['close_price']:.2f}, "
                      f"cancel={r['cancel_level']:.2f})")
        else:
            print("No cancel levels breached today.")
    elif args.fischer_add_subscriber:
        from .fischer_subscribers import add_subscriber
        email, first, last, portfolio = args.fischer_add_subscriber
        sid = add_subscriber(conn, email, first, last, portfolio)
        print(f"Added subscriber #{sid}: {first} {last} <{email}> -> {portfolio}")
    elif args.fischer_remove_subscriber:
        from .fischer_subscribers import deactivate_subscriber
        ok = deactivate_subscriber(conn, args.fischer_remove_subscriber)
        print(f"Deactivated: {args.fischer_remove_subscriber}" if ok else "Not found")
    elif args.fischer_reactivate_subscriber:
        from .fischer_subscribers import reactivate_subscriber
        ok = reactivate_subscriber(conn, args.fischer_reactivate_subscriber)
        print(f"Reactivated: {args.fischer_reactivate_subscriber}" if ok else "Not found")
    elif args.fischer_list_subscribers:
        from .fischer_subscribers import list_subscribers
        subs = list_subscribers(conn)
        if not subs:
            print("No subscribers. Add one with --fischer-add-subscriber")
        else:
            print(f"\n  FISCHER SUBSCRIBERS ({len(subs)})")
            print("  " + "=" * 60)
            for s in subs:
                status = "ACTIVE" if s["active"] else "inactive"
                print(f"  #{s['id']:3d} {s['first_name']} {s['last_name']} "
                      f"<{s['email']}> -> {s['portfolio_name']} "
                      f"({status}) max={s['max_daily_refreshes']}/day")
            print()
    elif args.fischer_add_portfolio:
        from .fischer_subscribers import add_portfolio
        name, label, tickers_csv = args.fischer_add_portfolio
        tickers = [t.strip() for t in tickers_csv.split(",")]
        add_portfolio(conn, name, label, tickers, {})
        # Share alloc is now computed dynamically from spot price at scan time
        print(f"Added portfolio '{name}' ({label}): {tickers}")
    elif args.fischer_list_portfolios:
        from .fischer_subscribers import list_portfolios
        ports = list_portfolios(conn)
        if not ports:
            print("No portfolios. Add one with --fischer-add-portfolio")
        else:
            print(f"\n  FISCHER PORTFOLIOS ({len(ports)})")
            print("  " + "=" * 60)
            for p in ports:
                conv = "yes" if p["show_conviction"] else "no"
                print(f"  {p['portfolio_name']}: {p['label']} "
                      f"-> {','.join(p['tickers'])} (conviction={conv})")
            print()
    elif args.fischer_refresh:
        from .fischer_subscribers import manual_refresh
        ok = manual_refresh(conn, args.fischer_refresh, db_path=args.db)
        print("Refresh sent" if ok else "Failed — check logs")
    elif args.positions:
        _show_positions(conn)
    elif args.monitor:
        run_monitor(conn, interval=args.interval, config=AlertConfig())
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
