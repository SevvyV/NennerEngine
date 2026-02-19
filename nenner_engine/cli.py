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
    parser.add_argument("--db", type=str, default=default_db,
                        help=f"Database path (default: {default_db})")

    args = parser.parse_args()

    # Initialize database
    conn = init_db(args.db)
    migrate_db(conn)

    if args.rebuild_state:
        compute_current_state(conn)
        print("Current state rebuilt successfully.")
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
