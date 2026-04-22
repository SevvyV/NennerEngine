"""Dashboard lifecycle — argparse, schema migration, background threads, app.run.

Calling main() boots the whole dashboard process: applies DB migrations,
starts the alert monitor and equity-stream threads, registers an atexit
hook to stop them cleanly, then hands control to Dash's dev server.

The email scheduler is intentionally NOT started here — the external
NennerEngineMonitor process owns it. Running it in both processes caused
duplicate stock reports.
"""

import argparse
import logging
import os
import threading

from . import app as _app_module
from . import data as _data

log = logging.getLogger("nenner_engine")


def main():
    parser = argparse.ArgumentParser(description="Nenner Signal Dashboard")
    parser.add_argument("--port", type=int, default=8050, help="Port (default: 8050)")
    parser.add_argument("--db", type=str, default=None, help="Database path")
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    args = parser.parse_args()

    if args.db:
        # Mutate the data module's DB_PATH so every callback opens the
        # right database. This must happen BEFORE any callback runs.
        _data.DB_PATH = args.db

    # Configure logging for monitor threads (logger "nenner" used by all NE modules)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print(f"Starting Nenner Signal Dashboard on http://127.0.0.1:{args.port}")
    print(f"Database: {_data.DB_PATH}")

    # Apply schema migrations once at startup — callback connections rely
    # on the schema being current but shouldn't pay the migration cost on
    # every request.
    try:
        from nenner_engine.db import init_db, migrate_db
        _startup_conn = init_db(_data.DB_PATH)
        migrate_db(_startup_conn)
        _startup_conn.close()
        log.info("Schema migrations applied")
    except Exception as e:
        log.error(f"Schema migration failed at startup: {e}", exc_info=True)
        raise

    # Start background monitor threads (alert evaluator + email scheduler).
    # Only in non-debug mode — Werkzeug's reloader forks a child process
    # and we don't want duplicate threads.
    if not args.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        import atexit
        from nenner_engine.alerts import AlertMonitorThread, AlertConfig
        from nenner_engine.equity_stream import EquityStreamThread

        _app_module._alert_monitor = AlertMonitorThread(
            db_path=_data.DB_PATH, interval=60, config=AlertConfig(),
        )
        _app_module._alert_monitor.start()
        log.info("Alert monitor thread started (60s interval)")

        # Email scheduler disabled in dashboard — NennerEngineMonitor owns it.
        # Running it in both processes caused duplicate stock reports (race on
        # alert_log dedup guard, both fire at 8:30 AM before either writes).
        log.info("Email scheduler disabled (owned by NennerEngineMonitor)")

        # DataBento equity stream — live spot prices for watchlist ETFs/equities
        _eq_stop = threading.Event()
        _app_module._equity_stream = EquityStreamThread(
            stop_event=_eq_stop, db_path=_data.DB_PATH,
        )
        _app_module._equity_stream.start()
        log.info("Equity stream thread started (DataBento EQUS.MINI)")

        def _shutdown():
            log.info("Dashboard shutting down — stopping background threads")
            if _app_module._alert_monitor:
                _app_module._alert_monitor.stop()
            if _app_module._email_sched:
                _app_module._email_sched.stop()
            if _app_module._equity_stream:
                _eq_stop.set()
                _app_module._equity_stream.join(timeout=10)

        atexit.register(_shutdown)

    _app_module.app.run(debug=args.debug, port=args.port)
