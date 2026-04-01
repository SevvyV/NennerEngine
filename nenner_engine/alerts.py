"""
Alert Engine
=============
Monitors price data and fires notifications when alert conditions are met.
Uses an evaluator registry so new alert types can be added without touching
the monitor loop.

Usage:
    python -m nenner_engine --monitor
    python -m nenner_engine --monitor --interval 30
    python -m nenner_engine --alert-history

Adding a new alert type:
    @register_evaluator
    def check_something(conn, prices):
        '''prices = {ticker: float}'''
        alerts = []
        if some_condition:
            alerts.append(make_alert("TKR", "Instrument", "MY_TYPE",
                                     "DANGER", "message", price))
        return alerts
"""

import logging
import signal as signal_mod
import sqlite3
import threading
import time
from datetime import datetime, timedelta, time as dt_time
from typing import Optional

from .alert_dispatch import (  # noqa: F401 — re-export for backwards compat
    get_telegram_config, send_toast, send_telegram,
    notify_fischer_refresh, log_alert, is_cooled_down,
    ALERT_COOLDOWN_MINUTES,
)

log = logging.getLogger("nenner")

PROXIMITY_DANGER_PCT = 0.5    # Cancel distance < 0.5% = DANGER
PROXIMITY_WARNING_PCT = 1.0   # Cancel distance < 1.0% = WATCH


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class AlertConfig:
    """Alert channel and behavior settings."""
    ENABLE_TOAST = False


# ---------------------------------------------------------------------------
# Alert Construction
# ---------------------------------------------------------------------------

def make_alert(ticker: str, instrument: str, alert_type: str,
               severity: str, message: str,
               current_price: Optional[float] = None, **extra) -> dict:
    """Build a standardized alert dict."""
    alert = {
        "ticker": ticker,
        "instrument": instrument,
        "alert_type": alert_type,
        "severity": severity,
        "message": message,
        "current_price": current_price,
        "cancel_dist_pct": extra.get("cancel_dist_pct"),
        "trigger_dist_pct": extra.get("trigger_dist_pct"),
        "effective_signal": extra.get("effective_signal"),
    }
    return alert


# ---------------------------------------------------------------------------
# Evaluator Registry
# ---------------------------------------------------------------------------

_evaluators: list = []


def register_evaluator(fn):
    """Register an alert evaluator: (conn, prices) -> list[alert_dict].

    Evaluators are called each monitor tick with the DB connection and
    a {ticker: price} dict. They return a list of alert dicts (or empty).
    """
    _evaluators.append(fn)
    return fn


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def dispatch_alert(alert: dict, cooldown_tracker: dict,
                   conn: sqlite3.Connection,
                   config: Optional[AlertConfig] = None) -> bool:
    """Check cooldown, send via enabled channels, log to DB.

    Returns True if alert was dispatched (not suppressed by cooldown).
    """
    if config is None:
        config = AlertConfig()

    ticker = alert["ticker"]
    alert_type = alert["alert_type"]

    if not is_cooled_down(cooldown_tracker, ticker, alert_type):
        log.debug(f"Cooldown active for {ticker}/{alert_type}, suppressing")
        return False

    channels_sent = []

    if config.ENABLE_TOAST:
        title = f"Nenner {alert['severity']}: {ticker}"
        if send_toast(title, alert["message"], alert["severity"]):
            channels_sent.append("toast")

    log_alert(conn, alert, channels_sent)
    cooldown_tracker[(ticker, alert_type)] = datetime.now()

    channels_str = ",".join(channels_sent) or "log-only"
    log.info(f"ALERT [{alert['severity']}] {alert['message']} -> {channels_str}")
    return True


# ---------------------------------------------------------------------------
# Built-in Evaluators
# ---------------------------------------------------------------------------

def evaluate_price_alerts(rows: list[dict]) -> list[dict]:
    """Evaluate cancel proximity alerts from price context rows.

    Args:
        rows: Output of get_prices_with_signal_context().
    """
    alerts = []
    for r in rows:
        ticker = r["ticker"]
        instrument = r.get("instrument", ticker)
        price = r.get("price")
        signal = r.get("effective_signal", "")

        if price is None:
            continue

        cancel_dist = r.get("cancel_dist_pct")
        if cancel_dist is not None:
            abs_dist = abs(cancel_dist)
            cancel_level = r.get("cancel_level")
            cancel_str = f"{cancel_level:,.2f}" if cancel_level else "?"

            if abs_dist < PROXIMITY_DANGER_PCT:
                alerts.append(make_alert(
                    ticker, instrument, "CANCEL_DANGER", "DANGER",
                    f"DANGER {ticker} ({instrument}) cancel {abs_dist:.2f}% away! "
                    f"Price={price:,.2f} Cancel={cancel_str} Signal={signal}",
                    price, cancel_dist_pct=cancel_dist, effective_signal=signal,
                ))
            elif abs_dist < PROXIMITY_WARNING_PCT:
                alerts.append(make_alert(
                    ticker, instrument, "CANCEL_WATCH", "WARNING",
                    f"WATCH {ticker} ({instrument}) cancel {abs_dist:.2f}% away. "
                    f"Price={price:,.2f} Cancel={cancel_str} Signal={signal}",
                    price, cancel_dist_pct=cancel_dist, effective_signal=signal,
                ))
    return alerts


def evaluate_custom_price_alerts(conn: sqlite3.Connection,
                                 price_by_ticker: dict[str, float]) -> list[dict]:
    """Check custom price alerts against current prices.

    Fires once per direction (above/below), then marks fired so it won't
    re-fire until the next trading day reset.
    """
    alerts = []
    rows = conn.execute(
        "SELECT * FROM custom_price_alerts WHERE active = 1"
    ).fetchall()

    for row in rows:
        ticker = row["ticker"]
        price = price_by_ticker.get(ticker)
        if price is None:
            continue

        above = row["above"]
        below = row["below"]
        note = row["note"] or ""

        if above and price >= above and not row["fired_above"]:
            alerts.append(make_alert(
                ticker, ticker, "CUSTOM_ABOVE", "DANGER",
                f"\U0001f6a8 {ticker} hit ${price:,.2f} — "
                f"ABOVE ${above:,.2f} threshold\n{note}",
                price,
            ))
            conn.execute(
                "UPDATE custom_price_alerts SET fired_above = 1 WHERE id = ?",
                (row["id"],))
            conn.commit()

        if below and price <= below and not row["fired_below"]:
            alerts.append(make_alert(
                ticker, ticker, "CUSTOM_BELOW", "DANGER",
                f"\U0001f6a8 {ticker} hit ${price:,.2f} — "
                f"BELOW ${below:,.2f} threshold\n{note}",
                price,
            ))
            conn.execute(
                "UPDATE custom_price_alerts SET fired_below = 1 WHERE id = ?",
                (row["id"],))
            conn.commit()

    return alerts


@register_evaluator
def _eval_custom_price_alerts(conn, prices):
    """Built-in evaluator: custom price alerts."""
    return evaluate_custom_price_alerts(conn, prices)


# ---------------------------------------------------------------------------
# Signal Change Detection (public API, not registered by default)
# ---------------------------------------------------------------------------

def detect_signal_changes(conn: sqlite3.Connection,
                          last_seen_id: int) -> tuple[list[dict], int]:
    """Detect new signals since last_seen_id.

    Returns (alerts_list, new_max_id).
    Not registered as an evaluator — call directly if needed.
    """
    rows = conn.execute("""
        SELECT id, date, instrument, ticker, signal_type, signal_status,
               origin_price, cancel_level, trigger_level
        FROM signals WHERE id > ? ORDER BY id ASC
    """, (last_seen_id,)).fetchall()

    if not rows:
        return [], last_seen_id

    alerts = []
    new_max_id = last_seen_id

    for row in rows:
        new_max_id = max(new_max_id, row["id"])
        ticker = row["ticker"]
        instrument = row["instrument"]
        sig_type = row["signal_type"]
        sig_status = row["signal_status"]
        origin = row["origin_price"]
        cancel = row["cancel_level"]

        if sig_status == "ACTIVE":
            origin_str = f"{origin:,.2f}" if origin else "?"
            cancel_str = f"{cancel:,.2f}" if cancel else "?"
            message = (
                f"NEW SIGNAL {ticker} ({instrument}) {sig_type} activated "
                f"from {origin_str}. Cancel at {cancel_str}"
            )
        elif sig_status == "CANCELLED":
            cancel_str = f"{cancel:,.2f}" if cancel else "?"
            message = (
                f"CANCELLED {ticker} ({instrument}) {sig_type} cancelled "
                f"at {cancel_str}. Implies reversal."
            )
        else:
            message = f"SIGNAL {ticker} ({instrument}) {sig_type} {sig_status}"

        alerts.append(make_alert(
            ticker, instrument, "SIGNAL_CHANGE", "INFO", message,
            origin, effective_signal=f"{sig_type}_{sig_status}",
        ))

    return alerts, new_max_id


# ---------------------------------------------------------------------------
# Alert History
# ---------------------------------------------------------------------------

def show_alert_history(conn: sqlite3.Connection, limit: int = 50):
    """Display recent alerts from the alert_log table."""
    rows = conn.execute("""
        SELECT created_at, ticker, instrument, alert_type, severity,
               message, current_price, channels_sent
        FROM alert_log ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall()

    if not rows:
        print("No alerts recorded yet.")
        return

    print(f"\n{'=' * 110}")
    print("  ALERT HISTORY (most recent first)")
    print(f"{'=' * 110}")
    print(f"  {'Time':<20} {'Ticker':<8} {'Type':<16} {'Sev':<8} "
          f"{'Channels':<15} {'Message'}")
    print("  " + "-" * 105)

    for r in rows:
        msg = r["message"]
        if len(msg) > 55:
            msg = msg[:55] + "..."
        print(f"  {r['created_at']:<20} {r['ticker']:<8} {r['alert_type']:<16} "
              f"{r['severity']:<8} {r['channels_sent'] or 'none':<15} {msg}")

    print(f"\n  Showing {len(rows)} entries")


# ---------------------------------------------------------------------------
# Alert Monitor Thread (used by dashboard single-process mode)
# ---------------------------------------------------------------------------

class AlertMonitorThread:
    """Daemon thread that runs the alert evaluation loop.

    Polls prices every `interval` seconds, runs all registered evaluators,
    dispatches alerts via cooldown tracker. Opens its own DB connection
    for thread safety.
    """

    def __init__(self, db_path: str, interval: int = 60,
                 config: Optional[AlertConfig] = None):
        self.db_path = db_path
        self.interval = interval
        self.config = config or AlertConfig()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _run(self):
        from .db import init_db, migrate_db
        from .prices import get_prices_with_signal_context

        conn = init_db(self.db_path)
        migrate_db(conn)

        cooldown_tracker: dict[tuple[str, str], datetime] = {}
        custom_alert_reset_date: Optional[str] = None
        check_count = 0

        log.info(f"AlertMonitorThread started. Interval={self.interval}s, "
                 f"evaluators={len(_evaluators)}")

        while not self._stop_event.is_set():
            try:
                check_count += 1
                now = datetime.now()

                rows = get_prices_with_signal_context(conn, try_t1=True)
                price_by_ticker = {
                    r["ticker"]: r.get("price")
                    for r in rows if r.get("price")
                }

                # Reset custom alert fired flags once per trading day
                today_iso = now.date().isoformat()
                if custom_alert_reset_date != today_iso:
                    conn.execute(
                        "UPDATE custom_price_alerts "
                        "SET fired_above = 0, fired_below = 0 "
                        "WHERE active = 1"
                    )
                    conn.commit()
                    custom_alert_reset_date = today_iso
                    log.info("Custom price alerts reset for new trading day")

                # Run all registered evaluators
                all_alerts = []
                for evaluator in _evaluators:
                    try:
                        all_alerts.extend(evaluator(conn, price_by_ticker))
                    except Exception as e:
                        log.error(f"Evaluator {evaluator.__name__} failed: {e}",
                                  exc_info=True)

                for alert in all_alerts:
                    dispatch_alert(alert, cooldown_tracker, conn, self.config)

            except Exception as e:
                log.error(f"AlertMonitorThread error: {e}", exc_info=True)

            self._stop_event.wait(timeout=self.interval)

        conn.close()
        log.info(f"AlertMonitorThread stopped after {check_count} checks")

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="AlertMonitor", daemon=True,
        )
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=30)


# ---------------------------------------------------------------------------
# Monitor Daemon (CLI entry point — blocks on main thread)
# ---------------------------------------------------------------------------

def run_monitor(conn: sqlite3.Connection, interval: int = 60,
                config: Optional[AlertConfig] = None):
    """Run the alert monitoring daemon.

    Polls every `interval` seconds, runs all registered evaluators,
    dispatches alerts, and handles graceful shutdown via Ctrl+C.

    The EmailScheduler (started here) owns auto-cancel, email checks,
    Fischer scans, and stock reports. This loop only handles price alerts.
    """
    from .prices import get_prices_with_signal_context

    if config is None:
        config = AlertConfig()

    # Graceful shutdown
    shutdown = False

    def handle_sigint(signum, frame):
        nonlocal shutdown
        shutdown = True
        print("\nShutting down alert monitor...")

    signal_mod.signal(signal_mod.SIGINT, handle_sigint)

    # Start email scheduler (owns auto-cancel, email checks, Fischer, stock reports)
    email_sched = None
    try:
        from .email_scheduler import EmailScheduler
        db_path = conn.execute("PRAGMA database_list").fetchone()[2]
        email_sched = EmailScheduler(
            db_path=db_path, check_on_start=True, daily_check=True,
            interval_minutes=15,
        )
        email_sched.start()
        log.info("Email scheduler active (startup + daily 8:35 AM ET + every 15m 8-11 AM ET)")
    except Exception as e:
        log.warning(f"Email scheduler failed to start: {e}")

    cooldown_tracker: dict[tuple[str, str], datetime] = {}
    custom_alert_reset_date: Optional[str] = None
    scheduler_death_logged = False  # only log once per death

    log.info(f"Alert monitor started. Interval={interval}s, "
             f"evaluators={len(_evaluators)}")

    check_count = 0
    total_alerts = 0

    while not shutdown:
        try:
            check_count += 1
            now = datetime.now()
            log.debug(f"--- Alert check #{check_count} at {now.strftime('%H:%M:%S')} ---")

            # Health check: restart email scheduler if its thread died
            if email_sched and not email_sched._thread.is_alive():
                if not scheduler_death_logged:
                    log.error("Email scheduler thread died — restarting")
                    scheduler_death_logged = True
                try:
                    email_sched.start()
                    log.info("Email scheduler thread restarted successfully")
                    scheduler_death_logged = False
                except Exception as e:
                    log.error(f"Email scheduler restart failed: {e}")

            # Fetch prices
            rows = get_prices_with_signal_context(conn, try_t1=True)
            price_by_ticker = {
                r["ticker"]: r.get("price")
                for r in rows if r.get("price")
            }

            # Reset custom alert fired flags once per trading day
            today_iso = now.date().isoformat()
            if custom_alert_reset_date != today_iso:
                conn.execute(
                    "UPDATE custom_price_alerts "
                    "SET fired_above = 0, fired_below = 0 "
                    "WHERE active = 1"
                )
                conn.commit()
                custom_alert_reset_date = today_iso
                log.info("Custom price alerts reset for new trading day")

            # Run all registered evaluators
            all_alerts = []
            for evaluator in _evaluators:
                try:
                    all_alerts.extend(evaluator(conn, price_by_ticker))
                except Exception as e:
                    log.error(f"Evaluator {evaluator.__name__} failed: {e}",
                              exc_info=True)

            # Dispatch
            fired = 0
            for alert in all_alerts:
                if dispatch_alert(alert, cooldown_tracker, conn, config):
                    fired += 1

            total_alerts += fired
            if fired:
                log.info(f"Fired {fired} alerts this check "
                         f"({total_alerts} total)")

        except KeyboardInterrupt:
            break
        except Exception as e:
            log.error(f"Error in alert check: {e}", exc_info=True)

        # Sleep in 1s increments for responsive Ctrl+C
        for _ in range(interval):
            if shutdown:
                break
            time.sleep(1)

    if email_sched:
        email_sched.stop()

    log.info(f"Alert monitor stopped. {check_count} checks, "
             f"{total_alerts} alerts fired.")
