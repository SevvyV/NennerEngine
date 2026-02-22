"""
Email Scheduler
================
Background thread that automatically checks for new Nenner emails:
  1. On engine/dashboard startup (immediate check)
  2. Every day at 8:00 AM Eastern Time
  3. Optionally on a recurring interval (e.g. every N minutes)
  4. Detects new trade initiations (direction changes) and sends Telegram alerts
  5. Sends daily "Top 10 Trades" report at configured times (9:00 AM, 10:30 AM ET)

Thread-safe: uses its own SQLite connection per check cycle.
"""

import logging
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("nenner")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DAILY_CHECK_HOUR = 8       # 8:00 AM
DAILY_CHECK_MINUTE = 0     # :00
DAILY_CHECK_TZ = "US/Eastern"

# Interval check window (Eastern Time) -- only check during these hours
INTERVAL_WINDOW_START = 8   # 8:00 AM ET
INTERVAL_WINDOW_END = 11    # 11:00 AM ET (exclusive -- last check fires <=10:30)

# How often the scheduler thread wakes up to see if it's time (seconds)
_TICK_INTERVAL = 30

# Daily Top 10 Trades report times (hour, minute) in Eastern Time
DAILY_REPORT_TIMES = [(9, 0), (10, 30)]

# Auto-cancel: run after market close to catch breached cancel levels
AUTO_CANCEL_HOUR = 16       # 4:30 PM ET
AUTO_CANCEL_MINUTE = 30


# ---------------------------------------------------------------------------
# Timezone helper (stdlib-only via zoneinfo, Python 3.9+)
# ---------------------------------------------------------------------------

def _now_eastern() -> datetime:
    """Return current datetime in US/Eastern."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(DAILY_CHECK_TZ))
    except ImportError:
        # Fallback: assume UTC-5 (EST) -- close enough for scheduling
        return datetime.utcnow() - timedelta(hours=5)


# ---------------------------------------------------------------------------
# Trade Direction Change Detection
# ---------------------------------------------------------------------------

def _snapshot_current_state(conn: sqlite3.Connection) -> dict[str, dict]:
    """Take a snapshot of current_state: {ticker: {effective_signal, instrument, ...}}.

    Used for before/after comparison to detect new trades.
    """
    old_rf = conn.row_factory
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT ticker, instrument, asset_class, effective_signal,
               origin_price, cancel_level
        FROM current_state
    """).fetchall()
    conn.row_factory = old_rf
    return {r["ticker"]: dict(r) for r in rows}


def _detect_direction_changes(before: dict, after: dict) -> list[dict]:
    """Compare before/after state snapshots to find direction changes.

    A direction change is when effective_signal flips (BUY->SELL or SELL->BUY),
    indicating a new trade has been initiated.

    Returns list of change dicts:
        [{"ticker": str, "instrument": str, "old_signal": str, "new_signal": str,
          "origin_price": float, "cancel_level": float}]
    """
    changes = []

    for ticker, after_state in after.items():
        before_state = before.get(ticker)
        new_signal = after_state.get("effective_signal")

        if before_state is None:
            # New ticker appeared -- treat as a new trade
            if new_signal in ("BUY", "SELL"):
                changes.append({
                    "ticker": ticker,
                    "instrument": after_state.get("instrument", ticker),
                    "asset_class": after_state.get("asset_class", ""),
                    "old_signal": "NEW",
                    "new_signal": new_signal,
                    "origin_price": after_state.get("origin_price"),
                    "cancel_level": after_state.get("cancel_level"),
                })
            continue

        old_signal = before_state.get("effective_signal")
        if old_signal != new_signal and new_signal in ("BUY", "SELL"):
            changes.append({
                "ticker": ticker,
                "instrument": after_state.get("instrument", ticker),
                "asset_class": after_state.get("asset_class", ""),
                "old_signal": old_signal or "NONE",
                "new_signal": new_signal,
                "origin_price": after_state.get("origin_price"),
                "cancel_level": after_state.get("cancel_level"),
            })

    return changes


def _send_trade_alerts(changes: list[dict], db_path: str):
    """Send Telegram alerts for each direction change (new trade initiated).

    Format:
        [green/red circle] NEW TRADE: TICKER
        Instrument Name
        BUY from 2,850.00 (was SELL)
        Cancel at 2,800.00
    """
    if not changes:
        return

    try:
        from .alerts import get_telegram_config, send_telegram, AlertConfig
        config = AlertConfig()
        if not config.ENABLE_TELEGRAM:
            return

        token, chat_id = get_telegram_config()
        if not token or not chat_id:
            log.warning("Trade alerts: Telegram not configured, skipping")
            return

        for change in changes:
            emoji = "\U0001f7e2" if change["new_signal"] == "BUY" else "\U0001f534"
            origin_str = (f"{change['origin_price']:,.2f}"
                          if change["origin_price"] else "?")
            cancel_str = (f"{change['cancel_level']:,.2f}"
                          if change["cancel_level"] else "?")
            was_str = f"was {change['old_signal']}" if change["old_signal"] != "NEW" else "new instrument"

            msg = (
                f"{emoji} <b>NEW TRADE: {change['ticker']}</b>\n"
                f"{change['instrument']}\n"
                f"<b>{change['new_signal']}</b> from {origin_str} ({was_str})\n"
                f"Cancel at {cancel_str}"
            )
            send_telegram(msg, token, chat_id)
            log.info(f"Trade alert sent: {change['ticker']} "
                     f"{change['old_signal']} -> {change['new_signal']}")

    except Exception as e:
        log.error(f"Trade alert send failed: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Daily Top 10 Trades Report
# ---------------------------------------------------------------------------

def _send_daily_top_trades(db_path: str):
    """Build and send the daily Top 10 Trades report via Telegram.

    Opens its own DB connection, calls build_top_trades_message(), sends.
    """
    try:
        from .alerts import get_telegram_config, send_telegram, AlertConfig
        from .trade_stats import build_top_trades_message
        from .db import init_db, migrate_db

        config = AlertConfig()
        if not config.ENABLE_TELEGRAM:
            return

        token, chat_id = get_telegram_config()
        if not token or not chat_id:
            log.warning("Daily report: Telegram not configured, skipping")
            return

        conn = init_db(db_path)
        migrate_db(conn)

        msg = build_top_trades_message(conn, limit=10)
        conn.close()

        if msg:
            send_telegram(msg, token, chat_id)
            log.info("Daily Top 10 Trades report sent via Telegram")
        else:
            log.info("Daily Top 10 report: insufficient data, skipped")

    except Exception as e:
        log.error(f"Daily Top 10 report failed: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Auto-Cancel (breached cancel levels)
# ---------------------------------------------------------------------------

def _run_auto_cancel(db_path: str, date_str: Optional[str] = None):
    """Run auto-cancellation check for breached cancel levels.

    Fetches closing prices, checks if any cancel levels were breached,
    inserts CANCELLED signals, rebuilds current_state, and sends Telegram
    alerts for any resulting direction changes.

    Args:
        db_path: Path to nenner_signals.db
        date_str: Date to check (YYYY-MM-DD). If None, uses today.
    """
    try:
        from .db import init_db, migrate_db, compute_current_state
        from .auto_cancel import check_auto_cancellations

        conn = init_db(db_path)
        migrate_db(conn)

        # Snapshot before
        state_before = _snapshot_current_state(conn)

        if date_str is None:
            date_str = _now_eastern().strftime("%Y-%m-%d")

        results = check_auto_cancellations(conn, date_str)

        if results:
            log.info(f"Auto-cancel: {len(results)} cancellation(s) on {date_str}")
            compute_current_state(conn)

            # Detect direction changes and send alerts
            state_after = _snapshot_current_state(conn)
            changes = _detect_direction_changes(state_before, state_after)
            if changes:
                log.info(f"Auto-cancel: {len(changes)} direction change(s)")
                _send_trade_alerts(changes, db_path)
        else:
            log.info(f"Auto-cancel: no breaches on {date_str}")

        conn.close()

    except Exception as e:
        log.error(f"Auto-cancel failed: {e}", exc_info=True)


def _run_auto_cancel_catchup(db_path: str):
    """Run auto-cancel for recent trading days to catch up.

    Checks the last 3 trading days to cover weekends and holidays
    when the dashboard wasn't running.
    """
    now_et = _now_eastern()
    for days_back in range(3):
        check_date = now_et - timedelta(days=days_back)
        # Skip weekends
        if check_date.weekday() >= 5:
            continue
        date_str = check_date.strftime("%Y-%m-%d")
        log.info(f"Auto-cancel catchup: checking {date_str}")
        _run_auto_cancel(db_path, date_str)


# ---------------------------------------------------------------------------
# Core check function
# ---------------------------------------------------------------------------

def run_email_check(db_path: str) -> dict:
    """Run an incremental email check using its own DB connection.

    Also detects direction changes (new trades) and sends Telegram alerts.

    Returns a summary dict:
        {"new_emails": int, "error": str|None, "timestamp": str,
         "trade_changes": list[dict]}
    """
    from .db import init_db, migrate_db, compute_current_state
    from .imap_client import check_new_emails

    result = {
        "new_emails": 0,
        "error": None,
        "timestamp": datetime.now().isoformat(),
        "trade_changes": [],
    }

    conn = None
    try:
        conn = init_db(db_path)
        migrate_db(conn)

        # Snapshot BEFORE email check
        state_before = _snapshot_current_state(conn)

        # Capture log output to count new emails
        log.info("Email scheduler: checking for new Nenner emails...")
        check_new_emails(conn)

        # Count emails parsed in last 5 minutes as proxy for "new"
        row = conn.execute("""
            SELECT COUNT(*) FROM emails
            WHERE date_parsed >= datetime('now', '-5 minutes')
        """).fetchone()
        new_count = row[0] if row else 0
        result["new_emails"] = new_count

        if new_count > 0:
            log.info(f"Email scheduler: {new_count} new email(s) found, rebuilding state...")
            compute_current_state(conn)
            log.info("Email scheduler: state rebuilt successfully")

            # Snapshot AFTER state rebuild — detect direction changes
            state_after = _snapshot_current_state(conn)
            changes = _detect_direction_changes(state_before, state_after)
            result["trade_changes"] = changes

            if changes:
                log.info(f"Email scheduler: {len(changes)} direction change(s) detected")
                _send_trade_alerts(changes, db_path)
        else:
            log.info("Email scheduler: no new emails")

    except Exception as e:
        result["error"] = str(e)
        log.error(f"Email scheduler error: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

    return result


# ---------------------------------------------------------------------------
# Scheduler thread
# ---------------------------------------------------------------------------

class EmailScheduler:
    """Background thread that checks for Nenner emails on startup and daily at 8 AM ET.

    Also sends daily Top 10 Trades reports at configured times (9:00 AM, 10:30 AM ET).
    """

    def __init__(self, db_path: str, check_on_start: bool = True,
                 daily_check: bool = True, interval_minutes: Optional[int] = None,
                 interval_window: Optional[tuple[int, int]] = None):
        """
        Args:
            db_path:           Path to nenner_signals.db
            check_on_start:    Run an email check immediately on start
            daily_check:       Enable the 8:00 AM ET daily check
            interval_minutes:  If set, also check every N minutes (in addition to daily)
            interval_window:   (start_hour, end_hour) in ET -- interval checks only
                               fire within this window. Defaults to (8, 11).
        """
        self.db_path = db_path
        self.check_on_start = check_on_start
        self.daily_check = daily_check
        self.interval_minutes = interval_minutes
        self.interval_window = interval_window or (INTERVAL_WINDOW_START, INTERVAL_WINDOW_END)

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_daily_date: Optional[str] = None  # "YYYY-MM-DD" of last daily check
        self._last_interval_check: Optional[datetime] = None
        self._last_result: Optional[dict] = None
        self._lock = threading.Lock()

        # Track daily report sends: set of "YYYY-MM-DD_HH:MM" keys
        self._sent_reports: set[str] = set()
        # Track auto-cancel: date string of last run
        self._last_auto_cancel_date: Optional[str] = None

    @property
    def last_result(self) -> Optional[dict]:
        with self._lock:
            return self._last_result

    def _set_result(self, result: dict):
        with self._lock:
            self._last_result = result

    def _do_check(self, reason: str) -> dict:
        """Execute an email check and store the result."""
        log.info(f"Email scheduler: triggered ({reason})")
        result = run_email_check(self.db_path)
        result["trigger"] = reason
        self._set_result(result)
        return result

    def _check_daily_reports(self, now_et: datetime):
        """Check if it's time to send a daily Top 10 Trades report.

        Fires at each configured time in DAILY_REPORT_TIMES, once per day per slot.
        """
        for report_hour, report_minute in DAILY_REPORT_TIMES:
            # 5-minute tolerance window
            if (now_et.hour == report_hour
                    and report_minute <= now_et.minute < report_minute + 5):
                report_key = f"{now_et.strftime('%Y-%m-%d')}_{report_hour:02d}:{report_minute:02d}"
                if report_key not in self._sent_reports:
                    self._sent_reports.add(report_key)
                    log.info(f"Email scheduler: sending daily Top 10 report "
                             f"({report_hour}:{report_minute:02d} ET)")
                    _send_daily_top_trades(self.db_path)

    def _check_auto_cancel(self, now_et: datetime):
        """Check if it's time to run auto-cancel (4:30 PM ET, once per day)."""
        if (now_et.hour == AUTO_CANCEL_HOUR
                and AUTO_CANCEL_MINUTE <= now_et.minute < AUTO_CANCEL_MINUTE + 5):
            today_str = now_et.strftime("%Y-%m-%d")
            if self._last_auto_cancel_date != today_str:
                self._last_auto_cancel_date = today_str
                log.info(f"Auto-cancel: running daily check for {today_str}")
                _run_auto_cancel(self.db_path, today_str)

    def _run(self):
        """Main scheduler loop."""
        # --- Startup check ---
        if self.check_on_start:
            self._do_check("startup")

        # --- Startup auto-cancel catchup (covers missed days) ---
        log.info("Auto-cancel: running startup catchup")
        _run_auto_cancel_catchup(self.db_path)

        # --- Loop for daily and interval checks ---
        while not self._stop_event.is_set():
            try:
                now_et = _now_eastern()

                # Daily 8:00 AM ET check
                if self.daily_check:
                    today_str = now_et.strftime("%Y-%m-%d")
                    if (now_et.hour == DAILY_CHECK_HOUR
                            and now_et.minute >= DAILY_CHECK_MINUTE
                            and now_et.minute < DAILY_CHECK_MINUTE + 5
                            and self._last_daily_date != today_str):
                        self._last_daily_date = today_str
                        self._do_check(f"daily_8am_ET ({today_str})")

                # Interval-based check (only within the ET window, e.g. 8-11 AM)
                if self.interval_minutes:
                    win_start, win_end = self.interval_window
                    in_window = win_start <= now_et.hour < win_end
                    if in_window:
                        now = datetime.now()
                        if (self._last_interval_check is None
                                or now - self._last_interval_check
                                >= timedelta(minutes=self.interval_minutes)):
                            self._last_interval_check = now
                            # Skip if we just did a startup or daily check
                            last = self.last_result
                            if last and (datetime.now() - datetime.fromisoformat(last["timestamp"])
                                         < timedelta(minutes=2)):
                                pass  # too recent, skip
                            else:
                                self._do_check(
                                    f"interval_{self.interval_minutes}m "
                                    f"({now_et.strftime('%H:%M')} ET)"
                                )

                # Daily Top 10 Trades report (independent of email check)
                self._check_daily_reports(now_et)

                # Auto-cancel: 4:30 PM ET daily after market close
                self._check_auto_cancel(now_et)

            except Exception as e:
                log.error(f"Email scheduler loop error: {e}", exc_info=True)

            # Sleep in small increments for responsive shutdown
            self._stop_event.wait(timeout=_TICK_INTERVAL)

    def start(self):
        """Start the scheduler background thread."""
        if self._thread and self._thread.is_alive():
            log.warning("Email scheduler already running")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="NennerEmailScheduler",
            daemon=True,  # dies when main process exits
        )
        self._thread.start()
        log.info("Email scheduler started (daemon thread)")

    def stop(self):
        """Signal the scheduler to stop."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
            log.info("Email scheduler stopped")

    def trigger_now(self) -> dict:
        """Manually trigger an email check (from any thread)."""
        return self._do_check("manual")
