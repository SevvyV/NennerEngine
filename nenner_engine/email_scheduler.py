"""
Email Scheduler
================
Background thread that automatically checks for new Nenner emails:
  1. On engine/dashboard startup (immediate check)
  2. Every day at 8:00 AM Eastern Time
  3. Optionally on a recurring interval (e.g. every N minutes)

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

# How often the scheduler thread wakes up to see if it's time (seconds)
_TICK_INTERVAL = 30

# ---------------------------------------------------------------------------
# Timezone helper (stdlib-only via zoneinfo, Python 3.9+)
# ---------------------------------------------------------------------------

def _now_eastern() -> datetime:
    """Return current datetime in US/Eastern."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(DAILY_CHECK_TZ))
    except ImportError:
        # Fallback: assume UTC-5 (EST) — close enough for scheduling
        return datetime.utcnow() - timedelta(hours=5)


# ---------------------------------------------------------------------------
# Core check function
# ---------------------------------------------------------------------------

def run_email_check(db_path: str) -> dict:
    """Run an incremental email check using its own DB connection.

    Returns a summary dict:
        {"new_emails": int, "error": str|None, "timestamp": str}
    """
    from .db import init_db, migrate_db, compute_current_state
    from .imap_client import check_new_emails

    result = {"new_emails": 0, "error": None, "timestamp": datetime.now().isoformat()}

    conn = None
    try:
        conn = init_db(db_path)
        migrate_db(conn)

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
    """Background thread that checks for Nenner emails on startup and daily at 8 AM ET."""

    def __init__(self, db_path: str, check_on_start: bool = True,
                 daily_check: bool = True, interval_minutes: Optional[int] = None):
        """
        Args:
            db_path:           Path to nenner_signals.db
            check_on_start:    Run an email check immediately on start
            daily_check:       Enable the 8:00 AM ET daily check
            interval_minutes:  If set, also check every N minutes (in addition to daily)
        """
        self.db_path = db_path
        self.check_on_start = check_on_start
        self.daily_check = daily_check
        self.interval_minutes = interval_minutes

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_daily_date: Optional[str] = None  # "YYYY-MM-DD" of last daily check
        self._last_interval_check: Optional[datetime] = None
        self._last_result: Optional[dict] = None
        self._lock = threading.Lock()

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

    def _run(self):
        """Main scheduler loop."""
        # --- Startup check ---
        if self.check_on_start:
            self._do_check("startup")

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

                # Interval-based check
                if self.interval_minutes:
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
                            self._do_check(f"interval_{self.interval_minutes}m")

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
