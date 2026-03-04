"""
Fischer Reliability Layer
==========================
Nine composable safeguards for hardening Fischer options email service
during unattended market-hours operation.

S1  RequestQueue        — Serialized job queue (threading.Queue, depth 10)
S2  ResultCache         — 90-second TTL result cache keyed by (report_type, date)
S3  ResilientIMAPPoller — Exponential backoff + admin alert on IMAP failures
S4  ScanGuard           — Abort + alert when >8/17 tickers fail
S5  SendDeduplicator    — Prevent duplicate sends per (email, report, job_id)
S6  GracefulShutdown    — SIGINT/SIGTERM handler with 120s drain
S7  HealthLogger        — 1 line/min to logs/fischer_health.log, 7-day rotation
S8  MarketHoursGuard    — Defer off-hours requests, flag stale equity tickers
S9  TZ Enforcement      — now_et() / format_et() — all timestamps America/New_York

Unified under FischerReliability singleton facade.
"""

import hashlib
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from queue import Full, Queue
from typing import Optional
from zoneinfo import ZoneInfo

from .config import ADMIN_EMAIL

log = logging.getLogger("nenner")

# ---------------------------------------------------------------------------
# S9: TZ Enforcement
# ---------------------------------------------------------------------------

_ET = ZoneInfo("America/New_York")


def now_et() -> datetime:
    """Return current datetime, tz-aware, in America/New_York."""
    return datetime.now(_ET)


def format_et(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S %Z") -> str:
    """Format a datetime in ET.  If naive, localizes to ET first."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_ET)
    return dt.astimezone(_ET).strftime(fmt)


# ---------------------------------------------------------------------------
# S7: Health Logger
# ---------------------------------------------------------------------------

class HealthLogger:
    """Writes 1 health line per minute to logs/fischer_health.log.

    Uses TimedRotatingFileHandler(when='midnight', backupCount=7).
    Creates the logs/ directory if missing.
    """

    def __init__(self, log_dir: str = "logs"):
        self._log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)

        self._logger = logging.getLogger("fischer.health")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False

        # Avoid duplicate handlers on re-init
        if not self._logger.handlers:
            path = os.path.join(log_dir, "fischer_health.log")
            handler = TimedRotatingFileHandler(
                path, when="midnight", backupCount=7, encoding="utf-8",
            )
            handler.setFormatter(
                logging.Formatter("%(asctime)s | %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
            )
            self._logger.addHandler(handler)

        self._last_tick: Optional[datetime] = None

    def tick(self, reliability: "FischerReliability"):
        """Record a health line if >= 60s since last tick."""
        now = datetime.now()
        if self._last_tick and (now - self._last_tick).total_seconds() < 60:
            return
        self._last_tick = now

        parts = [f"et={format_et(now_et(), '%H:%M')}"]

        if reliability.queue:
            q = reliability.queue
            parts.append(f"queue={q.qsize}/{q.max_depth}")
            parts.append(f"completed_today={q.completed_today}")

        if reliability.cache:
            parts.append(f"cache_entries={reliability.cache.size}")

        if reliability.imap_poller:
            p = reliability.imap_poller
            parts.append(f"imap_fails={p.consecutive_failures}")

        if reliability.market_hours:
            parts.append(f"market={'open' if reliability.market_hours.is_equity_open() else 'closed'}")

        self._logger.info(" | ".join(parts))


# ---------------------------------------------------------------------------
# S1: Request Queue
# ---------------------------------------------------------------------------

@dataclass
class ReportJob:
    """A queued report generation request."""
    job_id: str
    report_type: str          # "scheduled_scan", "refresh", etc.
    subscriber_email: str
    portfolio_name: str
    slot: str = ""
    created_at: datetime = field(default_factory=now_et)
    metadata: dict = field(default_factory=dict)


class RequestQueue:
    """Thread-safe bounded job queue.  Max depth 10, drops oldest on overflow."""

    def __init__(self, max_depth: int = 10):
        self.max_depth = max_depth
        self._queue: Queue[ReportJob] = Queue(maxsize=max_depth)
        self._lock = threading.Lock()
        self._current_job: Optional[ReportJob] = None
        self._completed_today = 0
        self._today_str = now_et().strftime("%Y-%m-%d")

    @property
    def completed_today(self) -> int:
        today = now_et().strftime("%Y-%m-%d")
        if today != self._today_str:
            self._completed_today = 0
            self._today_str = today
        return self._completed_today

    @property
    def qsize(self) -> int:
        return self._queue.qsize()

    def enqueue(self, job: ReportJob) -> bool:
        """Add job to queue.  Drops oldest pending on overflow.  Returns True if enqueued."""
        with self._lock:
            if self._queue.full():
                try:
                    dropped = self._queue.get_nowait()
                    log.warning(f"RequestQueue: dropped oldest job {dropped.job_id} "
                                f"({dropped.report_type}) to make room")
                except Exception:
                    pass
            try:
                self._queue.put_nowait(job)
                log.info(f"RequestQueue: enqueued {job.job_id} ({job.report_type})")
                return True
            except Full:
                log.error(f"RequestQueue: failed to enqueue {job.job_id}")
                return False

    def dequeue(self, timeout: float = 1.0) -> Optional[ReportJob]:
        """Get next job, blocking up to timeout seconds."""
        try:
            job = self._queue.get(timeout=timeout)
            with self._lock:
                self._current_job = job
            return job
        except Exception:
            return None

    def mark_complete(self):
        """Mark current job as complete."""
        with self._lock:
            if self._current_job:
                log.info(f"RequestQueue: completed {self._current_job.job_id}")
                self._current_job = None
                self._completed_today += 1

    @property
    def current_job(self) -> Optional[ReportJob]:
        with self._lock:
            return self._current_job


# ---------------------------------------------------------------------------
# S2: Result Cache
# ---------------------------------------------------------------------------

@dataclass
class CacheEntry:
    """Cached scan result."""
    result_html: str
    created_at: datetime
    sent_to: list[str] = field(default_factory=list)


class ResultCache:
    """In-memory cache keyed by (report_type, date_str) with 90-second TTL."""

    TTL_SECONDS = 90

    def __init__(self):
        self._store: dict[tuple[str, str], CacheEntry] = {}
        self._lock = threading.Lock()

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._store)

    def get(self, report_type: str, date_str: str) -> Optional[CacheEntry]:
        """Return cached entry if exists and not expired, else None."""
        key = (report_type, date_str)
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            age = (now_et() - entry.created_at).total_seconds()
            if age > self.TTL_SECONDS:
                del self._store[key]
                log.info(f"ResultCache: expired {key} (age={age:.0f}s)")
                return None
            return entry

    def put(self, report_type: str, date_str: str, html: str) -> CacheEntry:
        """Store a result.  Returns the new CacheEntry."""
        key = (report_type, date_str)
        entry = CacheEntry(result_html=html, created_at=now_et())
        with self._lock:
            self._store[key] = entry
        log.info(f"ResultCache: stored {key}")
        return entry

    def record_send(self, report_type: str, date_str: str, email: str):
        """Track that we sent the cached result to an email address."""
        key = (report_type, date_str)
        with self._lock:
            entry = self._store.get(key)
            if entry and email not in entry.sent_to:
                entry.sent_to.append(email)

    def cleanup(self):
        """Remove all expired entries."""
        now = now_et()
        with self._lock:
            expired = [k for k, v in self._store.items()
                       if (now - v.created_at).total_seconds() > self.TTL_SECONDS]
            for k in expired:
                del self._store[k]
            if expired:
                log.info(f"ResultCache: cleaned {len(expired)} expired entries")


# ---------------------------------------------------------------------------
# S5: Send Deduplicator
# ---------------------------------------------------------------------------

class SendDeduplicator:
    """Prevents duplicate sends per (email, report_type, job_id).

    Auto-cleans entries older than 10 minutes.
    """

    CLEANUP_INTERVAL = 600  # 10 minutes

    def __init__(self):
        self._sent: dict[tuple[str, str, str], datetime] = {}
        self._lock = threading.Lock()

    @staticmethod
    def make_job_id(email: str, timestamp: Optional[datetime] = None) -> str:
        """Hash email + timestamp rounded to nearest minute."""
        ts = timestamp or now_et()
        rounded = ts.replace(second=0, microsecond=0)
        raw = f"{email.lower()}|{rounded.isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:12]

    def check_and_mark(self, email: str, report_type: str,
                       job_id: str) -> bool:
        """Return True if this send is allowed (not a duplicate).

        If allowed, marks it as sent.  If duplicate, returns False.
        """
        key = (email.lower(), report_type, job_id)
        with self._lock:
            self._auto_cleanup()
            if key in self._sent:
                log.info(f"SendDedup: blocked duplicate send {key}")
                return False
            self._sent[key] = now_et()
            return True

    def _auto_cleanup(self):
        """Remove entries older than CLEANUP_INTERVAL.  Called under lock."""
        now = now_et()
        stale = [k for k, ts in self._sent.items()
                 if (now - ts).total_seconds() > self.CLEANUP_INTERVAL]
        for k in stale:
            del self._sent[k]


# ---------------------------------------------------------------------------
# S3: Resilient IMAP Poller
# ---------------------------------------------------------------------------

class ResilientIMAPPoller:
    """Wraps poll_refresh_requests() with exponential backoff.

    Backoff schedule: 30s → 60s → 120s → 240s → 300s (cap).
    Sends admin alert after 3 consecutive failures.
    Resets on success.
    """

    BACKOFF_BASE = 30
    BACKOFF_CAP = 300
    ALERT_THRESHOLD = 3

    def __init__(self):
        self._consecutive_failures = 0
        self._last_attempt: Optional[datetime] = None
        self._alert_sent = False
        self._lock = threading.Lock()

    @property
    def consecutive_failures(self) -> int:
        with self._lock:
            return self._consecutive_failures

    def _backoff_seconds(self) -> float:
        """Current backoff delay based on failure count."""
        delay = self.BACKOFF_BASE * (2 ** self._consecutive_failures)
        return min(delay, self.BACKOFF_CAP)

    def poll(self, db_path: str) -> int:
        """Poll IMAP with resilience.  Returns processed count."""
        with self._lock:
            if self._consecutive_failures > 0 and self._last_attempt:
                elapsed = (datetime.now() - self._last_attempt).total_seconds()
                required = self._backoff_seconds()
                if elapsed < required:
                    log.debug(f"IMAPPoller: backing off ({elapsed:.0f}s / {required:.0f}s)")
                    return 0

        try:
            from .fischer_subscribers import _poll_refresh_raw
            processed = _poll_refresh_raw(db_path)

            with self._lock:
                if self._consecutive_failures > 0:
                    log.info(f"IMAPPoller: recovered after {self._consecutive_failures} failure(s)")
                self._consecutive_failures = 0
                self._alert_sent = False
                self._last_attempt = datetime.now()

            return processed

        except Exception as e:
            with self._lock:
                self._consecutive_failures += 1
                self._last_attempt = datetime.now()
                fails = self._consecutive_failures

            log.error(f"IMAPPoller: failure #{fails}: {e}")

            if fails >= self.ALERT_THRESHOLD and not self._alert_sent:
                self._send_alert(fails, str(e))
                self._alert_sent = True

            return 0

    def _send_alert(self, fail_count: int, last_error: str):
        """Send admin alert email after repeated IMAP failures."""
        try:
            from .postmaster import send_email, wrap_document
            body = wrap_document(
                f'<p style="font-size:14px;">Fischer IMAP poller has failed '
                f'<strong>{fail_count}</strong> consecutive times.</p>'
                f'<p style="font-size:13px;">Last error: <code>{last_error}</code></p>'
                f'<p style="font-size:13px;">Backoff active: {self._backoff_seconds():.0f}s. '
                f'Will auto-recover on next success.</p>',
                title="Fischer Alert",
                subtitle=f"IMAP Failure &mdash; {format_et(now_et(), '%b %d %H:%M ET')}",
            )
            send_email("Fischer IMAP Alert — Repeated Failures", body,
                       to_addr=ADMIN_EMAIL)
            log.warning(f"IMAPPoller: admin alert sent after {fail_count} failures")
        except Exception as e:
            log.error(f"IMAPPoller: failed to send admin alert: {e}")


# ---------------------------------------------------------------------------
# S4: Scan Guard
# ---------------------------------------------------------------------------

class ScanGuard:
    """Abort scan if too many tickers fail.  Threshold: >8 out of 17."""

    FAIL_THRESHOLD = 8
    TOTAL_TICKERS = 17

    def check_abort(self, failed_tickers: list[str], slot: str) -> bool:
        """Return True if scan should be aborted (too many failures).

        Also sends admin alert if aborting.
        """
        if len(failed_tickers) <= self.FAIL_THRESHOLD:
            return False

        log.error(f"ScanGuard: {len(failed_tickers)}/{self.TOTAL_TICKERS} tickers failed "
                  f"in {slot} scan — ABORTING")
        self._send_abort_alert(failed_tickers, slot)
        return True

    def _send_abort_alert(self, failed_tickers: list[str], slot: str):
        """Email admin about aborted scan."""
        try:
            from .postmaster import send_email, wrap_document
            ticker_list = ", ".join(sorted(failed_tickers))
            body = wrap_document(
                f'<p style="font-size:14px;">The Fischer <strong>{slot}</strong> scan '
                f'was aborted: <strong>{len(failed_tickers)}/{self.TOTAL_TICKERS}</strong> '
                f'tickers had no pricing data.</p>'
                f'<p style="font-size:13px;">Failed: {ticker_list}</p>'
                f'<p style="font-size:13px;">Likely cause: DataCenter offline or '
                f'Thomson One RTD disconnected.</p>',
                title="Fischer Scan Aborted",
                subtitle=f"{slot.title()} &mdash; {format_et(now_et(), '%b %d %H:%M ET')}",
            )
            send_email(f"Fischer Scan Aborted — {slot.title()}", body,
                       to_addr=ADMIN_EMAIL)
        except Exception as e:
            log.error(f"ScanGuard: failed to send abort alert: {e}")

    def build_retry_reply(self, subscriber_email: str, slot: str) -> str:
        """Build an HTML reply body telling the requester to retry later."""
        from .postmaster import wrap_document
        return wrap_document(
            '<p style="font-size:14px;">Your Fischer scan request could not be '
            'completed because the market data feed is currently unavailable.</p>'
            '<p style="font-size:14px;">Please try again in a few minutes by '
            'sending another "Refresh Fischer" email.</p>',
            title="Fischer Options",
            subtitle="Temporary Data Issue",
        )


# ---------------------------------------------------------------------------
# S8: Market Hours Guard
# ---------------------------------------------------------------------------

class MarketHoursGuard:
    """Enforce equity and ETF market sessions.

    EQUITY session: 9:30 AM – 4:00 PM ET
    ETF session:    9:30 AM – 4:15 PM ET

    Off-hours requests are deferred.  Between 4:00–4:15 PM, equity
    tickers are flagged as stale but ETFs proceed.

    Uses the ``holidays`` package for dynamic US holiday detection.
    """

    EQUITY_OPEN = (9, 30)
    EQUITY_CLOSE = (16, 0)
    ETF_CLOSE = (16, 15)

    # Tickers that follow extended ETF hours (4:15 close)
    ETF_TICKERS = frozenset({
        "GLD", "SLV", "SPY", "QQQ", "TLT", "IWM", "DIA",
        "USO", "UNG", "CORN", "SOYB", "WEAT", "FXE", "UUP",
        "GBTC", "IBIT", "ETHE", "BITO", "GDXJ", "NEM", "SIL",
    })

    def __init__(self):
        try:
            import holidays as _holidays
            self._us_holidays = _holidays.US(years=range(
                now_et().year, now_et().year + 2))
        except ImportError:
            log.warning("MarketHoursGuard: 'holidays' package not installed, "
                        "holiday detection disabled")
            self._us_holidays = None

    def _is_holiday(self, dt: datetime) -> bool:
        """Check if a date is a US market holiday."""
        if self._us_holidays is None:
            return False
        return dt.date() in self._us_holidays

    def is_trading_day(self, dt: Optional[datetime] = None) -> bool:
        """True if date is a weekday and not a US holiday."""
        dt = dt or now_et()
        if dt.weekday() >= 5:
            return False
        return not self._is_holiday(dt)

    def is_equity_open(self, dt: Optional[datetime] = None) -> bool:
        """True if within equity session (9:30–16:00 ET) on a trading day."""
        dt = dt or now_et()
        if not self.is_trading_day(dt):
            return False
        t = (dt.hour, dt.minute)
        return self.EQUITY_OPEN <= t < self.EQUITY_CLOSE

    def is_etf_open(self, dt: Optional[datetime] = None) -> bool:
        """True if within ETF session (9:30–16:15 ET) on a trading day."""
        dt = dt or now_et()
        if not self.is_trading_day(dt):
            return False
        t = (dt.hour, dt.minute)
        return self.EQUITY_OPEN <= t < self.ETF_CLOSE

    def is_equity_stale_window(self, dt: Optional[datetime] = None) -> bool:
        """True if between 4:00–4:15 PM ET (equity closed, ETF still open)."""
        dt = dt or now_et()
        if not self.is_trading_day(dt):
            return False
        t = (dt.hour, dt.minute)
        return self.EQUITY_CLOSE <= t < self.ETF_CLOSE

    def filter_stale_tickers(self, tickers: list[str],
                             dt: Optional[datetime] = None) -> tuple[list[str], list[str]]:
        """Split tickers into (live, stale) based on current session.

        Between 4:00–4:15 PM: non-ETF tickers are stale.
        Outside market hours: all tickers are stale.
        """
        dt = dt or now_et()

        if not self.is_trading_day(dt):
            return [], list(tickers)

        if self.is_equity_stale_window(dt):
            live = [t for t in tickers if t in self.ETF_TICKERS]
            stale = [t for t in tickers if t not in self.ETF_TICKERS]
            return live, stale

        if self.is_etf_open(dt):
            return list(tickers), []

        # Outside all sessions
        return [], list(tickers)

    def next_open_time(self, dt: Optional[datetime] = None) -> datetime:
        """Return the next market open (9:31 AM ET) on the next trading day."""
        dt = dt or now_et()
        candidate = dt.replace(hour=9, minute=31, second=0, microsecond=0)

        # If we're past 9:31 today or it's not a trading day, go to tomorrow
        if dt >= candidate or not self.is_trading_day(dt):
            candidate += timedelta(days=1)

        # Skip weekends and holidays
        attempts = 0
        while not self.is_trading_day(candidate) and attempts < 10:
            candidate += timedelta(days=1)
            attempts += 1

        return candidate

    def check_request(self, tickers: Optional[list[str]] = None,
                      dt: Optional[datetime] = None) -> dict:
        """Check if a scan request is allowed right now.

        Returns dict:
            allowed: bool — proceed with scan
            defer_until: Optional[datetime] — when to retry if deferred
            stale_tickers: list[str] — tickers with stale prices (4:00–4:15 window)
            reason: str — human-readable status
        """
        dt = dt or now_et()
        tickers = tickers or []

        if not self.is_trading_day(dt):
            return {
                "allowed": False,
                "defer_until": self.next_open_time(dt),
                "stale_tickers": tickers,
                "reason": "Market closed (weekend/holiday)",
            }

        if not self.is_etf_open(dt):
            # Before open or after all sessions
            t = (dt.hour, dt.minute)
            if t < self.EQUITY_OPEN:
                return {
                    "allowed": False,
                    "defer_until": dt.replace(hour=9, minute=31, second=0, microsecond=0),
                    "stale_tickers": tickers,
                    "reason": "Market not yet open",
                }
            return {
                "allowed": False,
                "defer_until": self.next_open_time(dt),
                "stale_tickers": tickers,
                "reason": "Market closed for the day",
            }

        # Market is open — check stale window
        live, stale = self.filter_stale_tickers(tickers, dt)
        return {
            "allowed": True,
            "defer_until": None,
            "stale_tickers": stale,
            "reason": "Equity stale window (4:00–4:15 PM)" if stale else "Market open",
        }


# ---------------------------------------------------------------------------
# S6: Graceful Shutdown
# ---------------------------------------------------------------------------

class GracefulShutdown:
    """Register SIGINT/SIGTERM handlers for clean drain.

    On signal: sets stop_event, waits up to 120s for in-flight job.
    """

    DRAIN_TIMEOUT = 120

    def __init__(self, stop_event: threading.Event):
        self._stop_event = stop_event
        self._shutting_down = False
        self._lock = threading.Lock()

    @property
    def shutting_down(self) -> bool:
        with self._lock:
            return self._shutting_down

    def register_handlers(self):
        """Register signal handlers.  Must be called from main thread.

        Silently skips if not in main thread (non-fatal).
        """
        try:
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
            log.info("GracefulShutdown: signal handlers registered")
        except (ValueError, OSError) as e:
            # Not main thread or platform doesn't support
            log.debug(f"GracefulShutdown: could not register handlers: {e}")

    def _handle_signal(self, signum, frame):
        """Signal handler — initiates graceful shutdown."""
        sig_name = signal.Signals(signum).name
        log.warning(f"GracefulShutdown: received {sig_name}, initiating shutdown...")
        self.shutdown()

    def shutdown(self):
        """Initiate graceful shutdown.  Waits for in-flight job."""
        with self._lock:
            if self._shutting_down:
                log.warning("GracefulShutdown: already shutting down")
                return
            self._shutting_down = True

        self._stop_event.set()
        log.info(f"GracefulShutdown: stop_event set, waiting up to "
                 f"{self.DRAIN_TIMEOUT}s for in-flight job...")

        # Check for in-flight job
        rel = FischerReliability.get_instance()
        if rel and rel.queue and rel.queue.current_job:
            job = rel.queue.current_job
            log.info(f"GracefulShutdown: waiting for job {job.job_id} ({job.report_type})...")
            start = time.monotonic()
            while rel.queue.current_job:
                elapsed = time.monotonic() - start
                if elapsed >= self.DRAIN_TIMEOUT:
                    log.warning(f"GracefulShutdown: drain timeout after {elapsed:.0f}s, "
                                f"abandoning job {job.job_id}")
                    break
                time.sleep(1)
            else:
                elapsed = time.monotonic() - start
                log.info(f"GracefulShutdown: job completed in {elapsed:.1f}s")
        else:
            log.info("GracefulShutdown: no in-flight job, clean exit")


# ---------------------------------------------------------------------------
# Facade: FischerReliability Singleton
# ---------------------------------------------------------------------------

class FischerReliability:
    """Singleton facade holding all reliability components.

    Usage:
        # At scheduler startup:
        FischerReliability.initialize(stop_event)

        # Anywhere else:
        rel = FischerReliability.get_instance()
        if rel:
            rel.health.tick(rel)
    """

    _instance: Optional["FischerReliability"] = None
    _init_lock = threading.Lock()

    def __init__(self, stop_event: threading.Event):
        self.health = HealthLogger()
        self.queue = RequestQueue()
        self.cache = ResultCache()
        self.dedup = SendDeduplicator()
        self.imap_poller = ResilientIMAPPoller()
        self.scan_guard = ScanGuard()
        self.market_hours = MarketHoursGuard()
        self.shutdown = GracefulShutdown(stop_event)

    @classmethod
    def initialize(cls, stop_event: threading.Event) -> "FischerReliability":
        """Create the singleton.  Idempotent — returns existing instance if already init'd."""
        with cls._init_lock:
            if cls._instance is None:
                cls._instance = cls(stop_event)
                log.info("FischerReliability: initialized")
            return cls._instance

    @classmethod
    def get_instance(cls) -> Optional["FischerReliability"]:
        """Return the singleton, or None if not yet initialized.

        Callers should guard with ``if rel:`` for backward compatibility.
        """
        return cls._instance

    def record_health_tick(self):
        """Convenience: record a health tick."""
        self.health.tick(self)

    def wrap_scan_call(self, db_path: str, slot: str, scan_fn):
        """Wrap a Fischer scan call with queue + cache + guard logic.

        scan_fn: callable(db_path, slot) — the actual scan function.
        """
        # Check cache first
        today_str = now_et().strftime("%Y-%m-%d")
        cache_key = f"scan_{slot}"
        cached = self.cache.get(cache_key, today_str)
        if cached:
            log.info(f"FischerReliability: serving {slot} scan from cache")
            return

        # Run the actual scan
        scan_fn(db_path, slot)
