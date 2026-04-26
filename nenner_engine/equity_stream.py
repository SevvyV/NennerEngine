"""Lightweight DataBento equity stream for the NennerEngine dashboard.

Subscribes to EQUS.MINI / BBO_1S for watchlist equity/ETF tickers and
flushes midpoint prices to the shared DB every 60 seconds so the
dashboard's existing price pipeline picks them up automatically.
"""

import logging
import math
import os
import sqlite3
import threading
import time
from datetime import date as date_mod

from .config import load_env_once

log = logging.getLogger(__name__)

# Tickers to stream — equities and ETFs only (no futures like ES/NQ).
# GOOG is aliased to GOOGL for DataBento's symbology.
STREAM_TICKERS = [
    "AAPL", "BAC", "ETHE", "GBTC", "GDXJ", "GLD", "GOOGL",
    "MSFT", "NEM", "NVDA", "SLV", "SOYB", "TSLA", "UNG", "USO",
]


def _get_databento_key() -> str:
    """Resolve DataBento equities API key (ENV → Azure Key Vault)."""
    load_env_once()
    key = os.environ.get("DATABENTO_EQUITIES_API_KEY")
    if key:
        return key

    vault_url = os.environ.get("AZURE_KEYVAULT_URL")
    if vault_url:
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient

            client = SecretClient(
                vault_url=vault_url,
                credential=DefaultAzureCredential(),
            )
            secret = client.get_secret("Databento-Equities").value
            if secret:
                return secret
        except Exception as e:
            log.error("Key Vault error (DataBento Equities): %s", e)

    raise RuntimeError(
        "DATABENTO_EQUITIES_API_KEY not found in environment, .env, "
        "or Azure Key Vault."
    )


class EquityStreamThread(threading.Thread):
    """Persistent DataBento Live stream for equity spot prices.

    Subscribes to EQUS.MINI with BBO_1S schema, extracts bid/ask
    midpoints, and flushes to the shared price_history table every 60s.
    """

    def __init__(
        self,
        stop_event: threading.Event,
        db_path: str,
        *,
        tickers: list[str] | None = None,
    ) -> None:
        super().__init__(name="equity-stream", daemon=True)
        self._stop_event = stop_event
        self._db_path = db_path
        self._tickers = list(tickers or STREAM_TICKERS)
        self._healthy = False
        self._last_record_time: float = 0.0
        # Tracks whether the most recent DB flush succeeded. Records arriving
        # from DataBento update _last_record_time independently, so a
        # persistent flush failure (disk full, schema drift) would otherwise
        # be masked as healthy as long as quotes keep flowing. Keep this
        # flag separate so a single failure latches the stream as unhealthy
        # until a successful flush clears it.
        self._flush_failed: bool = False
        # In-memory spot cache: {ticker: {bid, ask, mid}}
        self._spots: dict[str, dict] = {}
        self._lock = threading.Lock()

    @property
    def is_healthy(self) -> bool:
        if not self._healthy:
            return False
        if self._flush_failed:
            return False
        if self._last_record_time == 0.0:
            return False
        return (time.monotonic() - self._last_record_time) < 60

    def get_snapshot(self) -> dict[str, dict]:
        """Return thread-safe copy of current quotes {ticker: {bid, ask, mid}}."""
        with self._lock:
            return {k: dict(v) for k, v in self._spots.items()}

    # Phrases that almost certainly indicate a DataBento credential /
    # entitlement problem rather than a transient network blip. Hitting
    # one of these means retrying for hours is pointless — alert the
    # operator on the FIRST failure. Kept tight on purpose: bare HTTP
    # codes like "401"/"403" or single words like "subscription" trigger
    # too many false positives on benign errors (transient gateway
    # responses, log lines that happen to contain the digits).
    _AUTH_FAILURE_MARKERS = (
        "authentication failed",
        "authentication error",
        "unauthorized",
        "invalid api key",
        "invalid_api_key",
        "api key expired",
        "api_key_expired",
        "key revoked",
        "subscription required",
        "subscription expired",
        "not entitled",
    )

    @classmethod
    def _looks_like_auth_failure(cls, err: Exception) -> bool:
        msg = f"{type(err).__name__}: {err}".lower()
        return any(m in msg for m in cls._AUTH_FAILURE_MARKERS)

    def run(self) -> None:
        log.info("EquityStreamThread: starting with %d tickers", len(self._tickers))

        # Reconnect backoff: 10s, 30s, 60s, then 5min cap. Admin gets a
        # single Telegram after _ALERT_AFTER consecutive failures (typically
        # signals a DataBento auth / subscription problem, not a transient
        # network blip). Auth-shaped errors short-circuit and alert
        # immediately on the first failure.
        _BACKOFF_SEQUENCE = [10, 30, 60, 300]
        _ALERT_AFTER = 3
        consecutive_failures = 0
        admin_alerted = False

        try:
            while not self._stop_event.is_set():
                self._wait_for_market_day()
                if self._stop_event.is_set():
                    break
                try:
                    self._run_stream()
                except Exception as e:
                    consecutive_failures += 1
                    self._healthy = False
                    backoff = _BACKOFF_SEQUENCE[
                        min(consecutive_failures - 1, len(_BACKOFF_SEQUENCE) - 1)
                    ]
                    log.exception(
                        "EquityStreamThread: stream error "
                        "(consecutive=%d), reconnecting in %ds",
                        consecutive_failures, backoff,
                    )
                    is_auth = self._looks_like_auth_failure(e)
                    if not admin_alerted and (
                        is_auth or consecutive_failures >= _ALERT_AFTER
                    ):
                        self._alert_admin_on_persistent_failure(
                            e, consecutive_failures, backoff,
                            auth_suspected=is_auth,
                        )
                        admin_alerted = True
                    if self._stop_event.wait(timeout=backoff):
                        break
                else:
                    # _run_stream returned cleanly (stop_event set inside loop)
                    if consecutive_failures:
                        log.info(
                            "EquityStreamThread: recovered after %d failure(s)",
                            consecutive_failures,
                        )
                    consecutive_failures = 0
                    admin_alerted = False
        finally:
            self._healthy = False
            log.info("EquityStreamThread: stopped")

    def _wait_for_market_day(self) -> None:
        """Sleep through weekends instead of hammering DataBento.

        Pre-market data starts around 4 AM ET on weekdays, so we gate on
        weekday + hour >= 4.  On Friday after 4 AM we let the stream run
        (post-market data is still useful); Saturday/Sunday we sleep until
        Monday 4 AM.
        """
        from datetime import timedelta
        from .tz import ET, now_et
        while not self._stop_event.is_set():
            t = now_et()
            wd = t.weekday()  # 0=Mon … 6=Sun
            if wd < 5 and t.hour >= 4:
                return
            # Compute next weekday 4 AM ET
            if wd < 5:
                # Before 4 AM on a weekday — wait until 4 AM today
                target = t.replace(hour=4, minute=0, second=0, microsecond=0)
            elif wd == 5:
                # Saturday → Monday 4 AM
                target = (t + timedelta(days=2)).replace(
                    hour=4, minute=0, second=0, microsecond=0,
                )
            else:
                # Sunday → Monday 4 AM
                target = (t + timedelta(days=1)).replace(
                    hour=4, minute=0, second=0, microsecond=0,
                )
            secs = max((target - t).total_seconds(), 60)
            log.info(
                "EquityStreamThread: market closed (%s), "
                "sleeping %.0f min until %s",
                t.strftime("%A %H:%M ET"), secs / 60,
                target.strftime("%A %H:%M ET"),
            )
            if self._stop_event.wait(timeout=secs):
                return

    @staticmethod
    def _alert_admin_on_persistent_failure(
        err: Exception, failures: int, next_backoff_s: int,
        *, auth_suspected: bool = False,
    ) -> None:
        """One-shot critical alert on DataBento connection failure.

        Routes through ``send_critical_alert`` so the admin still gets the
        message via email if Telegram itself is down. Auth-shaped errors
        get a distinct subject so they can be triaged without reading the
        body.
        """
        try:
            from .alert_dispatch import send_critical_alert
            subject = (
                "DataBento equity stream — auth/subscription failure"
                if auth_suspected
                else f"DataBento equity stream — {failures} reconnect failures"
            )
            body = (
                f"Last error: {type(err).__name__}: {err}\n"
                f"Will keep retrying at {next_backoff_s}s intervals."
            )
            if auth_suspected:
                body = (
                    "Stream is failing in a shape consistent with an expired "
                    "or revoked API key, or a lapsed subscription. Retrying "
                    "is futile until credentials are fixed.\n\n" + body
                )
            send_critical_alert(subject, body)
        except Exception:
            pass

    def _run_stream(self) -> None:
        import contextlib

        import databento as db
        from databento import BBOMsg, Dataset, Schema, SType

        log.info(
            "EquityStreamThread: subscribing to %d tickers on EQUS.MINI/bbo-1s",
            len(self._tickers),
        )

        live = db.Live(key=_get_databento_key())

        # Watchdog thread: the `for record in live:` iterator blocks on
        # socket recv, which means a plain _stop_event check only runs
        # when a record arrives. On low-volume weekends that can stall
        # shutdown for tens of seconds. This watchdog calls live.stop()
        # the moment _stop_event is set, which closes the socket and
        # breaks the iterator immediately.
        #
        # _session_done is a per-session sentinel so the watchdog exits
        # when _run_stream returns, rather than accumulating one leaked
        # blocking thread per reconnect cycle.
        session_done = threading.Event()

        def _shutdown_watchdog():
            while not self._stop_event.wait(timeout=1.0):
                if session_done.is_set():
                    return
            try:
                live.stop()
            except Exception:
                pass

        watchdog = threading.Thread(
            target=_shutdown_watchdog,
            name="equity-stream-watchdog",
            daemon=True,
        )
        watchdog.start()

        try:
            live.subscribe(
                dataset=Dataset.EQUS_MINI,
                schema=Schema.BBO_1S,
                stype_in=SType.RAW_SYMBOL,
                symbols=self._tickers,
            )

            self._healthy = True
            last_flush = time.monotonic()

            for record in live:
                if self._stop_event.is_set():
                    break

                if not isinstance(record, BBOMsg):
                    continue

                inst_id = getattr(record, "instrument_id", None)
                if inst_id is None:
                    continue
                symbol = live.symbology_map.get(inst_id)
                if symbol is None:
                    continue

                levels = getattr(record, "levels", None)
                if not levels:
                    continue
                level = levels[0]
                bid = float(level.pretty_bid_px)
                ask = float(level.pretty_ask_px)

                if math.isnan(bid):
                    bid = 0.0
                if math.isnan(ask):
                    ask = 0.0
                if bid <= 0 and ask <= 0:
                    continue

                # Compute midpoint
                if bid > 0 and ask > 0:
                    mid = (bid + ask) / 2.0
                elif bid > 0:
                    mid = bid
                else:
                    mid = ask

                with self._lock:
                    self._spots[symbol] = {"bid": bid, "ask": ask, "mid": mid}
                self._last_record_time = time.monotonic()

                # Flush to DB every 60 seconds
                now = time.monotonic()
                if now - last_flush >= 60:
                    self._flush_to_db()
                    last_flush = now

        finally:
            # Release the watchdog so it doesn't sit blocked forever
            # on _stop_event (one leaked thread per reconnect otherwise).
            session_done.set()
            with contextlib.suppress(Exception):
                live.stop()
            with contextlib.suppress(Exception):
                live.block_for_close(timeout=5)
            with contextlib.suppress(Exception):
                live.terminate()
            log.info("EquityStreamThread: Live session closed")

    _MAX_DAILY_CHANGE_PCT = 30.0

    def _flush_to_db(self) -> None:
        """Write cached midpoints to price_history (same upsert as FischerDaily)."""
        with self._lock:
            spots = dict(self._spots)
        if not spots:
            return
        try:
            today = date_mod.today().isoformat()
            conn = sqlite3.connect(self._db_path)
            try:
                prior = self._load_prior_closes(conn, spots.keys(), today)
                written = 0
                for ticker, quote in spots.items():
                    price = quote["mid"]
                    prev = prior.get(ticker)
                    if prev and prev > 0:
                        change_pct = abs((price - prev) / prev) * 100
                        if change_pct > self._MAX_DAILY_CHANGE_PCT:
                            log.warning(
                                "EquityStreamThread: rejecting %s %.2f "
                                "(%.1f%% vs prior %.2f)",
                                ticker, price, change_pct, prev,
                            )
                            continue
                    cur = conn.execute(
                        "UPDATE price_history "
                        "SET close = ?, fetched_at = datetime('now') "
                        "WHERE ticker = ? AND source = 'DATABENTO_EQUITY' "
                        "AND date LIKE ? || '%'",
                        (price, ticker, today),
                    )
                    if cur.rowcount == 0:
                        conn.execute(
                            "INSERT INTO price_history "
                            "(ticker, date, close, source) "
                            "VALUES (?, ?, ?, 'DATABENTO_EQUITY')",
                            (ticker, today, price),
                        )
                    written += 1
                conn.commit()
            finally:
                conn.close()
            self._flush_failed = False
            log.info(
                "EquityStreamThread: flushed %d/%d prices to DB",
                written, len(spots),
            )
        except Exception as e:
            # Without latching this flag, a healthy *record* stream would
            # mask a broken DB write — anyone polling is_healthy would see
            # True even while we're failing to persist quotes. The flag
            # stays True until the next flush succeeds.
            self._flush_failed = True
            log.error("EquityStreamThread: DB flush failed: %s", e)

    @staticmethod
    def _load_prior_closes(
        conn: sqlite3.Connection, tickers, today: str,
    ) -> dict[str, float]:
        """Load most recent close per ticker before *today* for sanity check."""
        prior: dict[str, float] = {}
        for ticker in tickers:
            row = conn.execute(
                "SELECT close FROM price_history "
                "WHERE ticker = ? AND date < ? "
                "ORDER BY date DESC LIMIT 1",
                (ticker, today),
            ).fetchone()
            if row:
                prior[ticker] = row[0]
        return prior
