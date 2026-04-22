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
        # In-memory spot cache: {ticker: {bid, ask, mid}}
        self._spots: dict[str, dict] = {}
        self._lock = threading.Lock()

    @property
    def is_healthy(self) -> bool:
        if not self._healthy:
            return False
        if self._last_record_time == 0.0:
            return False
        return (time.monotonic() - self._last_record_time) < 60

    def get_snapshot(self) -> dict[str, dict]:
        """Return thread-safe copy of current quotes {ticker: {bid, ask, mid}}."""
        with self._lock:
            return {k: dict(v) for k, v in self._spots.items()}

    def run(self) -> None:
        log.info("EquityStreamThread: starting with %d tickers", len(self._tickers))

        # Reconnect backoff: 10s, 30s, 60s, then 5min cap. Admin gets a
        # single Telegram after _ALERT_AFTER consecutive failures (typically
        # signals a DataBento auth / subscription problem, not a transient
        # network blip).
        _BACKOFF_SEQUENCE = [10, 30, 60, 300]
        _ALERT_AFTER = 3
        consecutive_failures = 0
        admin_alerted = False

        try:
            while not self._stop_event.is_set():
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
                    if (consecutive_failures >= _ALERT_AFTER
                            and not admin_alerted):
                        self._alert_admin_on_persistent_failure(e, consecutive_failures, backoff)
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

    @staticmethod
    def _alert_admin_on_persistent_failure(
        err: Exception, failures: int, next_backoff_s: int,
    ) -> None:
        """One-shot Telegram on persistent DataBento connection failure."""
        try:
            from .alert_dispatch import get_telegram_config, send_telegram
            token, chat_id = get_telegram_config()
            if not token or not chat_id:
                return
            send_telegram(
                f"🚨 NennerEngine equity stream: {failures} consecutive "
                f"DataBento reconnect failures.\n"
                f"Last error: {type(err).__name__}: {err}\n"
                f"Will keep retrying at {next_backoff_s}s intervals.",
                token, chat_id,
            )
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
        def _shutdown_watchdog():
            self._stop_event.wait()
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
            with contextlib.suppress(Exception):
                live.stop()
            with contextlib.suppress(Exception):
                live.block_for_close(timeout=5)
            with contextlib.suppress(Exception):
                live.terminate()
            log.info("EquityStreamThread: Live session closed")

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
                for ticker, quote in spots.items():
                    price = quote["mid"]
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
                conn.commit()
            finally:
                conn.close()
            log.info(
                "EquityStreamThread: flushed %d prices to DB", len(spots),
            )
        except Exception as e:
            log.debug("EquityStreamThread: DB flush failed: %s", e)
