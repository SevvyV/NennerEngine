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
from pathlib import Path

log = logging.getLogger("nenner")

# Tickers to stream — equities and ETFs only (no futures like ES/NQ).
# GOOG is aliased to GOOGL for DataBento's symbology.
STREAM_TICKERS = [
    "AAPL", "BAC", "ETHE", "GBTC", "GDXJ", "GLD", "GOOGL",
    "MSFT", "NEM", "NVDA", "SLV", "SOYB", "TSLA", "UNG", "USO",
]


def _get_databento_key() -> str:
    """Resolve DataBento equities API key (ENV → Azure Key Vault)."""
    key = os.environ.get("DATABENTO_EQUITIES_API_KEY")
    if key:
        return key

    # Try .env in project root
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.strip().startswith("DATABENTO_EQUITIES_API_KEY="):
                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                if val:
                    return val

    # Azure Key Vault
    vault_url = os.environ.get("AZURE_KEYVAULT_URL")
    if not vault_url and env_path.exists():
        for line in env_path.read_text().splitlines():
            s = line.strip()
            if s.startswith("AZURE_KEYVAULT_URL="):
                vault_url = s.split("=", 1)[1].strip().strip('"').strip("'")
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
        # In-memory spot cache: {ticker: midpoint_price}
        self._spots: dict[str, float] = {}
        self._lock = threading.Lock()

    @property
    def is_healthy(self) -> bool:
        if not self._healthy:
            return False
        if self._last_record_time == 0.0:
            return False
        return (time.monotonic() - self._last_record_time) < 60

    def run(self) -> None:
        log.info("EquityStreamThread: starting with %d tickers", len(self._tickers))
        try:
            while not self._stop_event.is_set():
                try:
                    self._run_stream()
                except Exception:
                    log.exception(
                        "EquityStreamThread: stream error, reconnecting in 10s"
                    )
                    self._healthy = False
                    if self._stop_event.wait(timeout=10):
                        break
        finally:
            self._healthy = False
            log.info("EquityStreamThread: stopped")

    def _run_stream(self) -> None:
        import contextlib

        import databento as db
        from databento import BBOMsg, Dataset, Schema, SType

        log.info(
            "EquityStreamThread: subscribing to %d tickers on EQUS.MINI/bbo-1s",
            len(self._tickers),
        )

        live = db.Live(key=_get_databento_key())
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
                    self._spots[symbol] = mid
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
                for ticker, price in spots.items():
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
