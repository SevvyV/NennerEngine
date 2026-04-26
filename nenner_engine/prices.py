"""
Price Data Layer
=================
Fetches, caches, and serves price data from yFinance with a TTL cache
to avoid excessive API calls.

Provides a unified interface for the dashboard and CLI.
"""

import logging
import math
import sqlite3
import threading
import time
from datetime import UTC, datetime, date, timedelta
from typing import Optional

log = logging.getLogger(__name__)

# TTL cache for yFinance prices — avoids hitting the API every 30s
_yf_cache: dict[str, float] = {}
_yf_cache_time: float = 0.0
_yf_cache_lock = threading.Lock()
from .config import YF_CACHE_TTL_SECONDS, YFINANCE_TIMEOUT  # noqa: E402


def _yf_download_with_timeout(yf_symbols, period: str, timeout: float = YFINANCE_TIMEOUT):
    """Run yf.download in a daemon thread bounded by *timeout* seconds.

    yfinance has no native timeout knob and can stall on Yahoo CDN hiccups;
    a synchronous call would freeze the scheduler thread for minutes. The
    daemon thread is abandoned on timeout — it will eventually finish
    whenever Yahoo responds, but won't block our caller.
    """
    import yfinance as yf
    box: dict = {"df": None, "err": None}

    def _run():
        try:
            box["df"] = yf.download(
                yf_symbols, period=period, progress=False, threads=True,
            )
        except Exception as e:
            box["err"] = e

    t = threading.Thread(target=_run, name="yfinance-download", daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        raise TimeoutError(
            f"yfinance.download exceeded {timeout}s "
            f"(symbols={len(yf_symbols)}, period={period})"
        )
    if box["err"] is not None:
        raise box["err"]
    return box["df"]


# ---------------------------------------------------------------------------
# Ticker Mapping: Canonical → yFinance
# ---------------------------------------------------------------------------

YFINANCE_MAP: dict[str, str | None] = {
    # Equity Indices (futures)
    "ES":       "ES=F",
    "NQ":       "NQ=F",
    "YM":       "YM=F",
    # Equity Indices (cash / index)
    "NYFANG":   "^NYFANG",
    "VIX":      "^VIX",
    "TSX":      "^GSPTSE",
    "DAX":      "^GDAXI",
    "FTSE":     "^FTSE",
    "AEX":      "^AEX",
    "NYA":      "^NYA",
    "SMI":      "^SSMI",
    "BTK":      "^BTK",
    # Precious Metals (futures)
    "GC":       "GC=F",
    "SI":       "SI=F",
    "HG":       "HG=F",
    # Precious Metals (ETF / Stock — same ticker)
    "GLD":      "GLD",
    "GDXJ":     "GDXJ",
    "NEM":      "NEM",
    "SLV":      "SLV",
    # Energy (futures)
    "CL":       "CL=F",
    "NG":       "NG=F",
    # Energy (ETFs — same ticker)
    "USO":      "USO",
    "UNG":      "UNG",
    # Agriculture (futures)
    "ZC":       "ZC=F",
    "ZS":       "ZS=F",
    "ZW":       "ZW=F",
    "LBS":      "LBR=F",
    # Agriculture (ETFs — same ticker)
    "CORN":     "CORN",
    "SOYB":     "SOYB",
    "WEAT":     "WEAT",
    # Fixed Income (futures)
    "ZB":       "ZB=F",
    "ZN":       "ZN=F",
    # Fixed Income (ETF — same ticker)
    "TLT":      "TLT",
    # Fixed Income (Europe) — not on yFinance
    "FGBL":     None,
    # Currencies
    "DXY":      "DX-Y.NYB",
    "EUR/USD":  "EURUSD=X",
    "FXE":      "FXE",
    "AUD/USD":  "AUDUSD=X",
    "USD/CAD":  "CAD=X",
    "USD/JPY":  "JPY=X",
    "USD/CHF":  "CHF=X",
    "GBP/USD":  "GBPUSD=X",
    "USD/BRL":  "BRL=X",
    "USD/ILS":  "ILS=X",
    # Crypto
    "BTC":      "BTC-USD",
    "ETH":      "ETH-USD",
    # Crypto ETFs — same ticker
    "GBTC":     "GBTC",
    "ETHE":     "ETHE",
    "BITO":     "BITO",
    # Single Stocks — same ticker
    "AAPL":     "AAPL",
    "GOOG":     "GOOG",
    "BAC":      "BAC",
    "MSFT":     "MSFT",
    "NVDA":     "NVDA",
    "TSLA":     "TSLA",
    "QQQ":      "QQQ",
    "SIL":      "SIL",
}

# Reverse lookup: yFinance symbol → canonical ticker
_YF_REVERSE = {v: k for k, v in YFINANCE_MAP.items() if v is not None}




# ---------------------------------------------------------------------------
# Price Storage (SQLite)
# ---------------------------------------------------------------------------

def store_prices(conn: sqlite3.Connection, prices: dict[str, dict], source: str):
    """Store price data into price_history table.

    Args:
        conn: SQLite connection.
        prices: {canonical_ticker: {"date": "YYYY-MM-DD", "open": float,
                 "high": float, "low": float, "close": float}}
                 OR simply {canonical_ticker: {"close": float}} for T1.
        source: 'yfinance' or 'xlwings'.
    """
    today = date.today().isoformat()
    for ticker, data in prices.items():
        price_date = data.get("date", today)
        conn.execute("""
            INSERT OR REPLACE INTO price_history
                (ticker, date, open, high, low, close, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            ticker, price_date,
            data.get("open"), data.get("high"),
            data.get("low"), data.get("close"),
            source,
        ))
    conn.commit()
    log.info(f"Stored {len(prices)} prices from {source}")


def get_cached_prices(conn: sqlite3.Connection,
                      tickers: list[str] | None = None,
                      max_age_hours: int = 24) -> dict[str, dict]:
    """Read the most recent price per ticker from price_history.

    Returns:
        {ticker: {"close": float, "date": str, "source": str, "fetched_at": str}}
    """
    if tickers:
        placeholders = ",".join("?" for _ in tickers)
        rows = conn.execute(f"""
            SELECT ticker, date, close, source, fetched_at
            FROM latest_prices
            WHERE ticker IN ({placeholders})
        """, tickers).fetchall()
    else:
        rows = conn.execute(
            "SELECT ticker, date, close, source, fetched_at FROM latest_prices"
        ).fetchall()

    result = {}
    # fetched_at is stored in UTC (SQLite datetime('now')), so compare in UTC
    cutoff = (datetime.now(UTC) - timedelta(hours=max_age_hours)).isoformat()
    for r in rows:
        fetched = r["fetched_at"] or ""
        if fetched >= cutoff:
            result[r["ticker"]] = {
                "close": r["close"],
                "date": r["date"],
                "source": r["source"],
                "fetched_at": r["fetched_at"],
            }
    return result


# ---------------------------------------------------------------------------
# yFinance
# ---------------------------------------------------------------------------

def fetch_yfinance_daily(conn: sqlite3.Connection,
                         tickers: list[str] | None = None,
                         period: str = "5d") -> dict[str, float]:
    """Fetch daily closes from yFinance and store in DB.

    Args:
        conn: SQLite connection (for storing results).
        tickers: List of canonical tickers, or None for all mapped instruments.
        period: yFinance period string ('1d', '5d', '1mo', '1y', etc.).

    Returns:
        {canonical_ticker: latest_close_price}
    """
    try:
        import yfinance as yf
    except ImportError:
        log.warning("yfinance not installed — skipping price fetch")
        return {}

    # Build the yFinance symbol list
    if tickers is None:
        tickers = list(YFINANCE_MAP.keys())

    yf_symbols = []
    yf_to_canonical = {}
    for t in tickers:
        sym = YFINANCE_MAP.get(t)
        if sym:
            yf_symbols.append(sym)
            yf_to_canonical[sym] = t

    if not yf_symbols:
        return {}

    log.info(f"Fetching {len(yf_symbols)} tickers from yFinance (period={period})…")

    try:
        df = _yf_download_with_timeout(yf_symbols, period=period)
    except TimeoutError as e:
        log.error(f"yFinance download timed out: {e}")
        return {}
    except Exception as e:
        log.error(f"yFinance download failed: {e}")
        return {}

    if df.empty:
        log.warning("yFinance returned empty DataFrame")
        return {}

    latest_prices: dict[str, float] = {}
    prices_to_store: dict[str, dict] = {}

    # yf.download returns MultiIndex columns when multiple tickers.
    # Columns are (Price, Ticker) — e.g. ("Close", "AAPL").
    if isinstance(df.columns, __import__("pandas").MultiIndex):
        # Multiple tickers
        for sym in yf_symbols:
            canonical = yf_to_canonical[sym]
            try:
                close_series = df["Close"][sym].dropna()
                if close_series.empty:
                    continue
                # Store all fetched rows (skip NaN/Inf — yfinance can emit
                # garbage on data gaps, and we never want infinities in DB).
                for dt_idx, close_val in close_series.items():
                    val = float(close_val)
                    if not math.isfinite(val) or val <= 0:
                        continue
                    d = dt_idx.strftime("%Y-%m-%d")
                    prices_to_store.setdefault(canonical, []).append({
                        "date": d,
                        "close": val,
                    })
                # Latest close
                latest = float(close_series.iloc[-1])
                if math.isfinite(latest) and latest > 0:
                    latest_prices[canonical] = latest
            except (KeyError, IndexError):
                log.debug(f"No data for {sym} ({canonical})")
    else:
        # Single ticker — columns are just Price names
        if len(yf_symbols) == 1:
            sym = yf_symbols[0]
            canonical = yf_to_canonical[sym]
            close_series = df["Close"].dropna()
            if not close_series.empty:
                for dt_idx, close_val in close_series.items():
                    val = float(close_val)
                    if not math.isfinite(val) or val <= 0:
                        continue
                    d = dt_idx.strftime("%Y-%m-%d")
                    prices_to_store.setdefault(canonical, []).append({
                        "date": d,
                        "close": val,
                    })
                latest = float(close_series.iloc[-1])
                if math.isfinite(latest) and latest > 0:
                    latest_prices[canonical] = latest

    # Persist to DB. executemany batches the INSERTs into a single
    # statement-prepare cycle — for a typical 80-ticker × 5-day fetch
    # that's ~400 rows, dropping per-row overhead by an order of magnitude.
    batch = [
        (ticker, row["date"], row["close"])
        for ticker, rows in prices_to_store.items()
        for row in rows
    ]
    if batch:
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO price_history "
                "(ticker, date, close, source) VALUES (?, ?, ?, 'yfinance')",
                batch,
            )
        except sqlite3.Error as e:
            log.error(f"DB batch insert failed for {len(batch)} rows: {e}")
    conn.commit()

    log.info(f"yFinance: got prices for {len(latest_prices)}/{len(yf_symbols)} tickers")
    return latest_prices


def backfill_yfinance(conn: sqlite3.Connection,
                      tickers: list[str] | None = None,
                      period: str = "1y") -> dict[str, float]:
    """One-time backfill of price_history with historical daily closes.

    Same as fetch_yfinance_daily but defaults to 1 year.
    """
    log.info(f"Backfilling yFinance prices (period={period})…")
    return fetch_yfinance_daily(conn, tickers=tickers, period=period)




# ---------------------------------------------------------------------------
# Unified Price Interface
# ---------------------------------------------------------------------------

def _fetch_yf_cached() -> dict[str, float]:
    """Return yFinance prices, using a 5-minute TTL cache.

    Thread-safe. On cache miss, fetches all mapped tickers in one batch call.
    """
    global _yf_cache, _yf_cache_time
    now = time.monotonic()
    if _yf_cache and (now - _yf_cache_time) < YF_CACHE_TTL_SECONDS:
        return _yf_cache

    with _yf_cache_lock:
        # Double-check after acquiring lock
        if _yf_cache and (time.monotonic() - _yf_cache_time) < YF_CACHE_TTL_SECONDS:
            return _yf_cache

        try:
            import yfinance as yf  # noqa: F401 — verified by _yf_download_with_timeout
        except ImportError:
            log.warning("yfinance not installed")
            return _yf_cache or {}

        yf_symbols = [v for v in YFINANCE_MAP.values() if v is not None]
        yf_to_canonical = {v: k for k, v in YFINANCE_MAP.items() if v is not None}

        try:
            df = _yf_download_with_timeout(yf_symbols, period="1d")
        except TimeoutError as e:
            log.error(f"yFinance cache refresh timed out: {e}")
            return _yf_cache or {}
        except Exception as e:
            log.error(f"yFinance batch download failed: {e}")
            return _yf_cache or {}

        if df.empty:
            return _yf_cache or {}

        prices: dict[str, float] = {}
        if isinstance(df.columns, __import__("pandas").MultiIndex):
            for sym in yf_symbols:
                try:
                    close_series = df["Close"][sym].dropna()
                    if not close_series.empty:
                        val = float(close_series.iloc[-1])
                        if math.isfinite(val) and val > 0:
                            prices[yf_to_canonical[sym]] = val
                except (KeyError, IndexError):
                    pass
        else:
            # Single ticker
            close_series = df["Close"].dropna()
            if not close_series.empty and len(yf_symbols) == 1:
                val = float(close_series.iloc[-1])
                if math.isfinite(val) and val > 0:
                    prices[yf_to_canonical[yf_symbols[0]]] = val

        if prices:
            _yf_cache = prices
            _yf_cache_time = time.monotonic()
            log.info(f"yFinance cache refreshed: {len(prices)} prices")

        return _yf_cache


_DATABENTO_ALIAS = {"GOOG": "GOOGL"}
_DATABENTO_ALIAS_REV = {v: k for k, v in _DATABENTO_ALIAS.items()}


def _fetch_databento_prices(conn: sqlite3.Connection,
                            tickers: list[str]) -> dict[str, dict]:
    """Read fresh DataBento equity prices from price_history.

    FischerDaily's equity stream writes 1-second quotes to the shared DB
    with source='DATABENTO_EQUITY'.  We read them here so the dashboard
    gets real-time prices without a yFinance round-trip.

    Returns {canonical_ticker: {"price", "source", "as_of"}} for entries
    fetched within the last 10 minutes.
    """
    # Map canonical tickers to DataBento tickers (e.g. GOOG → GOOGL)
    db_tickers = [_DATABENTO_ALIAS.get(t, t) for t in tickers]
    ph = ",".join("?" for _ in db_tickers)
    # fetched_at is stored in UTC (SQLite datetime('now')), so compare in UTC
    cutoff = (datetime.now(UTC) - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")

    rows = conn.execute(f"""
        SELECT ticker, close, fetched_at
        FROM latest_prices
        WHERE source = 'DATABENTO_EQUITY'
          AND ticker IN ({ph})
          AND fetched_at >= ?
    """, [*db_tickers, cutoff]).fetchall()

    result: dict[str, dict] = {}
    for r in rows:
        db_ticker = r["ticker"]
        canonical = _DATABENTO_ALIAS_REV.get(db_ticker, db_ticker)
        result[canonical] = {
            "price": r["close"],
            "source": "DATABENTO_EQUITY",
            "as_of": r["fetched_at"],
        }
    return result


def get_current_prices(conn: sqlite3.Connection,
                       tickers: list[str] | None = None,
                       try_t1: bool = True) -> dict[str, dict]:
    """Get the best available price for each ticker.

    Priority:
      1. DataBento equity stream (from FischerDaily, if fresh within 10 min)
      2. yFinance (with 5-minute TTL cache)
      3. Cached in price_history DB (if fresh within 48h)

    The try_t1 parameter is accepted for backward compatibility but ignored.

    Returns:
        {ticker: {"price": float, "source": str, "as_of": str}}
    """
    all_tickers = tickers or list(YFINANCE_MAP.keys())
    result: dict[str, dict] = {}

    # 1. DataBento equity stream (real-time, from FischerDaily's shared DB)
    try:
        db_prices = _fetch_databento_prices(conn, all_tickers)
        result.update(db_prices)
    except Exception as e:
        log.debug(f"DataBento price lookup failed: {e}")

    # 2. yFinance with TTL cache (fill gaps)
    missing = set(all_tickers) - set(result.keys())
    if missing:
        yf_prices = _fetch_yf_cached()
        now_str = datetime.now().isoformat(timespec="seconds")
        for ticker, price in yf_prices.items():
            if ticker in missing:
                result[ticker] = {
                    "price": price,
                    "source": "yfinance",
                    "as_of": now_str,
                }

    # 3. Fill remaining gaps from DB cache
    missing = set(all_tickers) - set(result.keys())
    if missing:
        cached = get_cached_prices(conn, list(missing), max_age_hours=48)
        for ticker, data in cached.items():
            result[ticker] = {
                "price": data["close"],
                "source": data["source"],
                "as_of": data["date"],
            }

    return result


def get_prices_with_signal_context(conn: sqlite3.Connection,
                                   tickers: list[str] | None = None,
                                   try_t1: bool = True) -> list[dict]:
    """Join current signal state with live prices and compute P/L metrics.

    Returns list of dicts, each containing:
        - All current_state fields (ticker, instrument, asset_class, effective_signal, …)
        - price, price_source, price_as_of
        - pnl (price - origin_price)
        - pnl_pct ((price - origin) / origin * 100)
        - cancel_dist_pct ((cancel - price) / price * 100, signed)
        - trigger_dist_pct ((trigger - price) / price * 100, signed)
    """
    # Fetch signal states
    rows = conn.execute("""
        SELECT ticker, instrument, asset_class, effective_signal,
               origin_price, cancel_direction, cancel_level,
               trigger_level, implied_reversal, last_signal_date
        FROM current_state
        ORDER BY asset_class, instrument
    """).fetchall()

    signal_tickers = [r["ticker"] for r in rows]
    if tickers:
        signal_tickers = [t for t in signal_tickers if t in tickers]

    # Fetch prices
    prices = get_current_prices(conn, signal_tickers, try_t1=try_t1)

    # Fetch latest unreached price targets per ticker+direction
    targets_by_ticker: dict[str, list[dict]] = {}
    if signal_tickers:
        ph = ",".join("?" for _ in signal_tickers)
        target_rows = conn.execute(f"""
            WITH recent_emails AS (
                SELECT id FROM emails ORDER BY date_sent DESC, id DESC LIMIT 5
            )
            SELECT pt.ticker, pt.target_price, pt.direction
            FROM price_targets pt
            INNER JOIN (
                SELECT ticker, direction, MAX(date) AS max_date
                FROM price_targets
                WHERE ticker IN ({ph}) AND reached = 0
                GROUP BY ticker, direction
            ) latest ON pt.ticker = latest.ticker
                    AND pt.direction = latest.direction
                    AND pt.date = latest.max_date
            WHERE pt.reached = 0
            AND EXISTS (
                SELECT 1 FROM price_targets pt2
                WHERE pt2.ticker = pt.ticker
                  AND pt2.email_id IN (SELECT id FROM recent_emails)
            )
            ORDER BY pt.ticker, pt.target_price
        """, signal_tickers).fetchall()
        for tr in target_rows:
            targets_by_ticker.setdefault(tr["ticker"], []).append(dict(tr))

    # Enrich each signal row with price data
    enriched = []
    for row in rows:
        d = dict(row)
        ticker = d["ticker"]
        if tickers and ticker not in tickers:
            continue

        price_info = prices.get(ticker)
        if price_info:
            d["price"] = price_info["price"]
            d["price_source"] = price_info["source"]
            d["price_as_of"] = price_info["as_of"]

            origin = d.get("origin_price")
            price = price_info["price"]

            # P/L calculation
            if origin and origin != 0 and price:
                d["pnl"] = price - origin
                d["pnl_pct"] = (price - origin) / abs(origin) * 100
                # For SELL signals, P/L is inverted (profit when price drops)
                if d.get("effective_signal") == "SELL":
                    d["pnl"] = -d["pnl"]
                    d["pnl_pct"] = -d["pnl_pct"]
            else:
                d["pnl"] = None
                d["pnl_pct"] = None

            # Cancel distance
            cancel = d.get("cancel_level")
            if cancel and price and price != 0:
                d["cancel_dist_pct"] = (cancel - price) / abs(price) * 100
            else:
                d["cancel_dist_pct"] = None

            # Trigger distance
            trigger = d.get("trigger_level")
            if trigger and price and price != 0:
                d["trigger_dist_pct"] = (trigger - price) / abs(price) * 100
            else:
                d["trigger_dist_pct"] = None
        else:
            d["price"] = None
            d["price_source"] = None
            d["price_as_of"] = None
            d["pnl"] = None
            d["pnl_pct"] = None
            d["cancel_dist_pct"] = None
            d["trigger_dist_pct"] = None

        # Price targets — pick the one aligned with signal direction
        ticker_targets = targets_by_ticker.get(ticker, [])
        sig_dir = d.get("effective_signal", "")
        target_dir = "UPSIDE" if sig_dir == "BUY" else "DOWNSIDE" if sig_dir == "SELL" else None
        matched = [t for t in ticker_targets if t["direction"] == target_dir] if target_dir else []
        if matched:
            d["target_price"] = matched[0]["target_price"]
            d["target_direction"] = matched[0]["direction"]
            price = d.get("price")
            if price and price != 0 and d["target_price"]:
                d["target_dist_pct"] = (d["target_price"] - price) / abs(price) * 100
            else:
                d["target_dist_pct"] = None
        else:
            d["target_price"] = None
            d["target_direction"] = None
            d["target_dist_pct"] = None

        enriched.append(d)

    return enriched
