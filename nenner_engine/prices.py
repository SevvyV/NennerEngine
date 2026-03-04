"""
Price Data Layer
=================
Fetches, caches, and serves price data from two sources:
  1. yFinance — daily OHLC closes for all instruments (batch download)
  2. xlwings / T1 (LSEG) — real-time quotes via RTD formulas in Excel

Provides a unified interface for the dashboard and CLI.
"""

import logging
import sqlite3
from datetime import datetime, date, timedelta
from typing import Optional

from .config import T1_WORKBOOK

log = logging.getLogger("nenner")


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
    "LBS":      "LBS=F",
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
# Ticker Mapping: Canonical → LSEG RIC (for T1 Excel RTD)
# ---------------------------------------------------------------------------
# Futures month codes: F=Jan G=Feb H=Mar J=Apr K=May M=Jun
#                      N=Jul Q=Aug U=Sep V=Oct X=Nov Z=Dec
# /1 = front-month continuous contract

LSEG_RIC_MAP: dict[str, str | None] = {
    # Equity Indices (futures)
    "ES":       "ES/1",
    "NQ":       "NQ/1-CM,USD,NORM",
    "YM":       "D./1",                 # Dow Jones mini uses "D." as the LSEG root
    # Equity Indices (cash index) — corrected to user's T1 entitlements
    "NYFANG":   "NYFANG-P",
    "VIX":      "VIX-UT",               # #NT/FND — not in current entitlements
    "TSX":      "T.TOR-T,CAD,NORM,D",
    "DAX":      "DAX-XE",
    "FTSE":     "UKX-FT",
    "AEX":      "AEX-AE,EUR,NORM,D",
    "NYA":      "NYA-P,USD,NORM,D",
    "SMI":      "SLA1YN-ST,CHF,NORM,D",
    "BTK":      "BTK-P",
    # Precious Metals (futures)
    "GC":       "GC/1",
    "SI":       "SI/1",
    "HG":       "HG/1",
    # Precious Metals (ETF / Stock — same ticker on LSEG)
    "GLD":      "GLD",
    "GDXJ":     "GDXJ",
    "NEM":      "NEM",
    "SLV":      "SLV",
    # Energy (futures)
    "CL":       "CL/1",
    "NG":       "NG/1",
    # Energy (ETFs)
    "USO":      "USO",
    "UNG":      "UNG",
    # Agriculture (futures) — updated to match Nenner_DataCenter RICs
    "ZC":       "ZG/1",
    "ZS":       "ZK/1",
    "ZW":       "ZW/H26",       # contract-specific — needs manual roll
    "LBS":      "LBR/1",
    # Agriculture (ETFs)
    "CORN":     "CORN",
    "SOYB":     "SOYB",
    "WEAT":     "WEAT",
    # Fixed Income (futures) — US/1 and FGBL/1 are #NT/FND
    "ZB":       "US/1",
    "ZN":       "ZN/1-CB,USD,NORM,D",
    # Fixed Income (ETF)
    "TLT":      "TLT",
    # Fixed Income (Europe)
    "FGBL":     "FGBL/1",
    # Currencies — all use =-FX suffix
    "DXY":      "NYICDX-P",
    "EUR/USD":  "EUR=-FX",
    "FXE":      "FXE",
    "AUD/USD":  "AUD=-FX",
    "USD/CAD":  "CAD=-FX",
    "USD/JPY":  "JPY=-FX",
    "USD/CHF":  "CHF=-FX",
    "GBP/USD":  "GBP=-FX",
    "USD/BRL":  "BRL=-FX",
    "USD/ILS":  "ILS=-FX",
    # Crypto — use =-FX suffix
    "BTC":      "BTC=-FX",
    "ETH":      "ETH=-FX",
    # Crypto ETFs
    "GBTC":     "GBTC",
    "ETHE":     "ETHE",
    "BITO":     "BITO",
    # Single Stocks
    "AAPL":     "AAPL",
    "GOOG":     "GOOG",
    "BAC":      "BAC",
    "MSFT":     "MSFT",
    "NVDA":     "NVDA",
    "TSLA":     "TSLA",
    "QQQ":      "QQQ",
    "SIL":      "SIL",
}

# Reverse lookup: LSEG RIC → canonical ticker
_RIC_REVERSE = {v: k for k, v in LSEG_RIC_MAP.items() if v is not None}


# ---------------------------------------------------------------------------
# T1 / xlwings Configuration
# ---------------------------------------------------------------------------

# Two-sheet layout in Nenner_DataCenter.xlsm:
#   Equities_RT    — stocks & ETFs  (col A = ticker, B = BID, E = LAST)
#   Futures_FX_RT  — futures, FX, indices, crypto, agriculture
#                    (col A = instrument name, B = RIC, C = BID, E = LAST)
T1_SHEETS: dict[str, dict] = {
    "Equities_RT": {
        "ticker_col": "A",     # Column A = ticker symbol (TSLA, AAPL, etc.)
        "ric_col": None,       # No RIC column — ticker IS the identifier
        "bid_col": "B",        # BID price
        "last_col": "E",       # LAST price (fallback when BID empty/zero)
        "data_start_row": 5,
    },
    "Futures_FX_RT": {
        "ticker_col": "A",    # Column A = instrument name (human-readable)
        "ric_col": "B",       # Column B = LSEG RIC
        "bid_col": "C",       # BID price
        "last_col": "E",      # LAST price (fallback when BID empty/zero)
        "data_start_row": 5,
    },
}

# Section headers in Futures_FX_RT to skip (these are labels, not data rows).
# When we hit one, we also skip the column-header row below it.
_SECTION_HEADERS = {
    "FUTURES", "FOREIGN EXCHANGE", "INDICES", "CRYPTO & AGRICULTURE",
    "Instrument",  # column header row
    "REAL-TIME FUTURES, FX & INDICES",  # sheet title
    "REAL-TIME EQUITY & ETF QUOTES",    # sheet title
}

# Instrument name → canonical ticker for Futures_FX_RT rows where
# column A is a human-readable name and the RIC doesn't reverse-map.
_INSTRUMENT_NAME_MAP: dict[str, str] = {
    "Nasdaq 100 E-mini": "NQ",
    "S&P 500 E-mini": "ES",
    "Dow E-mini": "YM",
    "Gold Futures": "GC",
    "Silver Futures": "SI",
    "Copper Futures": "HG",
    "Crude Oil WTI": "CL",
    "Natural Gas": "NG",
    "US Treasury Bond": "ZB",
    "10-Year T-Note": "ZN",
    "30-Year T-Bond": "ZB",
    "US Dollar Index Futures": "DXY",
    "Bitcoin (BTC)": "BTC",
    "Ethereum (ETH)": "ETH",
}

# Session-level T1 state: once we detect the workbook isn't open,
# we disable T1 for the rest of this process and send one email.
_t1_disabled_for_session: bool = False
_t1_email_sent: bool = False


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
    cutoff = (datetime.now() - timedelta(hours=max_age_hours)).isoformat()
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
        df = yf.download(yf_symbols, period=period, progress=False, threads=True)
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
                # Store all fetched rows
                for dt_idx, close_val in close_series.items():
                    d = dt_idx.strftime("%Y-%m-%d")
                    prices_to_store.setdefault(canonical, []).append({
                        "date": d,
                        "close": float(close_val),
                    })
                # Latest close
                latest_prices[canonical] = float(close_series.iloc[-1])
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
                    d = dt_idx.strftime("%Y-%m-%d")
                    prices_to_store.setdefault(canonical, []).append({
                        "date": d,
                        "close": float(close_val),
                    })
                latest_prices[canonical] = float(close_series.iloc[-1])

    # Persist to DB
    for ticker, rows in prices_to_store.items():
        for row in rows:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO price_history
                        (ticker, date, close, source)
                    VALUES (?, ?, ?, 'yfinance')
                """, (ticker, row["date"], row["close"]))
            except sqlite3.Error as e:
                log.debug(f"DB insert error for {ticker}: {e}")
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
# xlwings / T1 Bridge
# ---------------------------------------------------------------------------

def _is_workbook_open() -> bool:
    """Check if T1_WORKBOOK is already open in a running Excel instance.

    Compares by filename only (not full path) because OneDrive sync
    can cause the Excel-reported path to differ from the local path.

    Iterates through running Excel apps without launching a new one.
    Returns False if xlwings isn't installed or no Excel is running.
    """
    import os
    target_name = os.path.basename(T1_WORKBOOK).lower()
    try:
        import xlwings as xw
        for app in xw.apps:
            for wb in app.books:
                if wb.name.lower() == target_name:
                    return True
    except Exception:
        pass
    return False


def _send_t1_open_email():
    """Send a one-time email asking user to open the T1 workbook.

    Only sends once per session (tracked by _t1_email_sent flag).
    """
    global _t1_email_sent
    if _t1_email_sent:
        return
    try:
        from .postmaster import send_email
        send_email(
            "NennerEngine: Please open Nenner_DataCenter.xlsm",
            "<p>NennerEngine tried to read T1 live prices but the workbook "
            "is not open in Excel.</p>"
            f"<p>Please open: <b>{T1_WORKBOOK}</b></p>"
            "<p>T1 price fetching is disabled for this session. "
            "Restart the engine after opening the workbook to re-enable "
            "live prices.</p>",
        )
        _t1_email_sent = True
        log.info("Sent email notification: T1 workbook not open")
    except Exception as e:
        log.error(f"Failed to send T1 open-workbook email: {e}")


def _resolve_ticker(col_a: str, col_b: str | None) -> str | None:
    """Resolve canonical ticker from a spreadsheet row.

    Tries these strategies in order:
      1. Direct match — col_a IS a canonical ticker (Equities_RT tickers,
         FX pairs like "EUR/USD", indices like "VIX", "DXY", etc.)
      2. RIC reverse — col_b maps via _RIC_REVERSE
      3. Parenthetical — extract from "Corn Futures (ZC)" → ZC
      4. Instrument name — col_a maps via _INSTRUMENT_NAME_MAP

    Returns canonical ticker string, or None if unresolvable.
    """
    import re

    # 1. Direct: col_a is itself a known canonical ticker
    if col_a in LSEG_RIC_MAP:
        return col_a

    # 2. RIC reverse lookup
    if col_b:
        ric = str(col_b).strip()
        canonical = _RIC_REVERSE.get(ric)
        if canonical:
            return canonical

    # 3. Parenthetical: "Corn Futures (ZC)" → ZC
    m = re.search(r"\(([A-Z/]+)\)", col_a)
    if m:
        candidate = m.group(1)
        if candidate in LSEG_RIC_MAP:
            return candidate

    # 4. Instrument name fallback
    return _INSTRUMENT_NAME_MAP.get(col_a)


def _extract_price(ws, row: int, bid_col: str, last_col: str) -> float | None:
    """Read price from BID column, falling back to LAST.

    Filters out RTD error values (#N/A, #NT/FND), zero, and None.
    Returns a valid float price, or None if no usable price.
    """
    for col in (bid_col, last_col):
        val = ws.range(f"{col}{row}").value
        if val is None:
            continue
        if isinstance(val, str):
            # RTD error strings like "#N/A", "#NT/FND"
            if val.startswith("#"):
                continue
            val = val.strip()
            if not val:
                continue
        try:
            price = float(val)
            if price > 0:
                return price
        except (ValueError, TypeError):
            continue
    return None


def read_t1_prices() -> dict[str, float]:
    """Read real-time prices from Nenner_DataCenter.xlsm via xlwings.

    Reads two sheets:
      - Equities_RT:    col A = ticker, col B = BID, col E = LAST
      - Futures_FX_RT:  col A = instrument name, col B = RIC,
                        col C = BID, col E = LAST

    For each row, resolves the canonical ticker (direct match, RIC reverse,
    parenthetical extraction, or instrument name map) and extracts the price
    (BID preferred, LAST as fallback for indices/FX without streaming BID).

    If the workbook is not already open, sends a one-time email notification
    and disables T1 for the rest of this session (no auto-launch of Excel).

    Returns:
        {canonical_ticker: price} dict. Empty dict if workbook unavailable.
    """
    global _t1_disabled_for_session
    if _t1_disabled_for_session:
        return {}

    try:
        import xlwings as xw
    except ImportError:
        log.debug("xlwings not installed — T1 bridge unavailable")
        return {}

    if not _is_workbook_open():
        log.info("T1 workbook not open — disabling T1 for this session")
        _t1_disabled_for_session = True
        _send_t1_open_email()
        return {}

    # Workbook IS already open — connect by filename only to avoid
    # OneDrive path mismatch (Excel reports OneDrive path, we have local).
    import os
    wb_name = os.path.basename(T1_WORKBOOK)
    try:
        wb = xw.Book(wb_name)
    except Exception as e:
        log.debug(f"Cannot connect to T1 workbook '{wb_name}': {e}")
        return {}

    prices: dict[str, float] = {}

    for sheet_name, cfg in T1_SHEETS.items():
        try:
            ws = wb.sheets[sheet_name]
        except Exception:
            log.warning(f"T1 sheet '{sheet_name}' not found, skipping")
            continue

        row = cfg["data_start_row"]
        max_row = 200  # safety limit
        consecutive_empty = 0

        while row < max_row:
            col_a = ws.range(f'{cfg["ticker_col"]}{row}').value

            if col_a is None:
                consecutive_empty += 1
                # Stop after 5 consecutive empty rows (end of data)
                if consecutive_empty >= 5:
                    break
                row += 1
                continue
            consecutive_empty = 0

            col_a = str(col_a).strip()

            # Skip section headers and column labels
            if col_a in _SECTION_HEADERS:
                row += 1
                continue

            # Read RIC if this sheet has one
            col_b = None
            if cfg["ric_col"]:
                raw_b = ws.range(f'{cfg["ric_col"]}{row}').value
                if raw_b is not None:
                    col_b = str(raw_b).strip() or None

            # Resolve canonical ticker
            ticker = _resolve_ticker(col_a, col_b)
            if not ticker:
                log.debug(f"T1: unmapped row {sheet_name}!{row}: "
                          f"A={col_a!r} B={col_b!r}")
                row += 1
                continue

            # Extract price (BID → LAST fallback)
            price = _extract_price(
                ws, row, cfg["bid_col"], cfg["last_col"]
            )
            if price is not None and ticker not in prices:
                prices[ticker] = price

            row += 1

    log.info(f"T1 xlwings: read {len(prices)} live prices "
             f"from {len(T1_SHEETS)} sheets")
    return prices


def store_t1_prices(conn: sqlite3.Connection, prices: dict[str, float] | None = None):
    """Read T1 prices and store them in price_history.

    Args:
        conn: SQLite connection.
        prices: Pre-fetched T1 prices, or None to read fresh.
    """
    if prices is None:
        prices = read_t1_prices()
    if not prices:
        return

    today = date.today().isoformat()
    for ticker, price_val in prices.items():
        try:
            conn.execute("""
                INSERT OR REPLACE INTO price_history
                    (ticker, date, close, source)
                VALUES (?, ?, ?, 'xlwings')
            """, (ticker, today, price_val))
        except sqlite3.Error as e:
            log.debug(f"DB insert error for T1 {ticker}: {e}")
    conn.commit()
    log.info(f"Stored {len(prices)} T1 prices")


def setup_t1_sheet():
    """Populate the Nenner_Stock sheet with all instruments and RTD formulas.

    Writes LSEG RIC codes to column B and RTD BID formulas to column C.
    Organizes instruments by asset class with section headers.
    """
    try:
        import xlwings as xw
    except ImportError:
        log.error("xlwings not installed — cannot set up T1 sheet")
        return

    try:
        wb = xw.Book(T1_WORKBOOK)
        ws = wb.sheets[T1_SHEET]
    except Exception as e:
        log.error(f"Cannot open T1 workbook: {e}")
        return

    # Define the instrument layout grouped by asset class
    from .instruments import INSTRUMENT_MAP

    sections = [
        ("Single Stocks", [
            ("AAPL", "AAPL"), ("GOOG", "GOOG"), ("BAC", "BAC"),
            ("MSFT", "MSFT"), ("NVDA", "NVDA"), ("TSLA", "TSLA"),
            ("NEM", "NEM"),
        ]),
        ("Precious Metals", [
            ("GC", "GC/1"), ("SI", "SI/1"), ("HG", "HG/1"),
            ("GLD", "GLD"), ("GDXJ", "GDXJ"), ("SLV", "SLV"),
        ]),
        ("Energy", [
            ("CL", "CL/1"), ("NG", "NG/1"),
            ("USO", "USO"), ("UNG", "UNG"),
        ]),
        ("Equity Indices", [
            ("ES", "ES/1"), ("NQ", "NQ/1"), ("YM", "D./1"),
            ("VIX", ".VIX"), ("DXY", "DXY.N"),
            ("DAX", ".GDAXI"), ("FTSE", ".FTSE"),
            ("TSX", ".GSPTSE"), ("AEX", ".AEX"),
            ("NYA", ".NYA"), ("SMI", ".SSMI"),
            ("BTK", ".BTK"), ("NYFANG", ".NYFANG"),
        ]),
        ("Agriculture", [
            ("ZC", "C./1"), ("ZS", "S./1"), ("ZW", "W./1"), ("LBS", "LBS/1"),
            ("CORN", "CORN"), ("SOYB", "SOYB"), ("WEAT", "WEAT"),
        ]),
        ("Fixed Income", [
            ("ZB", "US/1"), ("ZN", "TY/1"), ("FGBL", "FGBL/1"),
            ("TLT", "TLT"),
        ]),
        ("Currencies", [
            ("EUR/USD", "EUR="), ("GBP/USD", "GBP="),
            ("USD/JPY", "JPY="), ("USD/CHF", "CHF="),
            ("AUD/USD", "AUD="), ("USD/CAD", "CAD="),
            ("USD/BRL", "BRL="), ("USD/ILS", "ILS="),
            ("FXE", "FXE"),
        ]),
        ("Crypto", [
            ("BTC", "BTC="), ("ETH", "ETH="),
            ("GBTC", "GBTC"), ("ETHE", "ETHE"), ("BITO", "BITO"),
        ]),
    ]

    # Write headers (row 3-4 area)
    ws.range("B3").value = "RIC"
    ws.range("C3").value = "BID"
    ws.range("D3").value = "Instrument"

    row = T1_DATA_START_ROW
    for section_name, instruments in sections:
        # Section header
        ws.range(f"B{row}").value = f"--- {section_name} ---"
        ws.range(f"B{row}").font.bold = True
        ws.range(f"B{row}").font.color = (150, 150, 150)
        row += 1

        for canonical, ric in instruments:
            info = INSTRUMENT_MAP.get(
                next((k for k, v in INSTRUMENT_MAP.items() if v["ticker"] == canonical), ""),
                {"ticker": canonical}
            )
            ws.range(f"B{row}").value = ric
            ws.range(f"C{row}").formula = f'=RTD("tf.rtdsvr",,"Q",$B{row},"BID")'
            ws.range(f"D{row}").value = canonical
            row += 1

        row += 1  # blank row between sections

    log.info(f"T1 sheet set up with instruments through row {row}")
    print(f"T1 Nenner_Stock sheet populated through row {row}.")
    print("RTD formulas will start streaming once LSEG T1 connects.")


# ---------------------------------------------------------------------------
# Unified Price Interface
# ---------------------------------------------------------------------------

def get_current_prices(conn: sqlite3.Connection,
                       tickers: list[str] | None = None,
                       try_t1: bool = True) -> dict[str, dict]:
    """Get the best available price for each ticker.

    Priority:
      1. xlwings/T1 (real-time, if available and try_t1=True)
      2. Cached in price_history (if fresh within 24h)
      3. (Does NOT trigger yFinance fetch — that's a separate CLI/cron action)

    Returns:
        {ticker: {"price": float, "source": str, "as_of": str}}
    """
    result: dict[str, dict] = {}

    # 1. Try T1 real-time prices
    t1_prices: dict[str, float] = {}
    if try_t1:
        try:
            t1_prices = read_t1_prices()
            # Also cache T1 prices in DB for later use
            if t1_prices:
                store_t1_prices(conn, t1_prices)
        except Exception as e:
            log.debug(f"T1 price read failed: {e}")

    for ticker, price in t1_prices.items():
        if tickers is None or ticker in tickers:
            result[ticker] = {
                "price": price,
                "source": "T1",
                "as_of": datetime.now().isoformat(timespec="seconds"),
            }

    # 2. Fill gaps from DB cache
    missing = set(tickers or list(YFINANCE_MAP.keys())) - set(result.keys())
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

        enriched.append(d)

    return enriched
