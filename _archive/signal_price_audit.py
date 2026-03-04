"""
Signal Price Audit
==================
Downloads historical daily closes from Yahoo Finance for all tracked instruments
and cross-references them against signal origin_prices and cancel_levels to
identify parsing errors, scale mismatches, and cross-instrument contamination.

Outputs:
  1. Console report of all flagged signals with severity ratings
  2. signal_price_audit_report.txt for reference
  3. With --fix: applies auto-corrections to the database

Usage:
    python signal_price_audit.py                  # Full audit -> report
    python signal_price_audit.py --fix            # Audit + apply auto-corrections
    python signal_price_audit.py --ticker ES      # Audit single ticker
    python signal_price_audit.py --skip-download  # Use cached price_history only
"""

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nenner_signals.db")

# Nenner ticker -> yFinance symbol
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
    "BTK":      "^BTK",
    # Precious Metals (futures)
    "GC":       "GC=F",
    "SI":       "SI=F",
    "HG":       "HG=F",
    # Precious Metals (ETF/Stock)
    "GLD":      "GLD",
    "GDXJ":     "GDXJ",
    "NEM":      "NEM",
    "SLV":      "SLV",
    # Energy (futures)
    "CL":       "CL=F",
    "NG":       "NG=F",
    # Energy (ETFs)
    "USO":      "USO",
    "UNG":      "UNG",
    # Agriculture (futures)
    "ZC":       "ZC=F",
    "ZS":       "ZS=F",
    "ZW":       "ZW=F",
    "LBS":      "LBS=F",
    # Agriculture (ETFs)
    "CORN":     "CORN",
    "SOYB":     "SOYB",
    "WEAT":     "WEAT",
    # Fixed Income (futures)
    "ZB":       "ZB=F",
    "ZN":       "ZN=F",
    # Fixed Income (ETF)
    "TLT":      "TLT",
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
    "AMZN":     "AMZN",
    "MMM":      "MMM",
    "C":        "C",
    "GS":       "GS",
    "AXP":      "AXP",
}

# Ratio thresholds for flagging
# origin_price can be far from current close (references historical entry)
# cancel_level should be near current close (it's the cancellation boundary)
ORIGIN_FLAG_RATIO = 3.0     # flag if signal/close > 3x or < 0.33x
CANCEL_FLAG_RATIO = 2.5     # tighter for cancel levels

# Instruments where yFinance shows split-adjusted prices, so old signal
# prices will deviate but are NOT parsing errors.
SPLIT_ADJUSTED_TICKERS = {
    # ETFs with reverse splits
    "WEAT",   # multiple reverse splits
    "UNG",    # multiple reverse splits
    "BITO",   # reverse split
    "CORN",   # reverse split
    "SOYB",   # reverse split
    "USO",    # 1:8 reverse split April 2020
    "GBTC",   # NAV adjustments / trust conversions
    "ETHE",   # NAV adjustments / trust conversions
    # Stocks with forward splits (yFinance shows adjusted)
    "AAPL",   # 4:1 split Aug 2020
    "AMZN",   # 20:1 split Jun 2022
    "GOOG",   # 20:1 split Jul 2022
    "TSLA",   # 5:1 Aug 2020 + 3:1 Aug 2022
    "NVDA",   # 10:1 split Jun 2024
}


# ---------------------------------------------------------------------------
# Phase 1: Download Historical Prices
# ---------------------------------------------------------------------------

def download_history(tickers=None, start="2018-01-01"):
    """Download daily closes from Yahoo Finance for all instruments.

    Returns dict: {nenner_ticker: pd.Series(date_string_index -> close)}
    """
    if tickers is None:
        tickers = list(YFINANCE_MAP.keys())

    yf_symbols = []
    yf_to_nenner = {}
    for t in tickers:
        sym = YFINANCE_MAP.get(t)
        if sym:
            yf_symbols.append(sym)
            yf_to_nenner[sym] = t

    if not yf_symbols:
        print("No valid yFinance symbols to download")
        return {}

    print(f"Downloading {len(yf_symbols)} tickers from Yahoo Finance ({start} to today)...")
    print(f"  Symbols: {', '.join(yf_symbols[:10])}{'...' if len(yf_symbols) > 10 else ''}")

    try:
        df = yf.download(yf_symbols, start=start, progress=True, threads=True)
    except Exception as e:
        print(f"ERROR: yFinance download failed: {e}")
        return {}

    if df.empty:
        print("ERROR: yFinance returned empty DataFrame")
        return {}

    closes: dict[str, pd.Series] = {}

    if isinstance(df.columns, pd.MultiIndex):
        # Multiple tickers — columns are (Price, Ticker)
        for sym in yf_symbols:
            nenner = yf_to_nenner[sym]
            try:
                series = df["Close"][sym].dropna()
                if not series.empty:
                    series.index = series.index.strftime("%Y-%m-%d")
                    closes[nenner] = series
            except (KeyError, IndexError):
                print(f"  WARNING: No data for {nenner} ({sym})")
    elif len(yf_symbols) == 1:
        # Single ticker
        sym = yf_symbols[0]
        nenner = yf_to_nenner[sym]
        series = df["Close"].dropna()
        if not series.empty:
            series.index = series.index.strftime("%Y-%m-%d")
            closes[nenner] = series

    print(f"\nGot history for {len(closes)}/{len(yf_symbols)} tickers:")
    for t, s in sorted(closes.items()):
        print(f"  {t:12s}: {s.index[0]} to {s.index[-1]}  ({len(s):,} days)")

    missing = set(yf_to_nenner.values()) - set(closes.keys())
    if missing:
        print(f"\n  MISSING: {', '.join(sorted(missing))}")

    return closes


def store_history_to_db(conn, closes):
    """Optionally store downloaded history into price_history table."""
    total = 0
    for ticker, series in closes.items():
        for date_str, close_val in series.items():
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO price_history
                        (ticker, date, close, source)
                    VALUES (?, ?, ?, 'yfinance_backfill')
                """, (ticker, date_str, float(close_val)))
                total += 1
            except sqlite3.Error:
                pass
    conn.commit()
    print(f"Stored {total:,} price rows in price_history")


# ---------------------------------------------------------------------------
# Phase 2: Load Signals
# ---------------------------------------------------------------------------

def load_signals(conn, ticker=None):
    """Load all signals that have origin_price or cancel_level."""
    sql = """
        SELECT s.id, s.date, s.ticker, s.origin_price, s.cancel_level,
               s.signal_type, s.signal_status, s.raw_text, s.email_id,
               e.subject as email_subject, e.date_sent as email_date
        FROM signals s
        LEFT JOIN emails e ON s.email_id = e.id
        WHERE (s.origin_price IS NOT NULL OR s.cancel_level IS NOT NULL)
    """
    params = []
    if ticker:
        sql += " AND s.ticker = ?"
        params.append(ticker)
    sql += " ORDER BY s.ticker, s.date"

    cursor = conn.execute(sql, params)
    cursor.row_factory = sqlite3.Row
    return cursor.fetchall()


# ---------------------------------------------------------------------------
# Phase 3: Price Lookup
# ---------------------------------------------------------------------------

def find_close(closes_series, date_str, lookback=7):
    """Find close price on date_str, or nearest prior trading day.

    Returns (close_price, actual_date) or (None, None).
    """
    if date_str in closes_series.index:
        return float(closes_series[date_str]), date_str

    # Try prior days (weekends, holidays)
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None, None

    for i in range(1, lookback + 1):
        prior = (dt - timedelta(days=i)).strftime("%Y-%m-%d")
        if prior in closes_series.index:
            return float(closes_series[prior]), prior

    return None, None


# ---------------------------------------------------------------------------
# Phase 4: Classify Errors
# ---------------------------------------------------------------------------

def classify_error(ticker, field, signal_val, actual_close, ratio):
    """Classify the error type and suggest a fix.

    Returns (severity, suggestion, corrected_value_or_None).
    Severity: "AUTO-FIX", "SPLIT" (not an error), "MANUAL", "CHECK"
    """
    # --- Split-adjusted tickers: not parsing errors ---
    if ticker in SPLIT_ADJUSTED_TICKERS:
        return "SPLIT", f"Ratio {ratio:.2f}x -- split-adjusted (not a parsing error)", None

    # --- DXY <-> EUR/USD scale confusion (ratio ~0.01) ---
    if ticker == "DXY" and 0.005 < ratio < 0.025:
        corrected = signal_val * 100
        if 70 < corrected < 135:
            return "AUTO-FIX", f"x100 -> {corrected:.1f} (EUR/USD format in DXY)", corrected

    # --- DXY old notation x100 (9620 = 96.20) ---
    if ticker == "DXY" and 50 < ratio < 200:
        corrected = signal_val / 100
        if 70 < corrected < 135:
            return "AUTO-FIX", f"/100 -> {corrected:.1f} (old notation, 9620=96.20)", corrected

    # --- FX pairs: old dealer notation (Nenner writes 125 meaning 1.2500) ---
    FX_OLD_NOTATION = {
        "EUR/USD":  (0.80, 1.60),
        "AUD/USD":  (0.50, 1.10),
        "GBP/USD":  (1.00, 2.00),
        "USD/CAD":  (0.90, 1.60),
        "USD/CHF":  (0.70, 1.20),
        "USD/ILS":  (2.50, 5.00),
        "USD/BRL":  (2.00, 7.00),
    }
    # Standard x100 notation (125 = 1.2500)
    if ticker in FX_OLD_NOTATION and 30 < ratio < 250:
        lo, hi = FX_OLD_NOTATION[ticker]
        corrected = signal_val / 100
        if lo * 0.8 <= corrected <= hi * 1.2:
            return "AUTO-FIX", f"/100 -> {corrected:.4f} (old FX dealer notation)", corrected

    # Pip notation x10000 (11320 = 1.1320, 13060 = 1.3060)
    if ticker in FX_OLD_NOTATION and 3000 < ratio < 20000:
        lo, hi = FX_OLD_NOTATION[ticker]
        corrected = signal_val / 10000
        if lo * 0.8 <= corrected <= hi * 1.2:
            return "AUTO-FIX", f"/10000 -> {corrected:.4f} (pip notation)", corrected

    # --- Natural Gas old notation (268 = 2.68, 723 = 7.23) ---
    if ticker == "NG" and 30 < ratio < 200:
        corrected = signal_val / 100
        if 1.0 < corrected < 15.0:
            return "AUTO-FIX", f"/100 -> {corrected:.2f} (old NG notation)", corrected

    # --- Copper old notation (270 = 2.70) ---
    if ticker == "HG" and 30 < ratio < 200:
        corrected = signal_val / 100
        if 1.5 < corrected < 6.0:
            return "AUTO-FIX", f"/100 -> {corrected:.2f} (old copper notation)", corrected

    # --- FTSE old notation (74.70 = 7470) ---
    if ticker == "FTSE" and 0.005 < ratio < 0.025:
        corrected = signal_val * 100
        if 5000 < corrected < 12000:
            return "AUTO-FIX", f"x100 -> {corrected:.0f} (old FTSE notation)", corrected

    # --- NQ / QQQ cross-contamination ---
    # Nenner references QQQ ETF prices but parser attributes to NQ futures
    if ticker == "NQ" and 0.01 < ratio < 0.06 and signal_val < 600:
        return "CROSS-REF", f"Value {signal_val:.1f} is QQQ ETF price, not NQ futures (~{actual_close:.0f})", None

    # --- ETH / ETHE cross-contamination ---
    if ticker == "ETH" and ratio < 0.05 and signal_val < 100:
        return "CROSS-REF", f"Value {signal_val:.2f} is ETHE ETF price, not ETH (~{actual_close:.0f})", None

    # --- BTK / IBB cross-contamination ---
    if ticker == "BTK" and ratio < 0.05 and signal_val < 250:
        return "CROSS-REF", f"Value {signal_val:.0f} is IBB ETF price, not BTK index (~{actual_close:.0f})", None

    # --- Soybeans: dropped leading "1," (ratio ~0.4-0.7) ---
    if ticker == "ZS" and 0.3 < ratio < 0.75:
        corrected = signal_val + 1000
        check_ratio = corrected / actual_close
        if 0.7 < check_ratio < 1.4:
            return "AUTO-FIX", f"+1000 -> {corrected:.0f} (dropped leading '1,')", corrected

    # --- Soybeans: SOYB ETF price in ZS (ratio < 0.02) ---
    if ticker == "ZS" and ratio < 0.05:
        return "MANUAL", f"Value {signal_val} looks like SOYB ETF, not ZS futures (~{actual_close:.0f})", None

    # --- Corn: dropped leading digit (ratio ~0.3-0.7) ---
    if ticker == "ZC" and ratio < 0.5:
        corrected = signal_val + 100
        check_ratio = corrected / actual_close
        if 0.7 < check_ratio < 1.4:
            return "AUTO-FIX", f"+100 -> {corrected:.0f} (dropped leading digit)", corrected

    # --- AEX with DAX value (AEX ~300-1000, DAX ~8000-25000) ---
    if ticker == "AEX" and signal_val > 3000:
        return "MANUAL", f"Value {signal_val:.0f} looks like DAX, not AEX (~{actual_close:.0f})", None

    # --- Gold with Silver value (GC ~1000-5000, SI ~10-90) ---
    if ticker == "GC" and ratio < 0.05:
        return "MANUAL", f"Value {signal_val:.2f} looks like Silver, not Gold (~{actual_close:.0f})", None

    # --- VIX with other instrument value ---
    if ticker == "VIX" and signal_val > 200:
        return "MANUAL", f"Value {signal_val:.0f} way too high for VIX (~{actual_close:.1f})", None

    # --- Wheat: dropped leading digit ---
    if ticker == "ZW" and 0.3 < ratio < 0.75:
        corrected = signal_val + 1000
        check_ratio = corrected / actual_close
        if 0.7 < check_ratio < 1.4:
            return "AUTO-FIX", f"+1000 -> {corrected:.0f} (dropped leading '1,')", corrected

    # --- Generic: severe deviation ---
    if ratio > 10 or ratio < 0.1:
        return "MANUAL", f"Ratio {ratio:.2f}x -- severe, needs manual review", None
    elif ratio > 5 or ratio < 0.2:
        return "MANUAL", f"Ratio {ratio:.2f}x -- large deviation", None
    else:
        return "CHECK", f"Ratio {ratio:.2f}x -- moderate deviation", None


# ---------------------------------------------------------------------------
# Phase 5: Audit
# ---------------------------------------------------------------------------

def audit_signals(signals, closes):
    """Compare signal prices against actual market closes.

    Returns (flagged_list, stats_dict).
    """
    flagged = []
    stats = defaultdict(lambda: {"origin_checked": 0, "cancel_checked": 0,
                                  "origin_flagged": 0, "cancel_flagged": 0})

    for sig in signals:
        ticker = sig["ticker"]
        date_str = sig["date"]

        if ticker not in closes:
            continue

        close_series = closes[ticker]

        # --- Check origin_price ---
        if sig["origin_price"] is not None:
            actual, actual_date = find_close(close_series, date_str)
            if actual and actual > 0:
                stats[ticker]["origin_checked"] += 1
                ratio = sig["origin_price"] / actual

                if ratio > ORIGIN_FLAG_RATIO or ratio < (1.0 / ORIGIN_FLAG_RATIO):
                    severity, suggestion, corrected = classify_error(
                        ticker, "origin_price", sig["origin_price"], actual, ratio
                    )
                    flagged.append({
                        "id": sig["id"],
                        "ticker": ticker,
                        "date": date_str,
                        "field": "origin_price",
                        "signal_value": sig["origin_price"],
                        "actual_close": actual,
                        "actual_date": actual_date,
                        "ratio": ratio,
                        "severity": severity,
                        "suggestion": suggestion,
                        "corrected_value": corrected,
                        "raw_text": (sig["raw_text"] or "")[:200],
                        "email_date": sig["email_date"],
                        "email_subject": sig["email_subject"] or "",
                        "signal_type": sig["signal_type"],
                    })
                    stats[ticker]["origin_flagged"] += 1

        # --- Check cancel_level ---
        if sig["cancel_level"] is not None:
            actual, actual_date = find_close(close_series, date_str)
            if actual and actual > 0:
                stats[ticker]["cancel_checked"] += 1
                ratio = sig["cancel_level"] / actual

                if ratio > CANCEL_FLAG_RATIO or ratio < (1.0 / CANCEL_FLAG_RATIO):
                    severity, suggestion, corrected = classify_error(
                        ticker, "cancel_level", sig["cancel_level"], actual, ratio
                    )
                    flagged.append({
                        "id": sig["id"],
                        "ticker": ticker,
                        "date": date_str,
                        "field": "cancel_level",
                        "signal_value": sig["cancel_level"],
                        "actual_close": actual,
                        "actual_date": actual_date,
                        "ratio": ratio,
                        "severity": severity,
                        "suggestion": suggestion,
                        "corrected_value": corrected,
                        "raw_text": (sig["raw_text"] or "")[:200],
                        "email_date": sig["email_date"],
                        "email_subject": sig["email_subject"] or "",
                        "signal_type": sig["signal_type"],
                    })
                    stats[ticker]["cancel_flagged"] += 1

    return flagged, stats


# ---------------------------------------------------------------------------
# Phase 6: Report
# ---------------------------------------------------------------------------

def generate_report(flagged, stats, output_path=None):
    """Generate a readable report of flagged signals."""
    severity_order = {"AUTO-FIX": 0, "MANUAL": 1, "CHECK": 2}
    flagged.sort(key=lambda x: (severity_order.get(x["severity"], 9), x["ticker"], x["date"]))

    lines = []
    lines.append("=" * 110)
    lines.append("SIGNAL PRICE AUDIT REPORT")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("Cross-reference of signal prices vs. Yahoo Finance daily closes")
    lines.append("=" * 110)
    lines.append("")

    # --- Summary ---
    by_severity = defaultdict(list)
    for f in flagged:
        by_severity[f["severity"]].append(f)

    total_origin = sum(s["origin_checked"] for s in stats.values())
    total_cancel = sum(s["cancel_checked"] for s in stats.values())
    total_checked = total_origin + total_cancel

    lines.append("SUMMARY")
    lines.append("-" * 50)
    lines.append(f"  Origin prices checked:  {total_origin:,}")
    lines.append(f"  Cancel levels checked:  {total_cancel:,}")
    lines.append(f"  Total checks:           {total_checked:,}")
    lines.append(f"  Total flagged:          {len(flagged)}")
    lines.append(f"    AUTO-FIX:             {len(by_severity.get('AUTO-FIX', []))}")
    lines.append(f"    MANUAL review:        {len(by_severity.get('MANUAL', []))}")
    lines.append(f"    CROSS-REF:            {len(by_severity.get('CROSS-REF', []))}  (ETF/futures cross-attribution)")
    lines.append(f"    SPLIT-ADJUSTED:       {len(by_severity.get('SPLIT', []))}  (stock/ETF splits, not errors)")
    lines.append(f"    CHECK (borderline):   {len(by_severity.get('CHECK', []))}")
    lines.append("")

    # --- Per-ticker summary ---
    lines.append("PER-TICKER FLAGGED COUNT")
    lines.append("-" * 50)
    severity_cats = ["auto", "cross", "manual", "split", "check"]
    ticker_flags = defaultdict(lambda: {c: 0 for c in severity_cats})
    sev_map = {"AUTO-FIX": "auto", "CROSS-REF": "cross", "MANUAL": "manual",
               "SPLIT": "split", "CHECK": "check"}
    for f in flagged:
        cat = sev_map.get(f["severity"], "check")
        ticker_flags[f["ticker"]][cat] += 1

    for t in sorted(ticker_flags.keys()):
        counts = ticker_flags[t]
        total = sum(counts.values())
        parts = []
        if counts["auto"]:
            parts.append(f"{counts['auto']} auto-fix")
        if counts["cross"]:
            parts.append(f"{counts['cross']} cross-ref")
        if counts["manual"]:
            parts.append(f"{counts['manual']} manual")
        if counts["split"]:
            parts.append(f"{counts['split']} split-adj")
        if counts["check"]:
            parts.append(f"{counts['check']} check")
        lines.append(f"  {t:12s}:  {total:3d} flagged  ({', '.join(parts)})")
    lines.append("")

    # --- Detailed findings (skip SPLIT -- those are not errors) ---
    for severity in ["AUTO-FIX", "CROSS-REF", "MANUAL", "CHECK"]:
        items = by_severity.get(severity, [])
        if not items:
            continue

        lines.append("=" * 110)
        if severity == "AUTO-FIX":
            lines.append(f"  AUTO-FIX  ({len(items)} items) -- clear errors, can be corrected automatically")
        elif severity == "MANUAL":
            lines.append(f"  MANUAL REVIEW  ({len(items)} items) -- needs human review of original email")
        else:
            lines.append(f"  CHECK  ({len(items)} items) -- borderline, may be legitimate")
        lines.append("=" * 110)
        lines.append("")

        # Group by ticker for readability
        by_ticker = defaultdict(list)
        for item in items:
            by_ticker[item["ticker"]].append(item)

        for ticker in sorted(by_ticker.keys()):
            ticker_items = by_ticker[ticker]
            lines.append(f"--- {ticker} ({len(ticker_items)} items) ---")

            for item in ticker_items:
                lines.append(
                    f"  #{item['id']:6d}  {item['date']}  "
                    f"{item['field']:13s}  "
                    f"signal={item['signal_value']:>12.4f}  "
                    f"close={item['actual_close']:>12.4f}  "
                    f"ratio={item['ratio']:>8.3f}x"
                )
                lines.append(f"           {item['suggestion']}")
                if severity == "MANUAL":
                    lines.append(f"           Email: {item['email_date']}  {item['email_subject'][:80]}")
                    if item["raw_text"]:
                        lines.append(f"           Text:  {item['raw_text'][:120]}")
                lines.append("")

            lines.append("")

    report = "\n".join(lines)

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\nReport written to: {output_path}")

    return report


# ---------------------------------------------------------------------------
# Phase 7: Apply Fixes
# ---------------------------------------------------------------------------

def apply_fixes(conn, flagged):
    """Apply auto-corrections to the database."""
    auto_fixes = [f for f in flagged if f["severity"] == "AUTO-FIX" and f["corrected_value"] is not None]

    if not auto_fixes:
        print("No auto-fixable items found.")
        return 0

    print(f"\nApplying {len(auto_fixes)} auto-corrections...")

    applied = 0
    for item in auto_fixes:
        signal_id = item["id"]
        field = item["field"]
        old_val = item["signal_value"]
        new_val = item["corrected_value"]

        conn.execute(
            f"UPDATE signals SET {field} = ? WHERE id = ?",
            (new_val, signal_id)
        )
        print(f"  #{signal_id:6d} {item['ticker']:8s} {field:13s}: {old_val} -> {new_val}")
        applied += 1

    conn.commit()
    print(f"\nApplied {applied} corrections to {DB_PATH}")
    return applied


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Signal Price Audit")
    parser.add_argument("--fix", action="store_true",
                        help="Apply auto-corrections to the database")
    parser.add_argument("--ticker", type=str,
                        help="Audit a single ticker only")
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip Yahoo Finance download (use if already backfilled)")
    parser.add_argument("--store", action="store_true",
                        help="Store downloaded prices into price_history table")
    args = parser.parse_args()

    # Connect to DB
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Phase 1: Download historical prices
    if args.skip_download:
        print("Skipping download -- loading from price_history table...")
        # Load from DB
        rows = conn.execute("""
            SELECT ticker, date, close FROM price_history
            WHERE close IS NOT NULL
            ORDER BY ticker, date
        """).fetchall()
        closes = {}
        for row in rows:
            t = row["ticker"]
            if t not in closes:
                closes[t] = {}
            closes[t][row["date"]] = row["close"]
        # Convert to Series
        closes = {t: pd.Series(data) for t, data in closes.items()}
        print(f"Loaded {len(closes)} tickers from price_history")
    else:
        tickers_to_download = [args.ticker] if args.ticker else None
        closes = download_history(tickers_to_download)

        if args.store and closes:
            store_history_to_db(conn, closes)

    if not closes:
        print("ERROR: No price data available. Exiting.")
        conn.close()
        return

    # Phase 2: Load signals
    signals = load_signals(conn, args.ticker)
    print(f"\nLoaded {len(signals):,} signals with price data")

    # Phase 3-5: Audit
    flagged, stats = audit_signals(signals, closes)

    # Phase 6: Report
    output_path = os.path.join(os.path.dirname(DB_PATH), "signal_price_audit_report.txt")
    report = generate_report(flagged, stats, output_path)
    # Print to console (handle Windows encoding)
    try:
        print(report)
    except UnicodeEncodeError:
        print(report.encode("ascii", errors="replace").decode("ascii"))

    # Phase 7: Apply fixes
    if args.fix:
        apply_fixes(conn, flagged)

    conn.close()


if __name__ == "__main__":
    main()
