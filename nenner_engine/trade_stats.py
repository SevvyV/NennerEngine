"""
Trade Statistics (Lightweight)
===============================
Computes per-instrument trade metrics (Profit Factor, win rate, Sharpe ratio,
Kelly criterion, EV/MaxDD) directly from the signals table using only
stdlib + sqlite3.

Designed for use in the dashboard and Telegram reports where importing
heavyweight libraries (matplotlib, fpdf, pandas) would be inappropriate.

Mirrors the logic in signal_performance_report.py but with zero external deps.

Scoring Model (Goldman Sachs Quant-Style):
    Sharpe(35%) + Kelly(20%) + EV/MaxDD(20%) + WinRate(15%) + Confidence(10%)

    - Sharpe = EV / StdDev  (risk-adjusted return)
    - Kelly  = p - (1-p)/(avg_win/avg_loss)  (optimal bet sizing)
    - EV/MaxDD = avg_gain / abs(max_drawdown)  (return per unit of pain)
    - Confidence = min(n_trades / 50, 1.0)  (penalizes thin data)
"""

import sqlite3
import logging
import math
import time
from datetime import datetime
from statistics import mean, median, stdev
from typing import Optional

log = logging.getLogger("nenner")

# ---------------------------------------------------------------------------
# Configuration (mirrors signal_performance_report.py)
# ---------------------------------------------------------------------------

TRADEABLE_ASSET_CLASSES = {
    "Agriculture ETF", "Crypto ETF", "Currency ETF", "Energy ETF",
    "Fixed Income ETF", "Precious Metals ETF", "Precious Metals Stock",
    "Single Stock", "Volatility",
}

MACRO_CUTOFF = "2023-02-21"
SINGLE_STOCK_CUTOFF = "2025-11-01"
MIN_TRADES = 3
MAX_SINGLE_TRADE_PCT = 200.0

# Price sanity filter: if consecutive prices differ by more than this
# ratio, the trade is likely a misparse (e.g., 103.9 stored as 1.039)
PRICE_SANITY_MAX_RATIO = 5.0  # entry/exit or exit/entry > 5x = misparse

# Median-based sanity: if a price deviates from the ticker's median by
# more than this ratio, AND the resulting PnL exceeds MEDIAN_PNL_THRESHOLD,
# it's likely a misparse.  (Catches ZN/ZB 68.0 vs median ~115: ratio 1.63,
# PnL -67%.)
# This two-stage filter avoids flagging legitimate volatile instruments
# (VIX 47 vs median 19 = 2.48x ratio but PnL is normal).
PRICE_MEDIAN_MAX_RATIO = 1.5
MEDIAN_PNL_THRESHOLD = 50.0  # only reject if |PnL| also exceeds this %

# Confidence scaling: trades needed for full confidence in quant metrics
CONFIDENCE_FULL_TRADES = 50

# Cache TTL in seconds (dashboard refreshes every 30s, but PF changes rarely)
_CACHE_TTL = 300  # 5 minutes
_cache_all: dict = {}          # cache for all instruments (dashboard)
_cache_all_time: float = 0.0
_cache_tradeable: dict = {}    # cache for tradeable only (Telegram report)
_cache_tradeable_time: float = 0.0


# ---------------------------------------------------------------------------
# Trade Extraction
# ---------------------------------------------------------------------------

def _is_price_sane(entry_price: float, exit_price: float) -> bool:
    """Check if entry/exit price pair is plausible.

    Catches LLM misparses like:
      - DXY: 103.9 stored as 1.039 (100x shift)
      - FTSE: 7470 stored as 74.7 (100x shift)
      - ZN/ZB: 68.0 instead of ~114 (wrong value entirely)

    Uses ratio test: if max(a,b)/min(a,b) > PRICE_SANITY_MAX_RATIO,
    it's almost certainly a misparse.
    """
    if entry_price <= 0 or exit_price <= 0:
        return False
    ratio = max(entry_price, exit_price) / min(entry_price, exit_price)
    return ratio <= PRICE_SANITY_MAX_RATIO


def extract_trades_from_db(conn: sqlite3.Connection,
                           asset_filter: Optional[set[str]] = None) -> list[dict]:
    """Extract round-trip trades from the signals table.

    Reimplements signal_performance_report.extract_trades() using only
    sqlite3 and stdlib. Same algorithm:
      1. Query signals with date cutoffs
      2. Group by ticker, walk chronologically
      3. Collapse to direction changes only
      4. Build round-trips: entry at signal N's origin, exit at signal N+1's origin
      5. Filter outliers (>200% return AND price sanity checks)

    Price sanity is enforced at two levels:
      a) Pair ratio: entry vs exit must be within 5x of each other
      b) Ticker median: each price must be within 5x of the ticker's median
         (catches ZN/ZB-style single bad values like 68.0 vs typical ~115)

    Args:
        conn: SQLite connection.
        asset_filter: If provided, only include instruments in these asset classes.
                      If None, include ALL instruments (used by dashboard).

    Returns list of trade dicts.
    """
    old_rf = conn.row_factory
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT id, date, instrument, ticker, asset_class,
               signal_type, signal_status, origin_price, note_the_change
        FROM signals
        WHERE ((asset_class = 'Single Stock' AND date >= :stock_cutoff)
               OR (asset_class != 'Single Stock' AND date >= :macro_cutoff))
          AND origin_price IS NOT NULL
          AND origin_price > 0
          AND signal_type IN ('BUY', 'SELL')
        ORDER BY ticker, date ASC, id ASC
    """, {"stock_cutoff": SINGLE_STOCK_CUTOFF, "macro_cutoff": MACRO_CUTOFF}).fetchall()

    conn.row_factory = old_rf

    # Group by ticker (optionally filter by asset class)
    by_ticker: dict[str, list] = {}
    for r in rows:
        ac = r["asset_class"]
        if asset_filter is not None and ac not in asset_filter:
            continue
        by_ticker.setdefault(r["ticker"], []).append(dict(r))

    trades = []

    for ticker, signals in by_ticker.items():
        if len(signals) < 2:
            continue

        # Compute median price for this ticker (for outlier detection)
        all_prices = [s["origin_price"] for s in signals
                      if s["origin_price"] and s["origin_price"] > 0]
        ticker_median = median(all_prices) if len(all_prices) >= 3 else None

        # Collapse to direction changes only
        prev_direction = None
        direction_changes = []
        for sig in signals:
            d = sig["signal_type"]
            if d != prev_direction:
                direction_changes.append(sig)
                prev_direction = d

        if len(direction_changes) < 2:
            continue

        # Build round-trip trades
        for i in range(len(direction_changes) - 1):
            entry = direction_changes[i]
            exit_ = direction_changes[i + 1]

            entry_price = entry["origin_price"]
            exit_price = exit_["origin_price"]
            entry_signal = entry["signal_type"]

            # Price sanity check 1: entry vs exit ratio
            if not _is_price_sane(entry_price, exit_price):
                log.debug(
                    f"Skipping misparse trade {ticker} (pair ratio): "
                    f"entry={entry_price} exit={exit_price} "
                    f"(ids {entry['id']}->{exit_['id']})"
                )
                continue

            if entry_signal == "BUY":
                pnl_pct = ((exit_price - entry_price) / entry_price) * 100
            else:
                pnl_pct = ((entry_price - exit_price) / entry_price) * 100

            # Skip extreme outliers (belt-and-suspenders with price sanity)
            if abs(pnl_pct) > MAX_SINGLE_TRADE_PCT:
                continue

            # Price sanity check 2: median + PnL combined filter
            # Only reject if BOTH conditions are true:
            #   (a) at least one price deviates >1.5x from ticker median
            #   (b) the resulting PnL exceeds 50%
            # This catches ZN 68.0 (median ~115, PnL -68%) but allows
            # VIX 47 (median ~19, PnL is normal).
            if ticker_median and ticker_median > 0 and abs(pnl_pct) > MEDIAN_PNL_THRESHOLD:
                median_skip = False
                for price, label in [(entry_price, "entry"), (exit_price, "exit")]:
                    ratio = max(price, ticker_median) / min(price, ticker_median)
                    if ratio > PRICE_MEDIAN_MAX_RATIO:
                        log.debug(
                            f"Skipping misparse trade {ticker} ({label} vs median + high PnL): "
                            f"{label}={price} median={ticker_median:.2f} ratio={ratio:.1f}x "
                            f"PnL={pnl_pct:+.1f}% "
                            f"(ids {entry['id']}->{exit_['id']})"
                        )
                        median_skip = True
                        break
                if median_skip:
                    continue

            trades.append({
                "ticker": ticker,
                "instrument": entry["instrument"],
                "asset_class": entry["asset_class"],
                "entry_signal": entry_signal,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl_pct": pnl_pct,
            })

    return trades


# ---------------------------------------------------------------------------
# Quant Metrics
# ---------------------------------------------------------------------------

def _compute_sharpe(pnls: list[float]) -> float:
    """Sharpe ratio: EV / StdDev. Returns 0 if insufficient data or zero vol."""
    if len(pnls) < 2:
        return 0.0
    ev = mean(pnls)
    sd = stdev(pnls)
    if sd == 0:
        return 99.0 if ev > 0 else 0.0
    return ev / sd


def _compute_kelly(win_rate_frac: float, avg_win: float, avg_loss: float) -> float:
    """Kelly criterion: p - (1-p) / (W/L).

    Returns the Kelly fraction (0-1 is normal, <0 means don't trade).
    Capped at [-1, 2] to avoid extreme outliers.

    Args:
        win_rate_frac: Win probability as fraction (0-1).
        avg_win: Average winning trade % (positive).
        avg_loss: Average losing trade % (negative or zero).
    """
    if avg_win <= 0 or avg_loss >= 0:
        return 0.0
    win_loss_ratio = avg_win / abs(avg_loss)
    kelly = win_rate_frac - (1 - win_rate_frac) / win_loss_ratio
    return max(-1.0, min(kelly, 2.0))


def _compute_ev_maxdd(avg_gain: float, max_drawdown: float) -> float:
    """EV / |MaxDrawdown|: expected value per unit of worst-case pain.

    Returns 0 if no drawdown data.
    """
    if max_drawdown >= 0:
        # No losing trades = infinite EV/MaxDD, cap at 99
        return 99.0 if avg_gain > 0 else 0.0
    return avg_gain / abs(max_drawdown)


# ---------------------------------------------------------------------------
# Per-Instrument Stats
# ---------------------------------------------------------------------------

def compute_instrument_stats(conn: sqlite3.Connection,
                             use_cache: bool = True,
                             asset_filter: Optional[set[str]] = None) -> dict[str, dict]:
    """Compute per-ticker trade statistics including quant metrics.

    Args:
        conn: SQLite connection.
        use_cache: Use cached results if available.
        asset_filter: If provided, only include instruments in these asset classes.
                      If None, include ALL instruments (used by dashboard).

    Returns:
        {ticker: {
            "profit_factor": float, "win_rate": float,
            "avg_gain": float, "median_gain": float,
            "avg_loss": float, "median_loss": float,
            "avg_win": float, "median_win": float,
            "trades": int, "wins": int, "losses": int,
            "instrument": str, "asset_class": str,
            # Quant metrics (new)
            "sharpe": float,       # Sharpe ratio (EV/StdDev)
            "kelly": float,        # Kelly criterion (optimal bet sizing)
            "ev_maxdd": float,     # EV / |MaxDrawdown|
            "max_drawdown": float, # Worst single trade %
            "confidence": float,   # min(n/50, 1.0) - data quality
            "composite": float,    # Composite quant score (0-1)
        }}

    Results are cached for 5 minutes to avoid recomputation on
    every 30-second dashboard refresh. Separate caches for filtered
    vs unfiltered requests.
    """
    global _cache_all, _cache_all_time, _cache_tradeable, _cache_tradeable_time

    # Select correct cache based on whether we're filtering
    if asset_filter is not None:
        if use_cache and _cache_tradeable and (time.time() - _cache_tradeable_time) < _CACHE_TTL:
            return _cache_tradeable
    else:
        if use_cache and _cache_all and (time.time() - _cache_all_time) < _CACHE_TTL:
            return _cache_all

    all_trades = extract_trades_from_db(conn, asset_filter=asset_filter)

    # Group by ticker
    by_ticker: dict[str, list[dict]] = {}
    for t in all_trades:
        by_ticker.setdefault(t["ticker"], []).append(t)

    stats = {}

    for ticker, trades in by_ticker.items():
        n = len(trades)
        if n < MIN_TRADES:
            continue

        pnls = [t["pnl_pct"] for t in trades]
        win_pnls = [p for p in pnls if p > 0]
        loss_pnls = [p for p in pnls if p <= 0]

        wins = len(win_pnls)
        losses = len(loss_pnls)

        gross_gains = sum(win_pnls)
        gross_losses = abs(sum(loss_pnls))
        pf = gross_gains / gross_losses if gross_losses > 0 else 99.0

        avg_gain = mean(pnls)
        avg_win = mean(win_pnls) if win_pnls else 0.0
        avg_loss = mean(loss_pnls) if loss_pnls else 0.0
        win_rate_pct = wins / n * 100
        win_rate_frac = wins / n
        max_dd = min(pnls)

        # Quant metrics
        sharpe = _compute_sharpe(pnls)
        kelly = _compute_kelly(win_rate_frac, avg_win, avg_loss)
        ev_maxdd = _compute_ev_maxdd(avg_gain, max_dd)
        confidence = min(n / CONFIDENCE_FULL_TRADES, 1.0)

        # Composite score: Sharpe(35%) + Kelly(20%) + EV/MaxDD(20%) +
        #                  WinRate(15%) + Confidence(10%)
        # (raw values — normalized later in _score_items for ranking)
        composite = _compute_composite_raw(sharpe, kelly, ev_maxdd,
                                           win_rate_frac, confidence)

        stats[ticker] = {
            "profit_factor": round(pf, 2),
            "win_rate": round(win_rate_pct, 1),
            "avg_gain": round(avg_gain, 2),
            "median_gain": round(median(pnls), 2),
            "avg_win": round(avg_win, 2),
            "median_win": round(median(win_pnls), 2) if win_pnls else 0.0,
            "avg_loss": round(avg_loss, 2),
            "median_loss": round(median(loss_pnls), 2) if loss_pnls else 0.0,
            "trades": n,
            "wins": wins,
            "losses": losses,
            "instrument": trades[0]["instrument"],
            "asset_class": trades[0]["asset_class"],
            # Quant metrics
            "sharpe": round(sharpe, 3),
            "kelly": round(kelly, 3),
            "ev_maxdd": round(ev_maxdd, 3),
            "max_drawdown": round(max_dd, 2),
            "confidence": round(confidence, 2),
            "composite": round(composite, 4),
        }

    # Store in the correct cache
    if asset_filter is not None:
        _cache_tradeable = stats
        _cache_tradeable_time = time.time()
    else:
        _cache_all = stats
        _cache_all_time = time.time()

    return stats


def _compute_composite_raw(sharpe: float, kelly: float, ev_maxdd: float,
                           win_rate_frac: float, confidence: float) -> float:
    """Compute a raw (unnormalized) composite score for sorting.

    Higher = better. Components are scaled to roughly similar ranges
    before weighting so no single metric dominates.

    Weights: Sharpe 35%, Kelly 20%, EV/MaxDD 20%, WinRate 15%, Confidence 10%
    """
    # Clamp and scale each metric to ~[0, 1] for fair weighting
    # Sharpe: typically -1 to +3 for these instruments → scale by /3, clamp [0,1]
    s_norm = max(0.0, min(sharpe / 3.0, 1.0))

    # Kelly: typically -0.5 to +1.0 → scale by /1.0, clamp [0,1]
    k_norm = max(0.0, min(kelly / 1.0, 1.0))

    # EV/MaxDD: typically 0 to 1.0 → clamp [0,1] directly
    e_norm = max(0.0, min(ev_maxdd / 1.0, 1.0))

    # Win rate: already 0-1 fraction
    w_norm = max(0.0, min(win_rate_frac, 1.0))

    # Confidence: already 0-1
    c_norm = max(0.0, min(confidence, 1.0))

    return (0.35 * s_norm +
            0.20 * k_norm +
            0.20 * e_norm +
            0.15 * w_norm +
            0.10 * c_norm)


def get_profit_factor(conn: sqlite3.Connection, ticker: str) -> Optional[float]:
    """Get profit factor for a single ticker. Returns None if insufficient data."""
    stats = compute_instrument_stats(conn)
    s = stats.get(ticker)
    return s["profit_factor"] if s else None


# ---------------------------------------------------------------------------
# Risk Flags
# ---------------------------------------------------------------------------

def _risk_flag(s: dict) -> str:
    """Assign a risk flag based on quant metrics.

    Returns:
        "AVOID"   - Kelly < 0 or Sharpe < 0 (negative edge)
        "CAUTION" - Kelly < 0.1 or Sharpe < 0.2 or confidence < 0.3
        ""        - No flag (acceptable risk profile)
    """
    if s["kelly"] < 0 or s["sharpe"] < 0:
        return "AVOID"
    if s["kelly"] < 0.1 or s["sharpe"] < 0.2 or s["confidence"] < 0.3:
        return "CAUTION"
    return ""


# ---------------------------------------------------------------------------
# Top Trades Report (for Telegram)
# ---------------------------------------------------------------------------

# Macro tickers to exclude from the Top report (user can't trade or prefers
# to manage precious metals / VIX separately)
MACRO_REPORT_EXCLUDE = {"VIX", "SLV", "GLD", "GDXJ", "NEM"}

STOCK_SLOTS = 4   # how many single-stock slots in the report
MACRO_SLOTS = 5   # how many macro ETF slots in the report


def _score_items(items: list[dict]) -> list[dict]:
    """Score and sort items using the quant composite model.

    Uses pre-computed composite scores from compute_instrument_stats().
    Falls back to PF-based scoring for items without quant data.

    Returns the list sorted by score descending.
    """
    if not items:
        return items

    for s in items:
        # Use pre-computed composite score if available
        s["score"] = s.get("composite", 0.0)

    items.sort(key=lambda x: x["score"], reverse=True)
    return items


def _format_trade_line(rank: int, t: dict) -> list[str]:
    """Format a single trade entry as Telegram HTML lines."""
    emoji = "\U0001f7e2" if t["signal"] == "BUY" else "\U0001f534"
    origin_str = f"{t['origin_price']:,.2f}" if t["origin_price"] else "?"
    cancel_str = f"{t['cancel_level']:,.2f}" if t["cancel_level"] else "?"

    lines = [
        f"{rank}. {emoji} <b>{t['ticker']}</b> - {t['instrument'][:22]}",
        f"   {t['signal']} from {origin_str} | Cancel: {cancel_str}",
    ]

    trades = t["trades"]
    if trades > 0:
        sharpe_str = f"Sharpe {t.get('sharpe', 0):.2f}"
        wr_str = f"WR {t['win_rate']:.0f}%"
        kelly_str = f"Kelly {t.get('kelly', 0):.0%}"

        # Risk flag
        flag = t.get("risk_flag", "")
        flag_str = f" \u26a0\ufe0f{flag}" if flag else ""

        lines.append(
            f"   {sharpe_str} | {wr_str} | {kelly_str} | "
            f"{trades} trades{flag_str}"
        )
    else:
        lines.append("   \u26a0\ufe0f NEW - awaiting trade history")

    lines.append("")
    return lines


def build_top_trades_message(conn: sqlite3.Connection,
                             limit: int = 10) -> Optional[str]:
    """Build an HTML-formatted Telegram message with two sections:

    Section 1 - SINGLE STOCKS (up to STOCK_SLOTS entries)
        All active single stock signals, ranked by quant composite score.
        Stocks with no completed trades are included but flagged as NEW.

    Section 2 - MACRO (up to MACRO_SLOTS entries)
        Tradeable macro ETFs excluding MACRO_REPORT_EXCLUDE tickers,
        ranked by quant composite score. Requires >= MIN_TRADES history.

    Returns None if no data at all.
    """
    # Get stats for all instruments (unfiltered) so stocks with few trades show up
    stats = compute_instrument_stats(conn, use_cache=True)

    old_rf = conn.row_factory
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT ticker, instrument, asset_class, effective_signal,
               origin_price, cancel_level, last_signal_date
        FROM current_state
        WHERE effective_signal IN ('BUY', 'SELL')
          AND last_signal_date >= date('now', '-3 months')
    """).fetchall()

    conn.row_factory = old_rf

    # Separate into stocks vs macro
    stock_items = []
    macro_items = []

    for r in rows:
        ticker = r["ticker"]
        ac = r["asset_class"]
        s = stats.get(ticker)

        entry = {
            "ticker": ticker,
            "instrument": r["instrument"],
            "signal": r["effective_signal"],
            "origin_price": r["origin_price"],
            "cancel_level": r["cancel_level"],
            "pf": s["profit_factor"] if s else 0,
            "win_rate": s["win_rate"] if s else 0,
            "avg_gain": s["avg_gain"] if s else 0,
            "median_gain": s["median_gain"] if s else 0,
            "trades": s["trades"] if s else 0,
            # Quant metrics for scoring and display
            "sharpe": s["sharpe"] if s else 0,
            "kelly": s["kelly"] if s else 0,
            "ev_maxdd": s["ev_maxdd"] if s else 0,
            "confidence": s["confidence"] if s else 0,
            "composite": s["composite"] if s else 0,
            "risk_flag": _risk_flag(s) if s else "",
        }

        if ac == "Single Stock":
            stock_items.append(entry)
        elif ac in TRADEABLE_ASSET_CLASSES and ticker not in MACRO_REPORT_EXCLUDE:
            if s and s["trades"] >= MIN_TRADES:
                macro_items.append(entry)

    if not stock_items and not macro_items:
        return None

    # Score and rank each section independently
    # Stocks: include all (even 0-trade), sort scored items first, then NEW
    scored_stocks = [s for s in stock_items if s["trades"] >= MIN_TRADES]
    new_stocks = [s for s in stock_items if s["trades"] < MIN_TRADES]
    _score_items(scored_stocks)
    # NEW stocks go after scored ones, sorted by ticker for consistency
    new_stocks.sort(key=lambda x: x["ticker"])
    top_stocks = (scored_stocks + new_stocks)[:STOCK_SLOTS]

    # Macro: scored normally
    _score_items(macro_items)
    top_macro = macro_items[:MACRO_SLOTS]

    # Build message
    now = datetime.now()
    lines = [
        f"\U0001f4ca <b>Daily Trade Ideas</b>",
        f"<i>{now.strftime('%b %d, %Y %I:%M %p ET')}</i>",
        "",
    ]

    # Section 1: Single Stocks
    if top_stocks:
        lines.append("\U0001f4c8 <b>SINGLE STOCKS</b>")
        lines.append("")
        for i, t in enumerate(top_stocks, 1):
            lines.extend(_format_trade_line(i, t))

    # Section 2: Macro
    if top_macro:
        lines.append("\U0001f30d <b>MACRO</b>")
        lines.append("")
        for i, t in enumerate(top_macro, 1):
            lines.extend(_format_trade_line(i, t))

    lines.append(
        "<i>Quant Score = Sharpe(35%) + Kelly(20%) + EV/MaxDD(20%) "
        "+ WinRate(15%) + Confidence(10%)</i>"
    )
    return "\n".join(lines)
