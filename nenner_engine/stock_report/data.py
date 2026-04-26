"""Stock report data gathering — DB queries and dict assembly.

All functions in this module read from the SQLite connection and return
plain dicts. No HTML, no LLM, no email — those live in sibling modules.
"""

import logging
import math
import sqlite3
from datetime import datetime, date
from typing import Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FOCUS_STOCKS = ["AAPL", "BAC", "GOOG", "MSFT", "NVDA", "TSLA", "ES", "NQ"]

STOCK_NAMES = {
    "AAPL": "Apple Inc.",
    "BAC": "Bank of America",
    "GOOG": "Alphabet Inc.",
    "MSFT": "Microsoft Corp.",
    "NVDA": "NVIDIA Corp.",
    "TSLA": "Tesla Inc.",
    "ES": "S&P 500 Futures",
    "NQ": "Nasdaq 100 Futures",
}

# Display names for tickers in the report (overrides raw ticker where shown)
DISPLAY_TICKER = {
    "ES": "S&P 500",
    "NQ": "QQQ",
}

# Danger/watch thresholds for cancel distance (also consumed by html.py)
CANCEL_DANGER_PCT = 1.0
CANCEL_WATCH_PCT = 2.5


# ---------------------------------------------------------------------------
# Data Gathering
# ---------------------------------------------------------------------------

def _get_cancel_trajectory(conn: sqlite3.Connection, ticker: str,
                           days: int = 30) -> list[float]:
    """Extract unique cancel level progression for a ticker over N days.

    Returns a list of cancel levels in chronological order, deduped.
    """
    rows = conn.execute(
        "SELECT cancel_level FROM signals "
        "WHERE ticker = ? AND date >= date('now', ?) "
        "AND cancel_level IS NOT NULL "
        "ORDER BY date ASC, id ASC",
        (ticker, f'-{days} days')
    ).fetchall()

    trajectory = []
    prev = None
    for r in rows:
        lvl = r["cancel_level"]
        if lvl != prev:
            trajectory.append(lvl)
            prev = lvl
    return trajectory


def _count_ntc(conn: sqlite3.Connection, ticker: str, days: int = 30) -> int:
    """Count note_the_change signals in the last N days."""
    row = conn.execute(
        "SELECT COUNT(*) FROM signals "
        "WHERE ticker = ? AND note_the_change = 1 "
        "AND date >= date('now', ?)",
        (ticker, f'-{days} days')
    ).fetchone()
    return row[0] if row else 0


def _get_latest_target(conn: sqlite3.Connection, ticker: str) -> Optional[dict]:
    """Get the most recent price target for a ticker."""
    row = conn.execute(
        "WITH recent_emails AS ("
        "  SELECT id FROM emails ORDER BY date_sent DESC, id DESC LIMIT 5"
        ") "
        "SELECT pt.target_price, pt.direction, pt.condition "
        "FROM price_targets pt "
        "WHERE pt.ticker = ? "
        "  AND EXISTS ("
        "    SELECT 1 FROM price_targets pt2"
        "    WHERE pt2.ticker = pt.ticker"
        "      AND pt2.email_id IN (SELECT id FROM recent_emails)"
        "  ) "
        "ORDER BY pt.date DESC, pt.id DESC LIMIT 1",
        (ticker,)
    ).fetchone()
    return dict(row) if row else None


def _get_cycles(conn: sqlite3.Connection, ticker: str) -> list[dict]:
    """Get latest cycle data for a ticker (up to 6 entries)."""
    rows = conn.execute(
        "SELECT timeframe, direction, until_description FROM cycles "
        "WHERE ticker = ? ORDER BY date DESC, id DESC LIMIT 6",
        (ticker,)
    ).fetchall()
    return [dict(r) for r in rows]


def _assess_cycle_alignment(signal: str, cycles: list[dict]) -> str:
    """Determine if cycles align with the current signal direction.

    Returns: 'ALIGNED', 'CONFLICTING', 'MIXED', or 'NO DATA'
    """
    if not cycles:
        return "NO DATA"

    # Map signal to expected cycle direction
    expected = "DOWN" if signal == "SELL" else "UP"

    aligned = 0
    conflicting = 0
    for c in cycles:
        d = (c.get("direction") or "").upper()
        if d == expected:
            aligned += 1
        elif d and d != expected:
            conflicting += 1

    total = aligned + conflicting
    if total == 0:
        return "NO DATA"
    ratio = aligned / total
    if ratio >= 0.7:
        return "ALIGNED"
    if ratio <= 0.3:
        return "CONFLICTING"
    return "MIXED"


def _compute_reward_risk(signal: str, price: float,
                         cancel: float, target: float) -> Optional[float]:
    """Compute reward-to-risk ratio from current price.

    Returns R:R as a float (e.g. 2.5 means 2.5:1), or None if not computable.
    """
    if not all([price, cancel, target]):
        return None

    if signal == "SELL":
        reward = price - target   # positive if target below price
        risk = cancel - price     # positive if cancel above price
    else:  # BUY
        reward = target - price   # positive if target above price
        risk = price - cancel     # positive if cancel below price

    if reward <= 0:
        return 0.0  # Already past target
    if risk <= 0:
        return None  # Cancel on same side as target — can't compute
    return round(reward / risk, 1)


def _get_target_progression(conn: sqlite3.Connection, ticker: str,
                            days: int = 60) -> list[dict]:
    """Get the target price progression for a ticker over N days.

    Returns chronological list of distinct targets with context:
      {date, target_price, direction, condition, reached}

    This captures Nenner's staircase pattern: target reached → new target set.
    """
    rows = conn.execute(
        "WITH recent_emails AS ("
        "  SELECT id FROM emails ORDER BY date_sent DESC, id DESC LIMIT 5"
        ") "
        "SELECT pt.date, pt.target_price, pt.direction, pt.condition, pt.raw_text "
        "FROM price_targets pt "
        "WHERE pt.ticker = ? AND pt.date >= date('now', ?) "
        "AND pt.target_price IS NOT NULL "
        "AND EXISTS ("
        "  SELECT 1 FROM price_targets pt2"
        "  WHERE pt2.ticker = pt.ticker"
        "    AND pt2.email_id IN (SELECT id FROM recent_emails)"
        ") "
        "ORDER BY pt.date ASC, pt.id ASC",
        (ticker, f'-{days} days')
    ).fetchall()

    # Dedupe to unique (date, target_price) pairs
    seen = set()
    progression = []
    for r in rows:
        key = (r["date"], r["target_price"])
        if key not in seen:
            seen.add(key)
            reached = ("reached" in (r["condition"] or "").lower()
                       or "reached" in (r["raw_text"] or "").lower())
            progression.append({
                "date": r["date"],
                "target_price": r["target_price"],
                "direction": r["direction"],
                "reached": reached,
            })
    return progression


def _detect_target_staircase(progression: list[dict]) -> dict:
    """Analyze target progression for staircase patterns.

    Returns:
        {
            "targets_reached": int,   # how many targets were hit
            "latest_target": float,   # current target
            "previous_target": float, # last reached target (if any)
            "is_staircasing": bool,   # target → reached → new target pattern
            "staircase_direction": str, # "LOWER" or "HIGHER" or None
        }
    """
    result = {
        "targets_reached": 0,
        "latest_target": None,
        "previous_target": None,
        "is_staircasing": False,
        "staircase_direction": None,
    }

    if not progression:
        return result

    result["latest_target"] = progression[-1]["target_price"]

    reached_targets = [p for p in progression if p["reached"]]
    result["targets_reached"] = len(reached_targets)

    if reached_targets:
        result["previous_target"] = reached_targets[-1]["target_price"]

        # Find non-reached targets set AFTER the last reached target
        last_reached_date = reached_targets[-1]["date"]
        newer_targets = [
            p for p in progression
            if p["date"] >= last_reached_date and not p["reached"]
        ]

        if newer_targets:
            result["is_staircasing"] = True
            new_tp = newer_targets[-1]["target_price"]
            old_tp = reached_targets[-1]["target_price"]
            if new_tp < old_tp:
                result["staircase_direction"] = "LOWER"
            elif new_tp > old_tp:
                result["staircase_direction"] = "HIGHER"

    return result


def _get_signal_history(conn: sqlite3.Connection, ticker: str,
                        limit: int = 8) -> list[dict]:
    """Get recent signal history for a ticker."""
    rows = conn.execute(
        "SELECT date, signal_type, signal_status, origin_price, "
        "cancel_level, note_the_change FROM signals "
        "WHERE ticker = ? ORDER BY date DESC, id DESC LIMIT ?",
        (ticker, limit)
    ).fetchall()
    return [dict(r) for r in rows]


def _detect_inflection_flags(stock: dict) -> list[str]:
    """Determine which inflection flags apply to a stock.

    Returns list of flag strings like 'CANCEL_DANGER', 'REVERSAL', etc.
    """
    flags = []
    cancel_dist = stock.get("cancel_dist_pct")
    if cancel_dist is not None:
        if abs(cancel_dist) < CANCEL_DANGER_PCT:
            flags.append("CANCEL_DANGER")
        elif abs(cancel_dist) < CANCEL_WATCH_PCT:
            flags.append("CANCEL_WATCH")

    if stock.get("implied_reversal"):
        flags.append("REVERSAL")

    ntc = stock.get("ntc_count_30d", 0)
    if ntc >= 4:
        flags.append("HIGH_CHURN")
    elif ntc >= 3:
        flags.append("CHURN")

    rr = stock.get("reward_risk")
    if rr is not None and rr < 1.0:
        flags.append("LOW_RR")

    target = stock.get("target_price")
    price = stock.get("price")
    if target and price:
        target_dist = abs(target - price) / price * 100
        if target_dist < 1.0:
            flags.append("AT_TARGET")

    risk_flag = stock.get("risk_flag", "")
    if risk_flag == "AVOID":
        flags.append("AVOID")

    # Target staircase — previous target reached, new target set further out
    staircase = stock.get("target_staircase", {})
    if staircase.get("is_staircasing") and staircase.get("targets_reached", 0) >= 1:
        flags.append("TARGET_REACHED")

    # Trade aging — current trade is approaching or exceeding average duration
    age_ratio = stock.get("trade_age_ratio")
    if age_ratio is not None and age_ratio >= 0.85:
        flags.append("TRADE_AGING")

    return flags


def gather_report_data(conn: sqlite3.Connection) -> list[dict]:
    """Gather all data needed for the stock report.

    Returns a list of enriched dicts, one per focus stock.
    """
    from ..prices import get_current_prices, fetch_yfinance_daily
    from ..trade_stats import compute_instrument_stats, _risk_flag

    # Fetch fresh prices for focus stocks
    try:
        fetch_yfinance_daily(conn, tickers=FOCUS_STOCKS, period="5d")
    except Exception as e:
        log.warning(f"Stock report: yfinance fetch failed: {e}")

    prices = get_current_prices(conn, FOCUS_STOCKS, try_t1=True)
    stats = compute_instrument_stats(conn, use_cache=False)

    stocks_data = []

    for ticker in FOCUS_STOCKS:
        state = conn.execute(
            "SELECT ticker, instrument, asset_class, effective_signal, "
            "origin_price, cancel_direction, cancel_level, "
            "trigger_level, implied_reversal, last_signal_date "
            "FROM current_state WHERE ticker = ?",
            (ticker,)
        ).fetchone()

        if not state:
            continue

        signal = state["effective_signal"]
        origin = state["origin_price"]
        cancel = state["cancel_level"]
        price_info = prices.get(ticker, {})
        price = price_info.get("price")
        price_source = price_info.get("source", "")
        price_as_of = price_info.get("as_of", "")

        # P/L (guard against zero / NaN / Inf in either input)
        pnl_pct = None
        if (origin and price
                and math.isfinite(origin) and math.isfinite(price)
                and origin > 0):
            if signal == "SELL":
                pnl_pct = (origin - price) / origin * 100
            else:
                pnl_pct = (price - origin) / origin * 100

        # Cancel distance
        cancel_dist_pct = None
        if (cancel and price
                and math.isfinite(cancel) and math.isfinite(price)
                and price > 0):
            cancel_dist_pct = (cancel - price) / price * 100

        # Target
        target_info = _get_latest_target(conn, ticker)
        target_price = target_info["target_price"] if target_info else None
        target_direction = target_info.get("direction") if target_info else None
        target_condition = target_info.get("condition") if target_info else None

        # Target distance
        target_dist_pct = None
        if (target_price and price
                and math.isfinite(target_price) and math.isfinite(price)
                and price > 0):
            target_dist_pct = abs(target_price - price) / price * 100

        # R:R
        reward_risk = _compute_reward_risk(signal, price, cancel, target_price)

        # Cancel trajectory & NTC count
        cancel_trajectory = _get_cancel_trajectory(conn, ticker, days=30)
        ntc_count = _count_ntc(conn, ticker, days=30)

        # Cycles
        cycles = _get_cycles(conn, ticker)
        cycle_alignment = _assess_cycle_alignment(signal, cycles)

        # Trade stats
        ticker_stats = stats.get(ticker)
        risk_flag = _risk_flag(ticker_stats) if ticker_stats else ""

        # Trade age (days since signal was given)
        trade_age_days = None
        if state["last_signal_date"]:
            try:
                sig_date = datetime.strptime(state["last_signal_date"], "%Y-%m-%d").date()
                trade_age_days = (date.today() - sig_date).days
            except (ValueError, TypeError):
                pass

        # Average trade duration for this instrument
        avg_duration = ticker_stats.get("avg_duration") if ticker_stats else None
        median_duration = ticker_stats.get("median_duration") if ticker_stats else None

        # Trade age ratio: how far through the average trade duration are we?
        trade_age_ratio = None
        if trade_age_days is not None and avg_duration and avg_duration > 0:
            trade_age_ratio = round(trade_age_days / avg_duration, 2)

        # Signal history
        signal_history = _get_signal_history(conn, ticker)

        # Target progression (staircase detection)
        target_progression = _get_target_progression(conn, ticker, days=60)
        target_staircase = _detect_target_staircase(target_progression)

        stock = {
            "ticker": ticker,
            "display_ticker": DISPLAY_TICKER.get(ticker, ticker),
            "name": STOCK_NAMES.get(ticker, ticker),
            "instrument": state["instrument"],
            "signal": signal,
            "origin_price": origin,
            "cancel_level": cancel,
            "cancel_direction": state["cancel_direction"],
            "implied_reversal": bool(state["implied_reversal"]),
            "last_signal_date": state["last_signal_date"],
            "price": price,
            "price_source": price_source,
            "price_as_of": price_as_of,
            "pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
            "cancel_dist_pct": round(cancel_dist_pct, 2) if cancel_dist_pct is not None else None,
            "target_price": target_price,
            "target_direction": target_direction,
            "target_condition": target_condition,
            "target_dist_pct": round(target_dist_pct, 2) if target_dist_pct is not None else None,
            "reward_risk": reward_risk,
            "cancel_trajectory": cancel_trajectory,
            "ntc_count_30d": ntc_count,
            "cycles": cycles,
            "cycle_alignment": cycle_alignment,
            "trade_stats": ticker_stats,
            "risk_flag": risk_flag,
            "signal_history": signal_history,
            "trade_age_days": trade_age_days,
            "avg_duration": avg_duration,
            "median_duration": median_duration,
            "trade_age_ratio": trade_age_ratio,
            "target_progression": target_progression,
            "target_staircase": target_staircase,
        }
        stock["inflection_flags"] = _detect_inflection_flags(stock)
        stocks_data.append(stock)

    return stocks_data
