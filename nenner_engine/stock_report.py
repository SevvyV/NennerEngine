"""
Stanley's Daily Stock Report
==============================
Generates and emails a comprehensive daily report for focus stocks
(AAPL, BAC, GOOG, MSFT, NVDA, TSLA) and equity index futures (ES, NQ, YM).

Sections:
  1. Portfolio Heat Map — quick-scan table with P/L, cancel distance, alerts
  2. Inflection Alerts — stocks at/through cancel levels, fresh reversals, churn
  3. Stock-by-Stock Detail — full context per instrument
  4. Exit Timing Framework — systematic ranking by exit urgency
  5. Stanley's Take — LLM-generated Druckenmiller-lens commentary

Email delivery uses the same Gmail credentials as the IMAP client (SMTP).
"""

import logging
import os
import sqlite3
import time
from datetime import datetime, date, timedelta
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

from .config import LLM_MODEL, LLM_MAX_TOKENS_REPORT, LLM_RETRY_ATTEMPTS

# Danger/watch thresholds for cancel distance
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
    from .prices import get_current_prices, fetch_yfinance_daily
    from .trade_stats import compute_instrument_stats, _risk_flag

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

        # P/L
        pnl_pct = None
        if origin and price and origin != 0:
            if signal == "SELL":
                pnl_pct = (origin - price) / origin * 100
            else:
                pnl_pct = (price - origin) / origin * 100

        # Cancel distance
        cancel_dist_pct = None
        if cancel and price and price != 0:
            cancel_dist_pct = (cancel - price) / price * 100

        # Target
        target_info = _get_latest_target(conn, ticker)
        target_price = target_info["target_price"] if target_info else None
        target_direction = target_info.get("direction") if target_info else None
        target_condition = target_info.get("condition") if target_info else None

        # Target distance
        target_dist_pct = None
        if target_price and price and price != 0:
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


# ---------------------------------------------------------------------------
# Email Infrastructure — delegated to postmaster
# ---------------------------------------------------------------------------

from .postmaster import send_email  # noqa: F401 — re-exported for backwards compat

# ---------------------------------------------------------------------------
# HTML Builders — Styles (canonical palette from postmaster)
# ---------------------------------------------------------------------------

from .postmaster import (  # noqa: E402
    FONT as _FONT,
    CLR_BG as _CLR_BG,
    CLR_WHITE as _CLR_WHITE,
    CLR_HEADER as _CLR_HEADER,
    CLR_TEXT as _CLR_TEXT,
    CLR_MUTED as _CLR_MUTED,
    CLR_BORDER as _CLR_BORDER,
    CLR_GREEN as _CLR_GREEN,
    CLR_RED as _CLR_RED,
    CLR_AMBER as _CLR_AMBER,
    CLR_PURPLE as _CLR_PURPLE,
    CLR_BLUE as _CLR_BLUE,
    CLR_LIGHT_RED as _CLR_LIGHT_RED,
    CLR_LIGHT_AMBER as _CLR_LIGHT_AMBER,
    CLR_LIGHT_PURPLE as _CLR_LIGHT_PURPLE,
    CLR_LIGHT_GREEN as _CLR_LIGHT_GREEN,
    CLR_ROW_ALT as _CLR_ROW_ALT,
    wrap_document as _wrap_document,
)


def _pnl_color(pnl: Optional[float]) -> str:
    if pnl is None:
        return _CLR_MUTED
    return _CLR_GREEN if pnl >= 0 else _CLR_RED


def _signal_color(signal: str) -> str:
    return _CLR_RED if signal == "SELL" else _CLR_GREEN


def _cancel_dist_color(dist: Optional[float]) -> str:
    if dist is None:
        return _CLR_MUTED
    if abs(dist) < CANCEL_DANGER_PCT:
        return _CLR_RED
    if abs(dist) < CANCEL_WATCH_PCT:
        return _CLR_AMBER
    return _CLR_TEXT


def _ntc_bars(count: int) -> str:
    """Generate tightening bars (more = more tightening)."""
    if count == 0:
        return "&mdash;"
    bars = "\u25bc" * min(count, 8)
    clr = _CLR_RED if count >= 4 else (_CLR_AMBER if count >= 3 else _CLR_MUTED)
    return f'<span style="color:{clr};">{bars}</span>'


def _fmt_price(val: Optional[float]) -> str:
    if val is None:
        return "&mdash;"
    return f"{val:,.2f}"


def _fmt_pct(val: Optional[float], plus: bool = True) -> str:
    if val is None:
        return "&mdash;"
    sign = "+" if plus and val > 0 else ""
    return f"{sign}{val:.1f}%"


# ---------------------------------------------------------------------------
# HTML Builders — Sections
# ---------------------------------------------------------------------------

def _build_header_html(stocks_data: list[dict]) -> str:
    now = datetime.now()
    sell_count = sum(1 for s in stocks_data if s["signal"] == "SELL")
    buy_count = len(stocks_data) - sell_count

    alerts = [s["display_ticker"] for s in stocks_data if s.get("inflection_flags")]
    alert_summary = ""
    if alerts:
        alert_summary = (
            f'<span style="color:{_CLR_AMBER};">'
            f'\u26a0 {", ".join(alerts[:3])}</span>'
        )

    return f'''
    <tr><td style="background-color:{_CLR_HEADER}; padding:28px 32px;">
      <h1 style="margin:0; font-size:22px; font-weight:700;
                 color:{_CLR_WHITE}; font-family:{_FONT};">
        Stanley's Daily Stock Report
      </h1>
      <p style="margin:6px 0 0; font-size:14px; color:#94a3b8; font-family:{_FONT};">
        {now.strftime("%A, %B %d, %Y")}
        &nbsp;&bull;&nbsp; {sell_count} SELL &middot; {buy_count} BUY
        {("&nbsp;&bull;&nbsp;" + alert_summary) if alert_summary else ""}
      </p>
    </td></tr>'''


def _target_color(stock: dict) -> str:
    """Color the target cell — amber when close, green when AT_TARGET."""
    flags = stock.get("inflection_flags", [])
    if "AT_TARGET" in flags:
        return _CLR_GREEN
    dist = stock.get("target_dist_pct")
    if dist is not None and dist < 3.0:
        return _CLR_AMBER
    return _CLR_TEXT


def _build_heat_map_html(stocks_data: list[dict]) -> str:
    th_style = (f"text-align:left; padding:8px 10px; font-size:11px; "
                f"font-weight:600; color:{_CLR_MUTED}; text-transform:uppercase; "
                f"letter-spacing:0.5px; border-bottom:2px solid {_CLR_BORDER};")
    th_r = th_style.replace("text-align:left", "text-align:right")
    th_c = th_style.replace("text-align:left", "text-align:center")

    rows_html = ""
    for i, s in enumerate(stocks_data):
        bg = _CLR_ROW_ALT if i % 2 == 1 else _CLR_WHITE
        td = f"padding:10px 8px; font-size:13px; border-bottom:1px solid {_CLR_BORDER};"
        td_r = td + " text-align:right;"
        td_c = td + " text-align:center;"

        # Alert badge
        flags = s.get("inflection_flags", [])
        alert_html = ""
        if "CANCEL_DANGER" in flags:
            alert_html = f'<span style="color:{_CLR_RED}; font-weight:600; font-size:11px;">\u26a0 DANGER</span>'
        elif "REVERSAL" in flags:
            alert_html = f'<span style="color:{_CLR_PURPLE}; font-weight:600; font-size:11px;">\U0001f504 REVERSAL</span>'
        elif "AT_TARGET" in flags:
            alert_html = f'<span style="color:{_CLR_GREEN}; font-weight:600; font-size:11px;">\U0001f3af TARGET</span>'
        elif "TRADE_AGING" in flags:
            alert_html = f'<span style="color:{_CLR_AMBER}; font-weight:600; font-size:11px;">\u23f3 AGING</span>'
        elif "CANCEL_WATCH" in flags:
            alert_html = f'<span style="color:{_CLR_AMBER}; font-weight:600; font-size:11px;">\u26a0 WATCH</span>'

        implied_star = "*" if s["implied_reversal"] else ""
        signal_str = f'{s["signal"]}{implied_star}'

        # Target with direction arrow
        target_str = _fmt_price(s["target_price"])
        if s.get("target_price"):
            arrow = "\u2193" if s.get("target_direction") == "DOWNSIDE" else "\u2191"
            target_str = f'{arrow}{_fmt_price(s["target_price"])}'

        # Trade age display: "day X / avg Y"
        age_days = s.get("trade_age_days")
        avg_dur = s.get("avg_duration")
        age_ratio = s.get("trade_age_ratio")
        if age_days is not None and avg_dur:
            age_clr = _CLR_RED if age_ratio and age_ratio >= 1.0 else (
                _CLR_AMBER if age_ratio and age_ratio >= 0.85 else _CLR_TEXT)
            age_str = f'<span style="color:{age_clr};">{age_days}/{avg_dur:.0f}</span>'
        elif age_days is not None:
            age_str = f'{age_days}'
        else:
            age_str = "&mdash;"

        rows_html += f'''
        <tr style="background-color:{bg};">
          <td style="{td} font-weight:600;">{s["display_ticker"]}</td>
          <td style="{td_c} color:{_signal_color(s['signal'])}; font-weight:600;">
            {signal_str}
          </td>
          <td style="{td_r} color:{_CLR_MUTED};">{_fmt_price(s["origin_price"])}</td>
          <td style="{td_r} font-weight:600;">{_fmt_price(s["price"])}</td>
          <td style="{td_r} color:{_target_color(s)};">{target_str}</td>
          <td style="{td_r} color:{_pnl_color(s['pnl_pct'])}; font-weight:600;">
            {_fmt_pct(s["pnl_pct"])}
          </td>
          <td style="{td_r} color:{_cancel_dist_color(s['cancel_dist_pct'])}; font-weight:600;">
            {_fmt_price(s["cancel_level"])}
          </td>
          <td style="{td_r} color:{_cancel_dist_color(s['cancel_dist_pct'])}; font-weight:600;">
            {_fmt_pct(s["cancel_dist_pct"])}
          </td>
          <td style="{td_c}">{age_str}</td>
          <td style="{td_c}">{_ntc_bars(s["ntc_count_30d"])}</td>
          <td style="{td_c}">{alert_html}</td>
        </tr>'''

    return f'''
    <tr><td style="padding:24px 32px 16px;">
      <h2 style="margin:0 0 12px; font-size:16px; color:{_CLR_TEXT};
                 font-family:{_FONT};">
        \U0001f4ca Portfolio Heat Map
      </h2>
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
             style="border-collapse:collapse;">
        <thead><tr>
          <th style="{th_style}">Ticker</th>
          <th style="{th_c}">Signal</th>
          <th style="{th_r}">Entry</th>
          <th style="{th_r}">Price</th>
          <th style="{th_r}">Target</th>
          <th style="{th_r}">P/L%</th>
          <th style="{th_r}">Cancel</th>
          <th style="{th_r}">Cancel%</th>
          <th style="{th_c}">Day</th>
          <th style="{th_c}">NTC</th>
          <th style="{th_c}">Alert</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
      <table role="presentation" cellpadding="0" cellspacing="0"
             style="margin:10px 0 0; font-size:11px; color:{_CLR_MUTED};
                    line-height:1.7; font-family:{_FONT};">
        <tr><td style="padding-right:8px; white-space:nowrap; font-weight:600;">Entry</td>
            <td>Price at which the current signal was given (signal origin level)</td></tr>
        <tr><td style="padding-right:8px; white-space:nowrap; font-weight:600;">Target</td>
            <td>Nenner's current price target &mdash; \u2191 upside / \u2193 downside.
            When reached, Nenner typically sets a new target further out or reverses the signal</td></tr>
        <tr><td style="padding-right:8px; white-space:nowrap; font-weight:600;">P/L%</td>
            <td>Profit/Loss since signal entry &mdash; positive means the trade is in your favor</td></tr>
        <tr><td style="padding-right:8px; white-space:nowrap; font-weight:600;">Cancel</td>
            <td>The dollar price where the signal is cancelled on a closing basis &mdash; this is the stop level</td></tr>
        <tr><td style="padding-right:8px; white-space:nowrap; font-weight:600;">Cancel%</td>
            <td>Distance from current price to the cancel level &mdash; smaller = closer to signal flip</td></tr>
        <tr><td style="padding-right:8px; white-space:nowrap; font-weight:600;">Day</td>
            <td>Current trade age / average trade duration for this instrument (e.g. 6/14 = day 6 of a trade that typically lasts 14 days). <span style="color:{_CLR_RED};">Red</span> when at or past average, <span style="color:{_CLR_AMBER};">amber</span> when &ge;85% through</td></tr>
        <tr><td style="padding-right:8px; white-space:nowrap; font-weight:600;">NTC</td>
            <td>\u25bc = number of cancel level changes (Note The Change) in last 30 days &mdash; more bars = Nenner is adjusting the cancel frequently, often a precursor to signal reversal</td></tr>
        <tr><td style="padding-right:8px; white-space:nowrap; font-weight:600;">*</td>
            <td>Implied reversal &mdash; prior signal was cancelled, current signal is inferred (cancellation = reversal in Nenner's system)</td></tr>
        <tr><td style="padding-right:8px; white-space:nowrap; font-weight:600;">R:R</td>
            <td>Reward-to-Risk ratio from current price &mdash; reward = distance to target, risk = distance to cancel level. Higher is better; below 1:1 means risk exceeds potential reward</td></tr>
      </table>
    </td></tr>'''


def _build_inflection_alerts_html(stocks_data: list[dict]) -> str:
    """Build inflection alert boxes for stocks with active flags."""
    alerts_html = ""

    for s in stocks_data:
        flags = s.get("inflection_flags", [])
        if not flags:
            continue

        # Choose border color and background
        if "CANCEL_DANGER" in flags:
            border_clr, bg_clr = _CLR_RED, _CLR_LIGHT_RED
        elif "REVERSAL" in flags:
            border_clr, bg_clr = _CLR_PURPLE, _CLR_LIGHT_PURPLE
        elif "LOW_RR" in flags or "AT_TARGET" in flags:
            border_clr, bg_clr = _CLR_GREEN, _CLR_LIGHT_GREEN
        elif "TARGET_REACHED" in flags:
            border_clr, bg_clr = _CLR_AMBER, _CLR_LIGHT_AMBER
        else:
            border_clr, bg_clr = _CLR_AMBER, _CLR_LIGHT_AMBER

        # Build title
        dt = s["display_ticker"]
        if "CANCEL_DANGER" in flags:
            title = f'{dt} &mdash; CANCEL IN DANGER ZONE'
        elif "REVERSAL" in flags:
            title = f'{dt} &mdash; SIGNAL JUST REVERSED ({s["last_signal_date"]})'
        elif "AT_TARGET" in flags:
            title = f'{dt} &mdash; AT PRICE TARGET'
        elif "TARGET_REACHED" in flags:
            staircase = s.get("target_staircase", {})
            prev_tp = staircase.get("previous_target")
            title = f'{dt} &mdash; TARGET {_fmt_price(prev_tp)} REACHED &mdash; NEW TARGET SET'
        elif "HIGH_CHURN" in flags:
            title = f'{dt} &mdash; HIGH CANCEL CHURN'
        elif "TRADE_AGING" in flags:
            age_days = s.get("trade_age_days", "?")
            avg_dur = s.get("avg_duration")
            avg_str = f'{avg_dur:.0f}' if avg_dur else "?"
            title = f'{dt} &mdash; TRADE AGING (DAY {age_days} OF AVG {avg_str})'
        elif "LOW_RR" in flags:
            title = f'{dt} &mdash; REWARD:RISK DETERIORATED'
        else:
            title = f'{dt} &mdash; APPROACHING CANCEL LEVEL'

        # Build detail lines
        lines = []

        if s["price"] and s["cancel_level"]:
            dist_str = _fmt_pct(s["cancel_dist_pct"])
            lines.append(
                f'Price {_fmt_price(s["price"])} vs cancel {_fmt_price(s["cancel_level"])} '
                f'({dist_str} away)'
            )

        if s["implied_reversal"]:
            opposite = "BUY" if s["signal"] == "BUY" else "SELL"
            lines.append(
                f'Prior {opposite} was cancelled &rarr; implied {s["signal"]} '
                f'from {_fmt_price(s["origin_price"])}'
            )

        # Target staircase info
        staircase = s.get("target_staircase", {})
        if staircase.get("is_staircasing"):
            prev_tp = staircase.get("previous_target")
            new_tp = staircase.get("latest_target")
            reached_count = staircase.get("targets_reached", 0)
            direction = staircase.get("staircase_direction", "")
            direction_label = direction.lower() if direction else "new"
            lines.append(
                f'Target staircase: {reached_count} target(s) reached in 60 days '
                f'&rarr; {direction_label} target set at {_fmt_price(new_tp)}'
                f'{f" (prev: {_fmt_price(prev_tp)})" if prev_tp else ""}'
            )

        if s["ntc_count_30d"] >= 3:
            traj = s["cancel_trajectory"]
            traj_str = " &rarr; ".join(f"{v:g}" for v in traj[-5:])
            lines.append(
                f'{s["ntc_count_30d"]} cancel changes in 30 days: {traj_str}'
            )

        if s["reward_risk"] is not None:
            lines.append(f'Reward:Risk from here = {s["reward_risk"]:.1f}:1')

        if s["risk_flag"]:
            lines.append(f'Quant flag: <b>{s["risk_flag"]}</b>')

        detail_html = "<br>".join(lines)

        alerts_html += f'''
        <div style="border-left:4px solid {border_clr}; background:{bg_clr};
                    padding:12px 16px; margin-bottom:10px;
                    border-radius:0 4px 4px 0; font-family:{_FONT};">
          <b style="color:{border_clr}; font-size:13px;">{title}</b><br>
          <span style="font-size:13px; color:{_CLR_TEXT}; line-height:1.6;">
            {detail_html}
          </span>
        </div>'''

    if not alerts_html:
        return ""

    return f'''
    <tr><td style="padding:0 32px 16px;">
      <h2 style="margin:0 0 12px; font-size:16px; color:{_CLR_TEXT};
                 font-family:{_FONT};">
        \u26a0\ufe0f Inflection Alerts
      </h2>
      {alerts_html}
    </td></tr>'''


def _build_stock_detail_html(stocks_data: list[dict]) -> str:
    """Build per-stock detail cards."""
    cards = ""

    for s in stocks_data:
        signal_emoji = "\U0001f534" if s["signal"] == "SELL" else "\U0001f7e2"
        implied_note = " (implied reversal)" if s["implied_reversal"] else ""

        # Info rows
        info_rows = ""
        row_style = f"font-size:13px; color:{_CLR_TEXT}; line-height:1.8;"

        info_rows += (
            f'<tr><td style="color:{_CLR_MUTED}; padding-right:12px; white-space:nowrap;">Signal</td>'
            f'<td style="{row_style}"><b style="color:{_signal_color(s["signal"])};">'
            f'{signal_emoji} {s["signal"]}</b> from {_fmt_price(s["origin_price"])}'
            f' ({s["last_signal_date"]}){implied_note}</td></tr>'
        )

        info_rows += (
            f'<tr><td style="color:{_CLR_MUTED}; padding-right:12px;">Cancel</td>'
            f'<td style="{row_style}">{_fmt_price(s["cancel_level"])}'
            f' ({_fmt_pct(s["cancel_dist_pct"])} from price)</td></tr>'
        )

        info_rows += (
            f'<tr><td style="color:{_CLR_MUTED}; padding-right:12px;">Price</td>'
            f'<td style="{row_style}">{_fmt_price(s["price"])}'
            f' &nbsp;|&nbsp; P/L: <span style="color:{_pnl_color(s["pnl_pct"])}; font-weight:600;">'
            f'{_fmt_pct(s["pnl_pct"])}</span></td></tr>'
        )

        if s["target_price"]:
            cond = f' ({s["target_condition"]})' if s.get("target_condition") else ""
            target_line = (
                f'{_fmt_price(s["target_price"])} '
                f'{s.get("target_direction", "")}{cond}'
            )
            # Add staircase context if applicable
            staircase = s.get("target_staircase", {})
            if staircase.get("is_staircasing"):
                prev_tp = staircase.get("previous_target")
                reached_n = staircase.get("targets_reached", 0)
                sc_dir = staircase.get("staircase_direction", "")
                sc_label = sc_dir.lower() if sc_dir else "new"
                target_line += (
                    f' &nbsp;&bull;&nbsp; '
                    f'<span style="color:{_CLR_AMBER}; font-weight:600;">'
                    f'{reached_n} target(s) reached &rarr; {sc_label} target</span>'
                )
            info_rows += (
                f'<tr><td style="color:{_CLR_MUTED}; padding-right:12px;">Target</td>'
                f'<td style="{row_style}">{target_line}</td></tr>'
            )

        if s["reward_risk"] is not None:
            rr_clr = _CLR_GREEN if s["reward_risk"] >= 2 else (
                _CLR_AMBER if s["reward_risk"] >= 1 else _CLR_RED)
            info_rows += (
                f'<tr><td style="color:{_CLR_MUTED}; padding-right:12px;">R:R</td>'
                f'<td style="{row_style}"><span style="color:{rr_clr}; font-weight:600;">'
                f'{s["reward_risk"]:.1f}:1</span> from current price</td></tr>'
            )

        # Cancel trajectory
        if s["cancel_trajectory"]:
            traj = s["cancel_trajectory"]
            traj_str = " &rarr; ".join(f"{v:g}" for v in traj[-6:])
            info_rows += (
                f'<tr><td style="color:{_CLR_MUTED}; padding-right:12px;">Cancel&nbsp;History</td>'
                f'<td style="{row_style}">{traj_str}'
                f' ({s["ntc_count_30d"]} changes in 30d)</td></tr>'
            )

        # Cycles
        if s["cycles"]:
            cycle_parts = []
            for c in s["cycles"][:4]:
                tf = c.get("timeframe", "?")
                d = c.get("direction", "?")
                cycle_parts.append(f'{tf} {d}')
            cycle_str = ", ".join(cycle_parts)
            align_clr = {
                "ALIGNED": _CLR_GREEN,
                "CONFLICTING": _CLR_RED,
                "MIXED": _CLR_AMBER,
            }.get(s["cycle_alignment"], _CLR_MUTED)
            info_rows += (
                f'<tr><td style="color:{_CLR_MUTED}; padding-right:12px;">Cycles</td>'
                f'<td style="{row_style}">{cycle_str}'
                f' &mdash; <span style="color:{align_clr}; font-weight:600;">'
                f'{s["cycle_alignment"]}</span></td></tr>'
            )

        # Trade stats
        ts = s.get("trade_stats")
        if ts:
            flag_html = ""
            if s["risk_flag"]:
                flag_clr = _CLR_RED if s["risk_flag"] == "AVOID" else _CLR_AMBER
                flag_html = f' &nbsp;<span style="color:{flag_clr}; font-weight:600;">{s["risk_flag"]}</span>'
            info_rows += (
                f'<tr><td style="color:{_CLR_MUTED}; padding-right:12px;">Quant</td>'
                f'<td style="{row_style}">Sharpe {ts["sharpe"]:.2f} | '
                f'WR {ts["win_rate"]:.0f}% | Kelly {ts["kelly"]:.0%} | '
                f'{ts["trades"]} trades{flag_html}</td></tr>'
            )
        else:
            info_rows += (
                f'<tr><td style="color:{_CLR_MUTED}; padding-right:12px;">Quant</td>'
                f'<td style="{row_style}; color:{_CLR_MUTED}; font-style:italic;">'
                f'Insufficient trade history</td></tr>'
            )

        # Trade duration
        age_days = s.get("trade_age_days")
        avg_dur = s.get("avg_duration")
        med_dur = s.get("median_duration")
        age_ratio = s.get("trade_age_ratio")
        if age_days is not None:
            dur_parts = [f'Day {age_days}']
            if avg_dur:
                dur_parts.append(f'avg {avg_dur:.0f}d')
            if med_dur:
                dur_parts.append(f'median {med_dur:.0f}d')
            dur_str = " | ".join(dur_parts)
            if age_ratio and age_ratio >= 1.0:
                dur_str += (
                    f' &mdash; <span style="color:{_CLR_RED}; font-weight:600;">'
                    f'AT/PAST AVERAGE DURATION</span>'
                )
            elif age_ratio and age_ratio >= 0.85:
                dur_str += (
                    f' &mdash; <span style="color:{_CLR_AMBER}; font-weight:600;">'
                    f'APPROACHING AVERAGE DURATION</span>'
                )
            info_rows += (
                f'<tr><td style="color:{_CLR_MUTED}; padding-right:12px;">Duration</td>'
                f'<td style="{row_style}">{dur_str}</td></tr>'
            )

        cards += f'''
        <div style="border:1px solid {_CLR_BORDER}; border-radius:8px;
                    padding:16px 20px; margin-bottom:14px; background:{_CLR_WHITE};">
          <h3 style="margin:0 0 10px; font-size:15px; color:{_CLR_TEXT};
                     font-family:{_FONT};">
            {s["display_ticker"]} &mdash; {s["name"]}
          </h3>
          <table role="presentation" cellpadding="0" cellspacing="0"
                 style="font-family:{_FONT};">
            {info_rows}
          </table>
        </div>'''

    return f'''
    <tr><td style="padding:0 32px 16px;">
      <h2 style="margin:0 0 12px; font-size:16px; color:{_CLR_TEXT};
                 font-family:{_FONT};">
        \U0001f4c8 Stock-by-Stock Detail
      </h2>
      {cards}
    </td></tr>'''


def _build_exit_framework_html(stocks_data: list[dict]) -> str:
    """Build exit timing framework — stocks ranked by exit urgency."""

    # Score each stock for exit urgency
    scored = []
    for s in stocks_data:
        urgency = 0
        reasons = []

        rr = s.get("reward_risk")
        if rr is not None and rr < 1.0:
            urgency += 3
            reasons.append(f'R:R is {rr:.1f}:1 (below 1:1)')
        elif rr is not None and rr < 2.0:
            urgency += 1
            reasons.append(f'R:R is {rr:.1f}:1 (compressing)')

        if "AT_TARGET" in s.get("inflection_flags", []):
            urgency += 3
            reasons.append(f'Price within 1% of target ({_fmt_price(s["target_price"])})')

        # Target staircase — targets are being hit and reset lower/higher
        staircase = s.get("target_staircase", {})
        if staircase.get("is_staircasing") and staircase.get("targets_reached", 0) >= 2:
            urgency += 2
            reached_n = staircase["targets_reached"]
            reasons.append(
                f'{reached_n} price targets reached in 60 days '
                f'(Nenner setting new targets — watch for reversal)'
            )
        elif staircase.get("is_staircasing") and staircase.get("targets_reached", 0) >= 1:
            urgency += 1
            reasons.append(
                f'Previous target reached, new target at {_fmt_price(staircase.get("latest_target"))}'
            )

        if "CANCEL_DANGER" in s.get("inflection_flags", []):
            urgency += 2
            reasons.append(f'Cancel distance only {_fmt_pct(s["cancel_dist_pct"])}')

        ntc = s.get("ntc_count_30d", 0)
        if ntc >= 4:
            urgency += 2
            reasons.append(f'Cancel tightened {ntc}x in 30 days (conviction weakening)')
        elif ntc >= 3:
            urgency += 1
            reasons.append(f'Cancel tightened {ntc}x in 30 days')

        if s["cycle_alignment"] == "CONFLICTING":
            urgency += 1
            reasons.append('Cycles conflicting with signal direction')

        if s["risk_flag"] == "AVOID":
            urgency += 2
            reasons.append('Quant metrics flagged AVOID')

        pnl = s.get("pnl_pct")
        if pnl is not None and pnl < -3:
            urgency += 1
            reasons.append(f'Losing position ({_fmt_pct(pnl)})')

        # Trade aging — at or past average trade duration
        age_ratio = s.get("trade_age_ratio")
        age_days = s.get("trade_age_days")
        avg_dur = s.get("avg_duration")
        if age_ratio is not None and age_ratio >= 1.0:
            urgency += 2
            reasons.append(
                f'Day {age_days} of avg {avg_dur:.0f}-day trade '
                f'(past average duration — diminishing edge)'
            )
        elif age_ratio is not None and age_ratio >= 0.85:
            urgency += 1
            reasons.append(
                f'Day {age_days} of avg {avg_dur:.0f}-day trade '
                f'(approaching average duration)'
            )

        if urgency > 0:
            scored.append((urgency, s, reasons))

    scored.sort(key=lambda x: x[0], reverse=True)

    if not scored:
        return ""

    items_html = ""
    for rank, (urgency, s, reasons) in enumerate(scored, 1):
        pnl_str = _fmt_pct(s["pnl_pct"])
        pnl_clr = _pnl_color(s["pnl_pct"])

        reasons_html = "<br>".join(
            f'&bull; {r}' for r in reasons
        )

        urgency_clr = _CLR_RED if urgency >= 4 else (_CLR_AMBER if urgency >= 2 else _CLR_MUTED)

        items_html += f'''
        <div style="padding:10px 14px; margin-bottom:8px;
                    border-left:3px solid {urgency_clr};
                    background:{_CLR_ROW_ALT}; border-radius:0 4px 4px 0;">
          <span style="font-size:14px; font-weight:600; color:{_CLR_TEXT};">
            {rank}. {s["display_ticker"]}
          </span>
          <span style="color:{pnl_clr}; font-weight:600; font-size:13px;">
            &nbsp;{pnl_str}
          </span>
          <br>
          <span style="font-size:12px; color:{_CLR_TEXT}; line-height:1.6;">
            {reasons_html}
          </span>
        </div>'''

    return f'''
    <tr><td style="padding:0 32px 16px;">
      <h2 style="margin:0 0 6px; font-size:16px; color:{_CLR_TEXT};
                 font-family:{_FONT};">
        \U0001f4c9 Exit Timing Framework
      </h2>
      <p style="margin:0 0 12px; font-size:12px; color:{_CLR_MUTED}; line-height:1.6;">
        Stocks ranked by exit urgency. A higher score means more reasons to consider
        reducing or closing the position. Factors scored:
        <b>R:R &lt; 1:1</b> (risk exceeds reward from here),
        <b>at target</b> (price within 1% of Nenner's target),
        <b>target staircase</b> (previous target reached, new target set &mdash; watch for reversal),
        <b>cancel danger</b> (price near the flip level),
        <b>NTC churn</b> (3+ cancel changes in 30 days = weakening conviction),
        <b>cycle conflict</b> (cycles moving against the signal),
        <b>quant AVOID</b> (negative Sharpe or Kelly on historical trades),
        <b>trade aging</b> (current trade approaching or exceeding average duration for this instrument).
      </p>
      {items_html}
    </td></tr>'''


def _build_stanley_take_html(take_text: str) -> str:
    if not take_text:
        return ""
    return f'''
    <tr><td style="padding:0 32px 24px;">
      <h2 style="margin:0 0 12px; font-size:16px; color:{_CLR_TEXT};
                 font-family:{_FONT};">
        \U0001f52e Stanley's Take
      </h2>
      <div style="border:1px solid {_CLR_BORDER}; border-radius:8px;
                  padding:16px 20px; background:{_CLR_ROW_ALT};
                  font-size:14px; color:{_CLR_TEXT}; line-height:1.7;
                  font-family:{_FONT};">
        {take_text}
      </div>
    </td></tr>'''


def _build_footer_html(stocks_data: list[dict]) -> str:
    now = datetime.now()
    sources = set(s.get("price_source", "") for s in stocks_data if s.get("price_source"))
    source_str = ", ".join(sorted(sources)) if sources else "cached"
    return f'''
    <tr><td style="padding:16px 32px; background:{_CLR_ROW_ALT};
                   border-top:1px solid {_CLR_BORDER};">
      <p style="margin:0; font-size:11px; color:{_CLR_MUTED}; font-family:{_FONT};">
        Generated by Stanley
        &nbsp;&bull;&nbsp; {now.strftime("%b %d, %Y %I:%M %p")}
        &nbsp;&bull;&nbsp; Prices: {source_str}
      </p>
      <p style="margin:4px 0 0; font-size:11px; color:{_CLR_MUTED}; font-family:{_FONT};">
        For internal use only
      </p>
    </td></tr>'''


# ---------------------------------------------------------------------------
# LLM Commentary
# ---------------------------------------------------------------------------

REPORT_SYSTEM_PROMPT = """\
You are Stanley, a trading advisor modeled after Stanley Druckenmiller. \
You write the "Stanley's Take" section of a daily stock report for a \
portfolio manager who trades 6 individual stocks using the Nenner Cycle \
Research system.

## Druckenmiller Principles
- Bet big when conviction is high; cut quickly when wrong
- The best traders protect capital above all else
- Timing exits is harder than timing entries — use systematic signals
- Don't get married to a position — when the thesis changes, get out
- Concentration beats diversification when you have an edge

## Your Task
Write 3-4 paragraphs analyzing today's portfolio state. Be SPECIFIC — \
reference actual prices, levels, and percentages. Focus on:
1. Which positions to hold vs exit, with clear reasoning
2. Where the inflection points are (cancel proximity, cycle turns)
3. Risk management — which positions have deteriorating R:R
4. Trade duration — if a trade is approaching or past its average duration, the edge is fading; don't recommend initiating new trades late in the cycle
5. Cross-stock observations (correlations, sector moves)

## Formatting
Write in HTML using only <b>, <i>, <br> tags. Use \\n for paragraph breaks.
Keep the total under 2000 characters. Be direct and opinionated — do not \
hedge with "consider" or "may want to." Give clear guidance.
"""


def _build_llm_context(stocks_data: list[dict]) -> str:
    """Build a structured text summary for the LLM."""
    lines = ["PORTFOLIO SNAPSHOT:"]
    for s in stocks_data:
        flags = ", ".join(s.get("inflection_flags", [])) or "none"
        rr_val = s.get("reward_risk")
        rr_str = f"{rr_val:.1f}:1" if rr_val is not None else "N/A"
        implied_str = " (implied)" if s["implied_reversal"] else ""
        lines.append(
            f"  {s['ticker']} | {s['signal']}{implied_str} "
            f"from {_fmt_price(s['origin_price']).replace('&mdash;', '—')} | "
            f"Price: {_fmt_price(s['price']).replace('&mdash;', '—')} | "
            f"P/L: {_fmt_pct(s['pnl_pct']).replace('&mdash;', '—')} | "
            f"Cancel: {_fmt_price(s['cancel_level']).replace('&mdash;', '—')} "
            f"({_fmt_pct(s['cancel_dist_pct']).replace('&mdash;', '—')} dist) | "
            f"Target: {_fmt_price(s['target_price']).replace('&mdash;', '—')} | "
            f"R:R: {rr_str} | "
            f"NTC 30d: {s['ntc_count_30d']} | "
            f"Cycles: {s['cycle_alignment']} | "
            f"Flags: {flags}"
        )

        # Trade duration
        age_days = s.get("trade_age_days")
        avg_dur = s.get("avg_duration")
        age_ratio = s.get("trade_age_ratio")
        if age_days is not None:
            dur_str = f"Day {age_days}"
            if avg_dur:
                dur_str += f", avg={avg_dur:.0f}d, ratio={age_ratio:.2f}" if age_ratio else f", avg={avg_dur:.0f}d"
            lines.append(f"    Duration: {dur_str}")

        ts = s.get("trade_stats")
        if ts:
            lines.append(
                f"    Stats: Sharpe {ts['sharpe']:.2f}, WR {ts['win_rate']:.0f}%, "
                f"Kelly {ts['kelly']:.0%}, {ts['trades']} trades, Flag: {s['risk_flag'] or 'none'}"
            )

        staircase = s.get("target_staircase", {})
        if staircase.get("is_staircasing"):
            prev_tp = staircase.get("previous_target")
            new_tp = staircase.get("latest_target")
            reached_n = staircase.get("targets_reached", 0)
            sc_dir = staircase.get("staircase_direction", "")
            lines.append(
                f"    Target Staircase: {reached_n} target(s) reached in 60 days, "
                f"prev={prev_tp}, new={new_tp}, direction={sc_dir}"
            )

    return "\n".join(lines)


def _generate_stanley_take(stocks_data: list[dict], api_key: str) -> str:
    """Call the Anthropic API for Stanley's interpretive commentary."""
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    context = _build_llm_context(stocks_data)

    user_message = (
        f"Here is today's portfolio state for the 6 focus stocks:\n\n"
        f"{context}\n\n"
        f"Write Stanley's Take for today's report."
    )

    last_error = None
    for attempt in range(LLM_RETRY_ATTEMPTS + 1):
        try:
            message = client.messages.create(
                model=LLM_MODEL,
                max_tokens=LLM_MAX_TOKENS_REPORT,
                system=[{
                    "type": "text",
                    "text": REPORT_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_message}],
            )
            return message.content[0].text
        except Exception as e:
            last_error = e
            if attempt < LLM_RETRY_ATTEMPTS:
                wait = 2 ** (attempt + 1)
                log.warning(f"Stock report LLM error (attempt {attempt + 1}), "
                            f"retrying in {wait}s: {e}")
                time.sleep(wait)

    log.error(f"Stock report LLM failed after {LLM_RETRY_ATTEMPTS + 1} attempts: {last_error}")
    return ""


# ---------------------------------------------------------------------------
# Report Assembly
# ---------------------------------------------------------------------------

def build_stock_report_html(stocks_data: list[dict],
                            stanley_take: str = "") -> str:
    """Assemble the full HTML email from gathered data and LLM commentary."""
    header = _build_header_html(stocks_data)
    heat_map = _build_heat_map_html(stocks_data)
    inflection = _build_inflection_alerts_html(stocks_data)
    detail = _build_stock_detail_html(stocks_data)
    exit_fw = _build_exit_framework_html(stocks_data)
    take = _build_stanley_take_html(stanley_take)
    footer = _build_footer_html(stocks_data)

    # Stock report uses table-row layout — assemble inner table, wrap in shell
    inner_table = f'''
        <table role="presentation" width="680" cellpadding="0" cellspacing="0"
               style="background-color:{_CLR_WHITE}; border-radius:8px;
                      overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,0.06);">
          {header}
          {heat_map}
          {inflection}
          {detail}
          {exit_fw}
          {take}
          {footer}
        </table>'''

    return _wrap_document(body_html=inner_table, max_width=1680)


def build_report_subject(stocks_data: list[dict]) -> str:
    """Build a descriptive email subject line."""
    now = datetime.now()
    day = now.strftime("%a %b %d")

    sell_count = sum(1 for s in stocks_data if s["signal"] == "SELL")
    buy_count = len(stocks_data) - sell_count

    # Highlight most urgent stock
    urgent = None
    for s in stocks_data:
        flags = s.get("inflection_flags", [])
        if "CANCEL_DANGER" in flags or "REVERSAL" in flags:
            urgent = s["ticker"]
            break

    parts = [f"Stanley's Stock Report \u2014 {day}", f"{sell_count}S/{buy_count}B"]
    if urgent:
        parts.append(f"\u26a0 {urgent}")
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Entry Points
# ---------------------------------------------------------------------------

def generate_and_send_stock_report(
    conn: sqlite3.Connection,
    db_path: str,
    send_email_flag: bool = True,
    include_llm: bool = True,
) -> str:
    """Generate the full stock report and optionally send via email.

    This is the primary integration point called from the scheduler or CLI.
    Returns the generated HTML.
    """
    log.info("Generating Stanley's Stock Report...")

    # 1. Gather data
    stocks_data = gather_report_data(conn)
    if not stocks_data:
        log.warning("Stock report: no data available for focus stocks")
        return ""

    # 2. Generate LLM commentary
    stanley_take = ""
    if include_llm:
        try:
            from .llm_parser import _get_cached_api_key
            api_key = _get_cached_api_key()
            stanley_take = _generate_stanley_take(stocks_data, api_key)
        except Exception as e:
            log.error(f"Stock report LLM commentary failed: {e}")

    # 3. Build HTML
    html = build_stock_report_html(stocks_data, stanley_take)
    subject = build_report_subject(stocks_data)

    # 4. Send email
    if send_email_flag:
        send_email(subject, html)

    log.info(f"Stock report generated: {len(html)} chars, "
             f"{len(stocks_data)} stocks, LLM={'yes' if stanley_take else 'no'}")
    return html


def generate_stock_report_on_demand(conn: sqlite3.Connection,
                                    db_path: str) -> str:
    """Generate the report and send it (for CLI testing).

    Returns the HTML body.
    """
    return generate_and_send_stock_report(
        conn, db_path, send_email_flag=True, include_llm=True,
    )
