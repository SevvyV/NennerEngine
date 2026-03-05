"""
Fischer Signal Integration — Nenner Conviction Scoring, Earnings, Ex-Div
=========================================================================
Spec reference: Fischer_Agent_Specification_v2.md §5.3, §5.4, §6

Queries the existing Nenner signal database directly (no bridge layer).
Provides:
  - Nenner Conviction Score (0–100)
  - Earnings announcement proximity check
  - Ex-dividend proximity check
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from .config import DEFAULT_DB_PATH

log = logging.getLogger("fischer")

# ---------------------------------------------------------------------------
# Database path — same as nenner_mcp_server.py
# ---------------------------------------------------------------------------
DB_PATH = Path(DEFAULT_DB_PATH)

# ---------------------------------------------------------------------------
# §6.2 — ETF to Nenner Futures Ticker Mapping
# ---------------------------------------------------------------------------
ETF_TO_NENNER: dict[str, str] = {
    "SPY": "ES",
    "QQQ": "NQ",
    "DIA": "YM",
    "GLD": "GC",
    "SLV": "SI",
    "TLT": "ZB",
    "USO": "CL",
    "UNG": "NG",
    "CORN": "ZC",
    "SOYB": "ZS",
    "WEAT": "ZW",
    "FXE": "EUR/USD",
    "UUP": "DXY",
    "GBTC": "BTC",
    "ETHE": "ETH",
    "BITO": "BTC",
    "MSTR": "BTC",
    "UDN": "DXY",   # inverse dollar — same signal, opposite direction
    "GDXJ": "GC",   # gold miners proxy to gold
    "NEM": "GC",
}

# Stocks covered directly by Nenner (map to self)
NENNER_DIRECT_STOCKS = {
    "AAPL", "GOOG", "BAC", "MSFT", "NVDA", "TSLA",
}


def _get_nenner_ticker(equity_ticker: str) -> str | None:
    """Map an equity/ETF ticker to its Nenner signal ticker.

    Returns None if no Nenner coverage exists.
    """
    upper = equity_ticker.upper()
    if upper in ETF_TO_NENNER:
        return ETF_TO_NENNER[upper]
    if upper in NENNER_DIRECT_STOCKS:
        return upper
    return None


def _get_db() -> sqlite3.Connection:
    """Connect to the Nenner signal database."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# §6.3 — Nenner Conviction Score
# ---------------------------------------------------------------------------

@dataclass
class ConvictionResult:
    """Conviction score with component breakdown."""
    ticker: str
    nenner_ticker: str | None
    score: int                    # 0–100
    components: dict[str, int]    # component name → points
    effective_signal: str | None  # BUY / SELL / NEUTRAL
    signal_aligns: bool | None    # True if signal supports the trade
    cycles_aligned: int           # 0, 1, 2, or 3
    cancel_distance_pct: float | None
    delta_cap: float              # effective delta cap based on score
    max_dte: int                  # effective max DTE based on score
    warning: str | None = None


def compute_conviction(
    equity_ticker: str,
    intent: str,
    spot: float | None = None,
) -> ConvictionResult:
    """Compute the Nenner Conviction Score for a trade (§6.3).

    Parameters
    ----------
    equity_ticker : the equity/ETF ticker (e.g., "SPY", "AAPL")
    intent : "covered_put" or "covered_call"
    spot : current price (for cancel distance calculation)

    Returns ConvictionResult with score 0–100 and parameter adjustments.
    """
    nenner_ticker = _get_nenner_ticker(equity_ticker)

    if nenner_ticker is None:
        return ConvictionResult(
            ticker=equity_ticker,
            nenner_ticker=None,
            score=50,
            components={"default_neutral": 50},
            effective_signal=None,
            signal_aligns=None,
            cycles_aligned=0,
            cancel_distance_pct=None,
            delta_cap=0.35,
            max_dte=7,
            warning=f"No Nenner signal available for {equity_ticker} "
                    f"— proceeding on options math only.",
        )

    conn = _get_db()
    try:
        return _score_from_db(conn, equity_ticker, nenner_ticker, intent, spot)
    finally:
        conn.close()


def _score_from_db(
    conn: sqlite3.Connection,
    equity_ticker: str,
    nenner_ticker: str,
    intent: str,
    spot: float | None,
) -> ConvictionResult:
    """Query DB and compute conviction score."""
    components: dict[str, int] = {}
    score = 50  # base

    # --- Signal direction ---
    row = conn.execute(
        "SELECT effective_signal, origin_price, cancel_direction, cancel_level "
        "FROM current_state WHERE ticker = ?",
        (nenner_ticker,),
    ).fetchone()

    effective_signal = None
    signal_aligns = None
    cancel_distance_pct = None

    if row:
        effective_signal = row["effective_signal"]
        cancel_level = row["cancel_level"]
        origin_price = row["origin_price"]

        # Determine alignment: covered_put = short stock + short put (bearish,
        # want stock flat/down) → SELL signal aligns.
        # covered_call = long stock + short call (bullish, want stock flat/up)
        # → BUY signal aligns.
        if intent == "covered_put":
            signal_aligns = effective_signal == "SELL"
        else:  # covered_call
            signal_aligns = effective_signal == "BUY"

        # --- Cancel level proximity ---
        price_for_dist = spot or origin_price
        if cancel_level and price_for_dist and price_for_dist > 0:
            cancel_distance_pct = abs(cancel_level - price_for_dist) / price_for_dist

        # --- Signal + cancel interaction ---
        # Cancel proximity modulates signal conviction:
        #   Opposing signal near cancel → dying, mild penalty
        #   Opposing signal far from cancel → entrenched, full penalty
        #   Aligned signal near cancel → shaky, reduced bonus
        #   Aligned signal far from cancel → solid, full bonus
        if signal_aligns:
            components["signal_aligned"] = 30
            score += 30
            if cancel_distance_pct is not None:
                if cancel_distance_pct < 0.02:
                    components["cancel_near_aligned"] = -20
                    score -= 20
                elif cancel_distance_pct > 0.05:
                    components["cancel_far_aligned"] = 15
                    score += 15
        elif effective_signal in ("BUY", "SELL"):
            if cancel_distance_pct is not None and cancel_distance_pct < 0.02:
                # Opposing signal about to be cancelled — mild penalty
                components["signal_opposed_dying"] = -10
                score -= 10
            elif cancel_distance_pct is not None and cancel_distance_pct < 0.05:
                # Opposing signal with some room — moderate penalty
                components["signal_opposed"] = -25
                score -= 25
            else:
                # Opposing signal far from cancel — entrenched, full penalty
                components["signal_opposed_strong"] = -40
                score -= 40

    # --- Cycle alignment ---
    cycles = conn.execute(
        "SELECT timeframe, direction FROM cycles "
        "WHERE ticker = ? "
        "ORDER BY date DESC LIMIT 3",
        (nenner_ticker,),
    ).fetchall()

    # Determine expected cycle direction for the trade
    if intent == "covered_put":
        favorable_direction = "UP"
    else:
        favorable_direction = "DOWN"

    cycles_aligned = 0
    seen_timeframes = set()
    for c in cycles:
        tf = c["timeframe"]
        if tf in seen_timeframes:
            continue
        seen_timeframes.add(tf)
        if c["direction"] and favorable_direction.lower() in c["direction"].lower():
            cycles_aligned += 1

    if cycles_aligned == 3:
        components["all_cycles_aligned"] = 25
        score += 25
    elif cycles_aligned >= 1:
        components["mixed_cycles"] = 10
        score += 10
    else:
        components["no_cycles_aligned"] = -20
        score -= 20

    # Clamp to 0–100
    score = max(0, min(100, score))

    # Effective parameters — conviction is informational, never gates the scan.
    # All strikes shown regardless of score; conviction context helps the trader decide.
    delta_cap = 0.35
    max_dte = 8

    warning = None
    if score < 30:
        warning = (
            f"Nenner conviction {score}/100 — signal opposes intent. "
            f"Signal: {effective_signal}, Cycles aligned: {cycles_aligned}/3."
        )

    return ConvictionResult(
        ticker=equity_ticker,
        nenner_ticker=nenner_ticker,
        score=score,
        components=components,
        effective_signal=effective_signal,
        signal_aligns=signal_aligns,
        cycles_aligned=cycles_aligned,
        cancel_distance_pct=cancel_distance_pct,
        delta_cap=delta_cap,
        max_dte=max_dte,
        warning=warning,
    )


# ---------------------------------------------------------------------------
# §5.3 — Ex-Dividend Awareness
# ---------------------------------------------------------------------------

@dataclass
class ExDivInfo:
    """Ex-dividend proximity info (only populated when ex-div < expiry)."""
    ex_date: date
    dividend_amount: float | None
    days_before_expiry: int
    warning: str


def check_ex_div(
    ticker: str,
    expiries: list[date],
) -> ExDivInfo | None:
    """Check if ex-dividend date falls before any option expiry (§5.3).

    Returns ExDivInfo only when ex-div is relevant (before expiry).
    Returns None if no ex-div concern.
    """
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        cal = t.calendar
    except Exception:
        return None

    if cal is None or cal.empty if hasattr(cal, 'empty') else not cal:
        return None

    # yFinance calendar can be a DataFrame or dict depending on version
    ex_date = None
    dividend_amount = None

    if isinstance(cal, dict):
        ex_date_val = cal.get("Ex-Dividend Date")
        dividend_amount = cal.get("Dividend")
    else:
        # DataFrame format
        try:
            if "Ex-Dividend Date" in cal.columns:
                ex_date_val = cal["Ex-Dividend Date"].iloc[0]
            elif "Ex-Dividend Date" in cal.index:
                ex_date_val = cal.loc["Ex-Dividend Date"].iloc[0]
            else:
                return None
        except Exception:
            return None

    if ex_date_val is None:
        return None

    # Convert to date
    if isinstance(ex_date_val, datetime):
        ex_date = ex_date_val.date()
    elif isinstance(ex_date_val, date):
        ex_date = ex_date_val
    else:
        try:
            ex_date = date.fromisoformat(str(ex_date_val)[:10])
        except Exception:
            return None

    # Check if ex-div falls before any expiry
    latest_expiry = max(expiries)
    if ex_date >= latest_expiry:
        return None  # ex-div is after all expiries — no concern

    # Find the earliest expiry affected
    affected_expiries = [e for e in expiries if e > ex_date]
    if not affected_expiries:
        return None

    earliest_affected = min(affected_expiries)
    days_before = (earliest_affected - ex_date).days

    return ExDivInfo(
        ex_date=ex_date,
        dividend_amount=float(dividend_amount) if dividend_amount else None,
        days_before_expiry=days_before,
        warning=(
            f"Ex-div {ex_date} falls before {earliest_affected} expiry — "
            f"early assignment risk elevated for ITM puts"
        ),
    )


# ---------------------------------------------------------------------------
# §5.4 — Earnings Announcement Awareness
# ---------------------------------------------------------------------------

@dataclass
class EarningsInfo:
    """Earnings proximity info."""
    earnings_date: date
    days_away: int
    confirmed: bool  # single date = confirmed, range = estimated
    affects_expiries: list[date]
    clean_expiries: list[date]


def check_earnings(
    ticker: str,
    expiries: list[date],
) -> EarningsInfo | None:
    """Check if earnings announcement falls within the option window (§5.4).

    Returns EarningsInfo when earnings date falls on or before the latest expiry.
    Returns None if no earnings concern.
    """
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        cal = t.calendar
    except Exception:
        return None

    if cal is None or (hasattr(cal, 'empty') and cal.empty):
        return None

    # Extract earnings dates
    earnings_dates = None

    if isinstance(cal, dict):
        earnings_dates = cal.get("Earnings Date")
    else:
        # DataFrame format
        try:
            if "Earnings Date" in cal.columns:
                earnings_dates = cal["Earnings Date"].tolist()
            elif "Earnings Date" in cal.index:
                earnings_dates = cal.loc["Earnings Date"].tolist()
        except Exception:
            pass

    if not earnings_dates:
        return None

    # Normalize to list of dates
    if not isinstance(earnings_dates, (list, tuple)):
        earnings_dates = [earnings_dates]

    parsed_dates = []
    for ed in earnings_dates:
        if isinstance(ed, datetime):
            parsed_dates.append(ed.date())
        elif isinstance(ed, date):
            parsed_dates.append(ed)
        else:
            try:
                parsed_dates.append(date.fromisoformat(str(ed)[:10]))
            except Exception:
                continue

    if not parsed_dates:
        return None

    # Use earliest date as conservative assumption
    earnings_date = min(parsed_dates)
    latest_expiry = max(expiries)

    if earnings_date > latest_expiry:
        return None  # earnings after all expiries — no concern

    today = date.today()
    days_away = (earnings_date - today).days

    affects = [e for e in expiries if e >= earnings_date]
    clean = [e for e in expiries if e < earnings_date]

    return EarningsInfo(
        earnings_date=earnings_date,
        days_away=days_away,
        confirmed=len(set(parsed_dates)) == 1,
        affects_expiries=affects,
        clean_expiries=clean,
    )


def classify_expiry_earnings(
    expiry: date,
    earnings_info: EarningsInfo | None,
) -> str:
    """Classify an expiry relative to earnings (§5.4).

    Returns: "CLEAN", "STRADDLES", or "EARNINGS_TODAY"
    """
    if earnings_info is None:
        return "CLEAN"

    if expiry < earnings_info.earnings_date:
        return "CLEAN"

    today = date.today()
    if earnings_info.earnings_date == today and expiry == today:
        return "EARNINGS_TODAY"

    return "STRADDLES"
