"""
Fischer Scanner
================
Pure scan/rank logic for the Fischer options pipeline.

Stateless functions that take data in and return results — no DB writes,
no email sends.  Imported by fischer_daily_report.py for orchestration.
"""

import logging
import sqlite3
import time as _time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from .config import FISCHER_DEBUG_DB

log = logging.getLogger("nenner")

# ---------------------------------------------------------------------------
# Unified Ticker Universe (v2)
# ---------------------------------------------------------------------------

# All tickers compete equally — top 10 selected by theta/max_profit
SCAN_TICKERS: list[str] = [
    "AAPL", "AMZN", "AVGO", "GOOGL", "IWM",
    "META", "MSFT", "NVDA", "QQQ", "TSLA",
    "GLD", "MSTR", "SLV", "UNG", "USO",
]

TOP_PICKS = 10

CAPITAL = 500_000

# Filter rules per intent — puts and calls can be tuned independently
_PUT_RULES = {
    "min_p_win": 0.55,
    "max_p_win": 0.65,
    "max_p_otm": 0.60,
    "min_ratio": 0.70,
    "max_ratio": 3.0,
}
_CALL_RULES = {
    "min_p_win": 0.55,
    "max_p_win": 0.65,
    "max_p_otm": 0.575,
    "min_ratio": 0.70,
    "max_ratio": 3.0,
}


def get_rules(intent: str) -> dict:
    """Return the filter rules dict for a given intent."""
    return _CALL_RULES if intent == "covered_call" else _PUT_RULES


# ---------------------------------------------------------------------------
# Scan Slot Configuration (3 daily scans)
# ---------------------------------------------------------------------------

ScanSlot = Literal["opening", "midday", "closing"]


@dataclass(frozen=True)
class ScanSlotConfig:
    name: ScanSlot
    label: str                                    # human-readable for email
    short_dte_range: tuple[int, int] | None       # None = conviction-based
    store_weekly: bool                            # persist 7-14 DTE recs in DB


SCAN_SLOTS: dict[ScanSlot, ScanSlotConfig] = {
    "opening": ScanSlotConfig(
        name="opening", label="Opening Scan",
        short_dte_range=None, store_weekly=True,
    ),
    "midday": ScanSlotConfig(
        name="midday", label="Midday Scan",
        short_dte_range=None, store_weekly=True,
    ),
    "closing": ScanSlotConfig(
        name="closing", label="Closing Scan",
        short_dte_range=(1, 7), store_weekly=True,
    ),
}


# ---------------------------------------------------------------------------
# Debug scan log — captures every strike evaluated, not just the winner
# ---------------------------------------------------------------------------

_CHAIN_RETRY_ATTEMPTS = 3
_CHAIN_RETRY_DELAY = 5  # seconds between retries


def _init_debug_db():
    """Create the debug DB and table if needed. Returns connection."""
    conn = sqlite3.connect(FISCHER_DEBUG_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scan_debug (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_time TEXT,
            ticker TEXT,
            intent TEXT,
            strike REAL,
            expiry TEXT,
            dte INTEGER,
            spot REAL,
            bid REAL,
            ask REAL,
            iv REAL,
            p_win REAL,
            p_otm REAL,
            premium_ratio REAL,
            max_profit_ps REAL,
            ev_per_contract REAL,
            verdict TEXT,
            fail_reasons TEXT
        )
    """)
    return conn


def _log_debug_strikes(ticker: str, intent: str, ranked: list):
    """Write every evaluated strike to the debug DB with pass/fail reasons."""
    if not ranked:
        return
    try:
        conn = _init_debug_db()
        R = get_rules(intent)
        now = datetime.now().isoformat(timespec="seconds")
        today_date = date.today()

        rows = []
        for r in ranked:
            reasons = []
            if r.p_profit < R["min_p_win"]:
                reasons.append(f"p_win {r.p_profit:.3f} < {R['min_p_win']}")
            if r.p_profit > R["max_p_win"]:
                reasons.append(f"p_win {r.p_profit:.3f} > {R['max_p_win']}")
            if r.p_expire_worthless > R["max_p_otm"]:
                reasons.append(f"p_otm {r.p_expire_worthless:.3f} > {R['max_p_otm']}")
            if r.premium_ratio is None:
                reasons.append("ratio=None (ATM)")
            elif r.premium_ratio < R["min_ratio"]:
                reasons.append(f"ratio {r.premium_ratio:.2f} < {R['min_ratio']}")
            elif r.premium_ratio > R["max_ratio"]:
                reasons.append(f"ratio {r.premium_ratio:.2f} > {R['max_ratio']}")

            verdict = "PASS" if not reasons else "FAIL"
            dte = (r.expiry - today_date).days

            rows.append((
                now, ticker, intent, r.strike, str(r.expiry), dte,
                r.spot, r.bid, r.ask, r.iv, r.p_profit,
                r.p_expire_worthless, r.premium_ratio,
                r.max_profit_per_share, r.net_ev_per_contract,
                verdict, "; ".join(reasons) if reasons else "",
            ))

        conn.executemany("""
            INSERT INTO scan_debug
            (scan_time, ticker, intent, strike, expiry, dte, spot, bid, ask,
             iv, p_win, p_otm, premium_ratio, max_profit_ps, ev_per_contract,
             verdict, fail_reasons)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
        conn.close()
    except Exception as e:
        log.debug(f"Debug scan log failed for {ticker}: {e}")


# ---------------------------------------------------------------------------
# Core scan functions
# ---------------------------------------------------------------------------

def calc_shares(spot: float, capital: int = CAPITAL) -> int:
    """Calculate share count closest to target capital, rounded to 100 shares."""
    if spot <= 0:
        return 100
    return max(100, int(round(capital / spot, -2)))


def select_top_trades(
    all_recs: list[dict],
    n: int = TOP_PICKS,
) -> list[dict]:
    """Rank all candidates by extrinsic_value/max_profit ascending, return top N.

    Lower ratio = more directional profit relative to theta proceeds.
    Trades with zero or missing max_profit are excluded.
    """
    eligible = []
    for rec in all_recs:
        bid = rec.get("bid", 0) or rec.get("premium_per_share", 0)
        strike = rec.get("strike", 0)
        spot = rec.get("spot_at_recommend", 0)
        max_prof = rec.get("max_profit_per_share", 0)
        if max_prof <= 0 or bid <= 0:
            continue
        intrinsic = max(0, strike - spot)  # put intrinsic
        extrinsic = bid - intrinsic
        if extrinsic <= 0:
            continue
        ratio = extrinsic / max_prof
        rec["_rank_ratio"] = round(ratio, 4)
        eligible.append((ratio, rec))

    # Sort ascending: smallest extrinsic/max_profit first (most directional)
    eligible.sort(key=lambda t: t[0])
    return [t[1] for t in eligible[:n]]


def scan_ticker(
    ticker: str,
    dte_range: tuple[int, int] | None = None,
    intent: str = "covered_put",
    chain_data: tuple | None = None,
) -> list:
    """Run the Fischer EV pipeline for one ticker, return ranked EVResults.

    Parameters
    ----------
    ticker : equity/ETF ticker to scan
    dte_range : optional (min_dte, max_dte) inclusive filter.
                If None, uses conviction-based max_dte with min=0.
    intent : "covered_put" or "covered_call"
    chain_data : optional pre-loaded (DataFrame, ChainMeta) from bulk read.
                 If provided, skips the read_chain() retry loop.
    """
    from .fischer_engine import (
        compute_ev, implied_volatility, rank_strikes, time_to_expiry,
    )
    from .fischer_chain import read_chain, StaleChainError, _chain_cache
    from .fischer_signals import compute_conviction

    opt_type = "P" if intent == "covered_put" else "C"

    chain_df = None
    meta = None

    if chain_data is not None:
        # Pre-loaded from OptionChains bulk read — skip subprocess
        chain_df, meta = chain_data
    else:
        # Fall back to per-ticker read from Options_RT
        for attempt in range(_CHAIN_RETRY_ATTEMPTS):
            try:
                chain_df, meta = read_chain(ticker)
            except (StaleChainError, Exception) as e:
                log.warning(f"Fischer daily: chain error for {ticker}: {e}")
                return []

            if chain_df.empty:
                return []

            # Validate: check that bids have populated (RTD feed may be slow)
            opts = chain_df[chain_df["type"] == opt_type]
            if not opts.empty and opts["bid"].sum() > 0:
                break  # good data

            if attempt < _CHAIN_RETRY_ATTEMPTS - 1:
                log.info(f"  {ticker}: {opt_type} bids are zero, waiting {_CHAIN_RETRY_DELAY}s "
                         f"for RTD feed (attempt {attempt + 1}/{_CHAIN_RETRY_ATTEMPTS})")
                _chain_cache.pop(ticker.upper(), None)
                _time.sleep(_CHAIN_RETRY_DELAY)
            else:
                log.warning(f"  {ticker}: {opt_type} bids still zero after "
                            f"{_CHAIN_RETRY_ATTEMPTS} attempts")
                return []

    conviction = compute_conviction(ticker, intent)

    chain_filtered = chain_df[chain_df["type"] == opt_type].copy()
    if chain_filtered.empty:
        return []

    today_date = date.today()

    if dte_range is not None:
        min_dte, max_dte = dte_range
        chain_filtered = chain_filtered[
            chain_filtered["expiry"].apply(
                lambda e: min_dte <= (e - today_date).days <= max_dte
            )
        ]
    elif conviction.max_dte < 7:
        chain_filtered = chain_filtered[
            chain_filtered["expiry"].apply(
                lambda e: (e - today_date).days <= conviction.max_dte
            )
        ]

    if chain_filtered.empty:
        return []

    spot = meta.spot
    rate = meta.rate
    div_yield = meta.div_yield

    # Compute EV for each strike using per-strike IV from market mid.
    # No smile polynomial — market prices already embed the real skew.
    # Thomson One provides flat vol per expiry; the true skew lives in
    # bid/ask prices, and Newton-Raphson extracts per-strike IV directly.
    results = []
    for _, row in chain_filtered.iterrows():
        exp = row["expiry"]
        exp_date = exp if isinstance(exp, date) else exp.date()
        T = time_to_expiry(exp_date)
        if T <= 0:
            continue

        bid = row["bid"]
        ask = row["ask"]
        strike = row["strike"]
        mid = (bid + ask) / 2 if ask > 0 else bid

        if mid <= 0:
            continue

        sigma = implied_volatility(mid, spot, strike, T, rate, div_yield, opt_type)
        if sigma is None:
            continue

        ev = compute_ev(
            S=spot, K=strike, T=T, r=rate, sigma=sigma, q=div_yield,
            bid=bid, ask=ask, option_type=opt_type, expiry=exp_date,
            capital=CAPITAL, nenner_score=conviction.score,
            entry_price=None,  # fresh trade — entry = spot
            oi=int(row.get("oi", 0)),
            volume=int(row.get("volume", 0)),
        )
        results.append(ev)

    if not results:
        return []

    return rank_strikes(results, intent=intent)


def select_best_candidate(ranked: list, ticker: str, intent: str = "covered_put") -> tuple | None:
    """Pick the best EVResult from ranked results using P(Win) band filtering.

    Returns (ev, flag) or None if no results.
    Flag: "clean" = 0DTE in band, "deferred" = later expiry in band,
          "near_miss" = closest to band.
    """
    if not ranked:
        return None

    _log_debug_strikes(ticker, intent, ranked)
    R = get_rules(intent)
    in_band = [r for r in ranked
               if R["min_p_win"] <= r.p_profit <= R["max_p_win"]
               and r.p_expire_worthless <= R["max_p_otm"]
               and r.premium_ratio is not None
               and R["min_ratio"] <= r.premium_ratio <= R["max_ratio"]]

    if in_band:
        today_date = date.today()
        dte0 = [r for r in in_band if (r.expiry - today_date).days == 0]
        later = [r for r in in_band if (r.expiry - today_date).days > 0]

        if dte0:
            best = max(dte0, key=lambda r: r.max_profit_per_share)
            return (best, "clean")
        else:
            best = max(later, key=lambda r: r.max_profit_per_share)
            return (best, "deferred")
    else:
        # No strike passes ratio/P(Win) band — skip this ticker
        log.info(f"  {ticker}: no strike in ratio band, skipping")
        return None


def assemble_sections(
    all_results: dict,
    report_sections,
) -> dict:
    """For each report section, rank all tickers by theta/max_profit, pick top 10.

    Parameters
    ----------
    all_results : dict keyed by (ticker, intent, dte_label) -> rec dict
    report_sections : tuple of ReportSection instances

    Returns dict keyed by ReportSection -> list of rec dicts (up to TOP_PICKS).
    """
    sections_data = {}
    for section in report_sections:
        intent = section.intent
        dte_label = f"{section.dte_range[0]}-{section.dte_range[1]}"

        # Collect all tickers that have results for this section
        candidates = []
        for t in SCAN_TICKERS:
            key = (t, intent, dte_label)
            if key in all_results:
                candidates.append(all_results[key])

        sections_data[section] = select_top_trades(candidates)

    return sections_data
