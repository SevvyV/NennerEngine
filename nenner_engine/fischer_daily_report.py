"""
Fischer Daily Report
=====================
Automated daily covered-put recommendations with lifecycle tracking.

1. Every trading day at 10:00 AM ET, scans all equity tickers via the
   Fischer options pipeline, selects the top 5 by net EV (P(Win) >= 60%),
   stores them in fischer_recommendations, and emails the report.

2. At 4:30 PM ET on expiry days, settles expired recommendations:
   - ITM (stock < strike): assigned, P&L = (entry - strike) + premium
   - OTM (stock >= strike): expires worthless, cover short at close,
     P&L = (entry - close) + premium

Uses the same Fischer engine pipeline (read_chain → IV → compute_ev → rank)
and the existing Gmail SMTP infrastructure from stock_report.py.
"""

import logging
import sqlite3
import time as _time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal, Optional

log = logging.getLogger("nenner")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REPORT_TO = "sevagv@vartaniancapital.com"

# ---------------------------------------------------------------------------
# Unified Ticker Universe (v2)
# ---------------------------------------------------------------------------

# Always included in every section (10 tickers)
ALWAYS_TICKERS: tuple[str, ...] = (
    "AAPL", "AMZN", "AVGO", "GOOGL",
    "META", "MSFT", "NVDA", "QQQ", "TSLA",
)

# Macro pool — top 4 selected per section by premium:directional ratio
MACRO_POOL: tuple[str, ...] = (
    "GLD", "SLV", "TLT", "UNG", "USO",
)

MACRO_PICKS = 5

# Flat list of all tickers (used by settlement, scanning, etc.)
SCAN_TICKERS: list[str] = list(ALWAYS_TICKERS) + list(MACRO_POOL)

CAPITAL = 500_000
TOP_N = 99  # show all tickers per group (no limit)
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
    "max_p_otm": 0.60,
    "min_ratio": 0.70,
    "max_ratio": 3.0,
}

def _rules(intent: str) -> dict:
    """Return the filter rules dict for a given intent."""
    return _CALL_RULES if intent == "covered_call" else _PUT_RULES


# ---------------------------------------------------------------------------
# Debug scan log — captures every strike evaluated, not just the winner
# ---------------------------------------------------------------------------
_SCAN_DEBUG_DB = "E:/Workspace/NennerEngine/fischer_scan_debug.db"

def _init_debug_db():
    """Create the debug DB and table if needed. Returns connection."""
    conn = sqlite3.connect(_SCAN_DEBUG_DB)
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
        R = _rules(intent)
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
# Report Sections — 4 DTE × Intent combinations in a single email
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ReportSection:
    intent: str                    # "covered_put" or "covered_call"
    dte_range: tuple[int, int]     # (min_dte, max_dte) inclusive
    title: str                     # section header in email
    entry_note: str                # subtitle explaining entry assumption
    strike_label: str              # "put" or "call"

REPORT_SECTIONS: tuple[ReportSection, ...] = (
    ReportSection("covered_put",  (0, 7),
                  "Covered Puts 0\u20137 DTE",
                  "Entry assumes short sale at current spot", "put"),
    ReportSection("covered_put",  (8, 14),
                  "Covered Puts 8\u201314 DTE",
                  "Entry assumes short sale at current spot", "put"),
)

def _calc_shares(spot: float, capital: int = CAPITAL) -> int:
    """Calculate share count closest to target capital, rounded to 100 shares."""
    if spot <= 0:
        return 100
    return max(100, int(round(capital / spot, -2)))


def _select_macro_tickers(
    macro_results: dict[str, dict],
    n: int = MACRO_PICKS,
    intent: str = "covered_put",
) -> list[str]:
    """Rank macro pool tickers by premium_ratio quality, return top N.

    Filters to ratio within [min_ratio, max_ratio] and P(Win) >= min_p_win.
    If fewer than N qualify, returns all that qualify.
    """
    R = _rules(intent)
    eligible = []
    for ticker, rec in macro_results.items():
        ratio = rec.get("premium_ratio")
        if ratio is None:
            continue
        if not (R["min_ratio"] <= ratio <= R["max_ratio"]):
            continue
        if rec.get("p_win", 0) < R["min_p_win"]:
            continue
        # Prefer ratios near 2:1 (center of band) — lower distance = better
        dist_from_center = abs(ratio - 2.0)
        eligible.append((ticker, rec, dist_from_center))

    # Sort: closest to 2:1 center first, then by max_profit as tiebreaker
    eligible.sort(key=lambda t: (t[2], -t[1].get("max_profit_per_share", 0)))
    return [t[0] for t in eligible[:n]]


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
# Scan Pipeline
# ---------------------------------------------------------------------------

_CHAIN_RETRY_ATTEMPTS = 3
_CHAIN_RETRY_DELAY = 5  # seconds between retries


def _scan_ticker(
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


def _load_today_recs(conn: sqlite3.Connection, today_str: str) -> list[dict]:
    """Load all of today's recommendations (all slots)."""
    rows = conn.execute(
        "SELECT * FROM fischer_recommendations WHERE report_date = ? ORDER BY rank",
        (today_str,)
    ).fetchall()
    return [dict(r) for r in rows]


def _load_slot_recs(
    conn: sqlite3.Connection, today_str: str, slot: ScanSlot,
    intent: str = "covered_put",
    tickers: tuple[str, ...] | None = None,
) -> list[dict]:
    """Load recommendations for a specific date, scan slot, and intent.

    If tickers is provided, only load recs for those tickers.
    """
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        rows = conn.execute(
            f"SELECT * FROM fischer_recommendations "
            f"WHERE report_date = ? AND scan_slot = ? AND intent = ? "
            f"AND ticker IN ({placeholders}) ORDER BY rank",
            (today_str, slot, intent, *tickers)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM fischer_recommendations "
            "WHERE report_date = ? AND scan_slot = ? AND intent = ? ORDER BY rank",
            (today_str, slot, intent)
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------

def settle_expired_trades(conn: sqlite3.Connection) -> list[dict]:
    """Settle recommendations whose expiry has passed.

    For each unsettled trade where expiry <= today:
      - Fetch closing price on expiry day from price_history or yFinance
      - Covered Put:  ITM (close < strike) → assigned; OTM → cover short at close
      - Covered Call: ITM (close > strike) → called away; OTM → keep stock at close
      - Store results, return list of settlements

    Returns list of settlement dicts.
    """
    today_str = date.today().isoformat()

    rows = conn.execute("""
        SELECT id, ticker, strike, expiry, option_type, entry_price,
               premium_per_share, spot_at_recommend
        FROM fischer_recommendations
        WHERE settled = 0 AND expiry <= ?
    """, (today_str,)).fetchall()

    if not rows:
        log.info("Fischer settlement: no expired trades to settle")
        return []

    settlements = []
    for row in rows:
        rec_id = row["id"]
        ticker = row["ticker"]
        strike = row["strike"]
        expiry = row["expiry"]
        opt_type = row["option_type"] or "P"
        entry = row["entry_price"]
        premium = row["premium_per_share"]

        close_price = _get_closing_price(conn, ticker, expiry)
        if close_price is None:
            log.warning(f"Fischer settlement: no close price for {ticker} on {expiry}")
            continue

        if opt_type == "C":
            # Covered call: long stock + short call
            itm = 1 if close_price > strike else 0
            if itm:
                # Call assigned — stock called away at strike
                pnl_ps = (strike - entry) + premium
                notes = f"ITM: called away at {strike:.2f}, premium {premium:.2f}"
            else:
                # Call expires worthless — keep stock at close
                pnl_ps = (close_price - entry) + premium
                notes = f"OTM: expired worthless, stock at {close_price:.2f}"
        else:
            # Covered put: short stock + short put
            itm = 1 if close_price < strike else 0
            if itm:
                # Put assigned — short stock covered at strike
                pnl_ps = (entry - strike) + premium
                notes = f"ITM: assigned at {strike:.2f}, premium {premium:.2f}"
            else:
                # Put expires worthless — cover short at close
                pnl_ps = (entry - close_price) + premium
                notes = f"OTM: expired worthless, covered at {close_price:.2f}"

        # Total P&L for 1 contract (100 shares)
        pnl_total = pnl_ps * 100

        conn.execute("""
            UPDATE fischer_recommendations SET
                settled = 1,
                close_price_at_expiry = ?,
                itm_at_expiry = ?,
                pnl_per_share = ?,
                pnl_total = ?,
                settlement_date = ?,
                settlement_notes = ?
            WHERE id = ?
        """, (close_price, itm, round(pnl_ps, 4), round(pnl_total, 2),
              today_str, notes, rec_id))

        settlement = {
            "id": rec_id, "ticker": ticker, "strike": strike,
            "expiry": expiry, "entry": entry, "premium": premium,
            "close_price": close_price, "itm": bool(itm),
            "pnl_per_share": round(pnl_ps, 4),
            "pnl_total": round(pnl_total, 2),
            "notes": notes,
        }
        settlements.append(settlement)
        log.info(f"  Settled {ticker} {strike} exp {expiry}: "
                 f"{'ITM' if itm else 'OTM'} P&L=${pnl_total:+,.2f}")

    conn.commit()
    log.info(f"Fischer settlement: settled {len(settlements)} trades")
    return settlements


def _get_closing_price(conn: sqlite3.Connection, ticker: str, expiry_date: str) -> Optional[float]:
    """Get closing price for ticker on expiry date.

    Tries price_history table first, then falls back to yFinance.
    """
    row = conn.execute(
        "SELECT close FROM price_history WHERE ticker = ? AND date = ? "
        "ORDER BY source LIMIT 1",
        (ticker, expiry_date)
    ).fetchone()
    if row and row["close"]:
        return float(row["close"])

    # Fallback: fetch from yFinance
    try:
        from .prices import fetch_yfinance_daily
        fetch_yfinance_daily(conn, tickers=[ticker], period="5d")
        row = conn.execute(
            "SELECT close FROM price_history WHERE ticker = ? AND date = ? "
            "ORDER BY source LIMIT 1",
            (ticker, expiry_date)
        ).fetchone()
        if row and row["close"]:
            return float(row["close"])
    except Exception as e:
        log.warning(f"yFinance fallback failed for {ticker}: {e}")

    return None


# ---------------------------------------------------------------------------
# Email Builders
# ---------------------------------------------------------------------------

from .postmaster import (
    FONT as _FONT,
    CLR_BG as _CLR_BG,
    CLR_WHITE as _CLR_WHITE,
    CLR_HEADER as _CLR_HEADER,
    CLR_TEXT as _CLR_TEXT,
    CLR_MUTED as _CLR_MUTED,
    CLR_BORDER as _CLR_BORDER,
    CLR_GREEN as _CLR_GREEN,
    CLR_RED as _CLR_RED,
    CLR_ROW_ALT as _CLR_ROW_ALT,
    wrap_document as _wrap_document,
)


CLR_YELLOW = "#fff3cd"  # highlight for deferred/near-miss rows


# ---------------------------------------------------------------------------
# Disclosure — loaded from file so it can be updated without code changes
# ---------------------------------------------------------------------------

import pathlib as _pathlib
import re as _re

_DISCLOSURE_PATH = _pathlib.Path(__file__).parent / "fischer_disclosure.txt"
_FISCHER_DISCLOSURE: str = ""
if _DISCLOSURE_PATH.exists():
    _FISCHER_DISCLOSURE = _DISCLOSURE_PATH.read_text(encoding="utf-8").strip()


def _build_disclosure_html(raw: str) -> str:
    """Convert raw disclosure text into structured HTML paragraphs.

    - Strips decorative box-drawing lines (────)
    - Groups text into paragraphs on blank lines
    - Bolds section header phrases (text before first period)
    - Wraps in an email-safe centered container
    """
    lines = raw.split("\n")

    # Strip decorative box-drawing lines
    lines = [l for l in lines if not (l.strip() and all(c in "─━═" for c in l.strip()))]

    # Group into paragraphs (split on blank lines)
    paragraphs: list[str] = []
    buf: list[str] = []
    for line in lines:
        if line.strip() == "":
            if buf:
                paragraphs.append(" ".join(buf))
                buf = []
        else:
            buf.append(line.strip())
    if buf:
        paragraphs.append(" ".join(buf))

    # Build HTML <p> tags
    p_style = 'style="margin:0 0 8px 0;"'
    parts: list[str] = []
    for p in paragraphs:
        escaped = (p.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;"))

        # Title line (all-caps) — render bold (check original, not escaped)
        if p == p.upper():
            parts.append(
                f'<p {p_style}><strong>{escaped}</strong></p>')
        else:
            # Bold the section header (text up to and including first period)
            m = _re.match(r'^(.{4,40}?\.)\s', escaped)
            if m:
                header = m.group(1)
                rest = escaped[m.end() - 1:]  # keep the space
                parts.append(
                    f'<p {p_style}><strong>{header}</strong>{rest}</p>')
            else:
                parts.append(f'<p {p_style}>{escaped}</p>')

    body = "\n".join(parts)

    # Wrap in email-safe centered table matching the report's max-width.
    # Sits below the bordered box, inside its own presentation table.
    return (
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0">\n'
        '  <tr>\n'
        '    <td align="center" style="padding:0 10px 20px;">\n'
        '      <div style="max-width:960px; margin:0 auto; text-align:left;">\n'
        f'        <hr style="border:none; border-top:2px solid #000000;'
        f' margin:16px 0 12px 0;">\n'
        f'        <div style="font-size:13px; color:#000000;'
        f' line-height:1.6; font-family:{_FONT};">\n'
        f'          {body}\n'
        '        </div>\n'
        '      </div>\n'
        '    </td>\n'
        '  </tr>\n'
        '</table>\n'
    )


_DISCLOSURE_HTML: str = ""
if _FISCHER_DISCLOSURE:
    _DISCLOSURE_HTML = _build_disclosure_html(_FISCHER_DISCLOSURE)


def _wrap_fischer_document(**kwargs) -> str:
    """Wrap in standard email shell, then append the Fischer disclosure.

    Disclosure is injected between the main layout table and </body>,
    inside its own centered presentation table so it aligns with the
    report content above.
    """
    html = _wrap_document(**kwargs)
    if _DISCLOSURE_HTML:
        html = html.replace("</body>", f"{_DISCLOSURE_HTML}</body>", 1)
    return html


def _table_header(show_conviction: bool = True, intent: str = "covered_put") -> str:
    """Build HTML table header, optionally including the Conviction column."""
    conviction_th = (
        '<th style="padding:10px 12px; text-align:left;">Conviction</th>'
        if show_conviction else ""
    )
    max_profit_label = "Max Profit<br>if Put" if intent == "covered_put" else "Max Profit<br>if Called"
    return f"""
      <thead>
        <tr style="background:{_CLR_ROW_ALT}; border-bottom:2px solid {_CLR_BORDER};">
          <th style="padding:10px 12px; text-align:left;">Ticker</th>
          <th style="padding:10px 12px; text-align:left;">Spot</th>
          <th style="padding:10px 12px; text-align:left;">Strike</th>
          <th style="padding:10px 12px; text-align:left;">Expiry</th>
          <th style="padding:10px 12px; text-align:left;">Shares</th>
          <th style="padding:10px 12px; text-align:left;">Bid</th>
          <th style="padding:10px 12px; text-align:left;">P(OTM)</th>
          <th style="padding:10px 12px; text-align:left;">P(Win)</th>
          <th style="padding:10px 12px; text-align:left;">Theta($)</th>
          <th style="padding:10px 12px; text-align:left;">Dir($)</th>
          <th style="padding:10px 12px; text-align:left;">{max_profit_label}</th>
          {conviction_th}
        </tr>
      </thead>"""


def _format_expiry(expiry_str: str) -> str:
    """Format expiry '2026-03-06' → 'Fri 3/06'."""
    from datetime import datetime as _dt
    d = _dt.strptime(expiry_str[:10], "%Y-%m-%d")
    return f"{d.strftime('%a')} {d.month}/{d.strftime('%d')}"


def _build_rec_row(r: dict, show_conviction: bool = True) -> str:
    """Build a single HTML table row for a recommendation."""
    pwin_pct = r["p_win"] * 100
    potm_pct = r["p_otm"] * 100
    iv_pct = r["iv"] * 100
    ticker = r["ticker"]
    shares = r.get("shares") or _calc_shares(r["spot_at_recommend"])
    flag = r.get("flag", "clean")
    expiry_fmt = _format_expiry(r["expiry"])

    # Profit decomposition: Theta (extrinsic) + Dir (directional)
    spot = r["spot_at_recommend"]
    strike = r["strike"]
    bid = r["bid"]
    opt_type = r.get("option_type", "P")
    if opt_type == "P":
        intrinsic = max(strike - spot, 0)
        dir_per_share = spot - strike
    else:
        intrinsic = max(spot - strike, 0)
        dir_per_share = strike - spot
    theta_per_share = bid - intrinsic  # extrinsic value
    theta_total = theta_per_share * shares
    dir_total = dir_per_share * shares
    max_profit_total = r.get("max_profit_per_share", 0) * shares

    if flag == "deferred":
        row_style = f'border-bottom:1px solid {_CLR_BORDER}; background:{CLR_YELLOW};'
        pwin_style = 'padding:10px 12px; font-weight:600;'
    elif flag == "near_miss":
        row_style = f'border-bottom:1px solid {_CLR_BORDER};'
        pwin_style = f'padding:10px 12px; font-weight:600; background:{CLR_YELLOW};'
    else:
        row_style = f'border-bottom:1px solid {_CLR_BORDER};'
        pwin_style = 'padding:10px 12px; font-weight:600;'

    conviction_td = (
        f'<td style="padding:10px 12px;">{r["nenner_score"]}</td>'
        if show_conviction else ""
    )

    return f"""
        <tr style="{row_style}">
            <td style="padding:10px 12px; font-weight:700;">{ticker}</td>
            <td style="padding:10px 12px;">${spot:.2f}</td>
            <td style="padding:10px 12px; font-weight:600;">${strike:.2f}</td>
            <td style="padding:10px 12px;">{expiry_fmt}</td>
            <td style="padding:10px 12px;">{shares:,}</td>
            <td style="padding:10px 12px;">${bid:.2f}</td>
            <td style="padding:10px 12px;">{potm_pct:.1f}%</td>
            <td style="{pwin_style}">{pwin_pct:.1f}%</td>
            <td style="padding:10px 12px; font-weight:700; color:{_CLR_GREEN};">
                ${theta_total:,.0f}</td>
            <td style="padding:10px 12px; font-weight:700; color:{_CLR_GREEN};">
                ${dir_total:,.0f}</td>
            <td style="padding:10px 12px; font-weight:700; color:{_CLR_GREEN};">
                ${max_profit_total:,.0f}</td>
            {conviction_td}
        </tr>"""


def _sort_recs(recs: list[dict], display_order: tuple[str, ...] | None = None) -> list[dict]:
    """Sort recs by total max profit (per-share × shares) descending."""
    def _total_max_profit(r: dict) -> float:
        shares = _calc_shares(r.get("spot_at_recommend", 0))
        return r.get("max_profit_per_share", 0) * shares
    return sorted(recs, key=_total_max_profit, reverse=True)


def _build_section(
    recs: list[dict],
    weekly_recs: list[dict] | None,
    section_title: str,
    entry_note: str,
    strike_label: str,
    intent: str,
    show_conviction: bool = True,
    display_order: tuple[str, ...] | None = None,
) -> str:
    """Build HTML for one intent section (short-term + weekly tables)."""
    header = _table_header(show_conviction, intent=intent)
    sorted_recs = _sort_recs(recs, display_order)
    rows_html = "".join(_build_rec_row(r, show_conviction) for r in sorted_recs)

    weekly_section = ""
    if weekly_recs:
        sorted_weekly = _sort_recs(weekly_recs, display_order)
        weekly_rows_html = "".join(_build_rec_row(r, show_conviction) for r in sorted_weekly)
        if weekly_rows_html:
            weekly_section = f"""
    <h3 style="color:{_CLR_HEADER}; font-size:16px; margin:14px 0 4px 0;">
      Weekly Expiry (7&ndash;14 DTE)</h3>
    <div style="overflow-x:auto;">
    <table style="width:100%; border-collapse:collapse; font-size:13px;
                  color:{_CLR_TEXT};">
      {header}
      <tbody>
        {weekly_rows_html}
      </tbody>
    </table>
    </div>"""

    return f"""
    <h2 style="color:{_CLR_HEADER}; font-size:18px; margin:20px 0 4px 0;">
      {section_title}</h2>
    <p style="color:{_CLR_MUTED}; font-size:12px; margin:0 0 6px 0;">
      {entry_note} | Strike = {strike_label} strike |
      P(Win) band: {_rules(intent)["min_p_win"]:.1%} &ndash; {_rules(intent)["max_p_win"]:.0%}
    </p>

    <h3 style="color:{_CLR_HEADER}; font-size:16px; margin:8px 0 4px 0;">
      Short-Term Expiry (0&ndash;8 DTE)</h3>
    <div style="overflow-x:auto;">
    <table style="width:100%; border-collapse:collapse; font-size:13px;
                  color:{_CLR_TEXT};">
      {header}
      <tbody>
        {rows_html}
      </tbody>
    </table>
    </div>

    {weekly_section}"""



def _build_unified_email(
    sections_data: dict,
    failed_tickers: list[str],
    slot_label: str = "Opening Scan",
) -> str:
    """Build single HTML email with 4 sections: Put/Call × Short/Weekly DTE.

    Parameters
    ----------
    sections_data : dict mapping ReportSection → list of rec dicts
    failed_tickers : tickers where chain data was unavailable
    slot_label : e.g. "Opening Scan", "Midday Scan"
    """
    today = date.today().strftime("%B %d, %Y")

    sections_html = ""
    for section in REPORT_SECTIONS:
        recs = sections_data.get(section, [])
        if not recs:
            continue

        if sections_html:
            sections_html += (
                f'<hr style="border:none; border-top:2px solid {_CLR_BORDER};'
                f' margin:24px 0 8px 0;">'
            )

        header = _table_header(show_conviction=False, intent=section.intent)
        sorted_recs = _sort_recs(recs)
        rows_html = "".join(_build_rec_row(r, show_conviction=False) for r in sorted_recs)

        sections_html += f"""
    <h2 style="color:{_CLR_HEADER}; font-size:18px; margin:20px 0 4px 0;">
      {section.title}</h2>
    <p style="color:{_CLR_MUTED}; font-size:12px; margin:0 0 6px 0;">
      {section.entry_note} | Strike = {section.strike_label} strike |
      P(Win) floor: {_rules(section.intent)["min_p_win"]:.0%}
    </p>
    <div style="overflow-x:auto;">
    <table style="width:100%; border-collapse:collapse; font-size:13px;
                  color:{_CLR_TEXT};">
      {header}
      <tbody>{rows_html}</tbody>
    </table>
    </div>"""

    # Failed tickers notice
    failed_notice = ""
    if failed_tickers:
        failed_notice = f"""
    <p style="color:{_CLR_RED}; font-size:13px; font-weight:600; margin:12px 0 4px 0;">
      Unable to retrieve pricing for: {", ".join(failed_tickers)}
    </p>"""

    legend = f"""
    <div style="margin-top:16px; padding:14px 20px; font-size:13px;
                color:{_CLR_TEXT}; line-height:1.6;">
      <strong style="font-size:14px;">Legend</strong><br>
      <strong>Spot</strong> = price at scan &nbsp;|&nbsp;
      <strong>Strike</strong> = option strike &nbsp;|&nbsp;
      <strong>Shares</strong> = position size &nbsp;|&nbsp;
      <strong>Bid</strong> = option bid per share<br>
      <strong>IV</strong> = implied volatility &nbsp;|&nbsp;
      <strong>P(OTM)</strong> = probability expires worthless (&le;55%) &nbsp;|&nbsp;
      <strong>P(Win)</strong> = probability of profit at expiry<br>
      <strong>Theta($)</strong> = extrinsic (time-decay) income &nbsp;|&nbsp;
      <strong>Dir($)</strong> = directional profit if assigned &nbsp;|&nbsp;
      <strong>Max Profit</strong> = theta + directional combined (shares &times; max profit/sh)<br>
      <span style="background:{CLR_YELLOW}; padding:1px 5px;">Yellow row</span> = no 0DTE strike in band, deferred to later expiry &nbsp;|&nbsp;
      <span style="background:{CLR_YELLOW}; padding:1px 5px;">Yellow P(Win)</span> = no strike in band, showing closest match
    </div>"""

    return _wrap_fischer_document(
        body_html=f"{sections_html}{failed_notice}",
        title="Fischer Daily Scan",
        subtitle=f"{slot_label} &mdash; {today}",
        footer_text="Generated by Fischer Options Engine",
        notes_html=legend,
        max_width=960,
    )


def _build_settlement_email(settlements: list[dict]) -> str:
    """Build HTML email for settlement results."""
    today = date.today().strftime("%B %d, %Y")

    total_pnl = sum(s["pnl_total"] for s in settlements)
    total_color = _CLR_GREEN if total_pnl >= 0 else _CLR_RED

    rows_html = ""
    for s in settlements:
        pnl_color = _CLR_GREEN if s["pnl_total"] >= 0 else _CLR_RED
        status = "ITM (Assigned)" if s["itm"] else "OTM (Expired)"

        rows_html += f"""
        <tr style="border-bottom: 1px solid {_CLR_BORDER};">
            <td style="padding:10px 12px; font-weight:700;">{s['ticker']}</td>
            <td style="padding:10px 12px;">${s['strike']:.2f}</td>
            <td style="padding:10px 12px;">{s['expiry']}</td>
            <td style="padding:10px 12px;">${s['entry']:.2f}</td>
            <td style="padding:10px 12px;">${s['premium']:.2f}</td>
            <td style="padding:10px 12px;">${s['close_price']:.2f}</td>
            <td style="padding:10px 12px;">{status}</td>
            <td style="padding:10px 12px; font-weight:700; color:{pnl_color};">
                ${s['pnl_per_share']:+.2f}</td>
            <td style="padding:10px 12px; font-weight:700; color:{pnl_color};">
                ${s['pnl_total']:+,.2f}</td>
        </tr>"""

    content = f"""
    <p style="font-size:16px; font-weight:700; color:{total_color};">
      Net P&amp;L (per contract): ${total_pnl:+,.2f}
    </p>

    <div style="overflow-x:auto;">
    <table style="width:100%; border-collapse:collapse; font-size:13px;
                  color:{_CLR_TEXT};">
      <thead>
        <tr style="background:{_CLR_ROW_ALT}; border-bottom:2px solid {_CLR_BORDER};">
          <th style="padding:10px 12px; text-align:left;">Ticker</th>
          <th style="padding:10px 12px; text-align:left;">Strike</th>
          <th style="padding:10px 12px; text-align:left;">Expiry</th>
          <th style="padding:10px 12px; text-align:left;">Entry</th>
          <th style="padding:10px 12px; text-align:left;">Premium</th>
          <th style="padding:10px 12px; text-align:left;">Close</th>
          <th style="padding:10px 12px; text-align:left;">Status</th>
          <th style="padding:10px 12px; text-align:left;">P&amp;L/sh</th>
          <th style="padding:10px 12px; text-align:left;">P&amp;L/ct</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
    </div>"""

    return _wrap_fischer_document(
        body_html=content,
        title="Fischer Settlement Report",
        subtitle=f"Expiry Settlements &mdash; {today}",
        footer_text="Generated by Fischer Options Engine",
        max_width=900,
    )


# ---------------------------------------------------------------------------
# Orchestrators (called by email_scheduler)
# ---------------------------------------------------------------------------

def _send_offline_alert(send_email, config: ScanSlotConfig):
    """Send a short alert email when DataCenter appears offline (all tickers failed)."""
    today_fmt = date.today().strftime('%b %d')
    subject = f"\u26a0 Fischer Daily Scan \u2014 No Data \u2014 {today_fmt}"
    body = _wrap_document(
        body_html=f"""
        <p style="font-size:15px; color:{_CLR_RED}; font-weight:700;">
          DataCenter Offline \u2014 No Option Chain Data
        </p>
        <p style="font-size:13px; color:{_CLR_TEXT};">
          The {config.label} scan for all {len(SCAN_TICKERS)} tickers
          returned no pricing data.<br>
          This usually means Nenner_DataCenter.xlsm is not open or
          Thomson One RTD feed is disconnected.
        </p>
        <p style="font-size:13px; color:{_CLR_MUTED};">
          Tickers attempted: {", ".join(SCAN_TICKERS)}
        </p>""",
        title="Fischer Options",
        subtitle=f"Data Alert &mdash; {date.today().strftime('%B %d, %Y')}",
        footer_text="Generated by Fischer Options Engine",
        max_width=600,
    )
    send_email(subject, body, to_addr=REPORT_TO)
    log.warning("Fischer: sent DataCenter offline alert")


def _scan_all_tickers(
    conn: sqlite3.Connection,
    slot: ScanSlot,
) -> tuple[dict, list[str]]:
    """Scan all 17 tickers for both intents at both DTE ranges.

    Returns
    -------
    results : dict keyed by (ticker, intent, dte_label) -> rec dict
    failed  : list of tickers where no chain data was available
    """
    # Phase 0: bulk pre-load put + call chains from OptionChains.xlsm
    from .fischer_chain import read_all_chains
    bulk_put_chains: dict = {}
    bulk_call_chains: dict = {}
    try:
        bulk_put_chains, bulk_call_chains = read_all_chains()
        log.info(f"Fischer {slot}: bulk-loaded {len(bulk_put_chains)} put + "
                 f"{len(bulk_call_chains)} call chains from OptionChains")
    except Exception as e:
        log.warning(f"Fischer {slot}: OptionChains bulk read failed ({e}), falling back to Options_RT")

    results = {}
    failed = set()

    for section in REPORT_SECTIONS:
        intent = section.intent
        dte_range = section.dte_range
        dte_label = f"{dte_range[0]}-{dte_range[1]}"
        scan_slot = slot if dte_range[1] <= 7 else f"{slot}_weekly"

        # Clear any prior results for this slot/intent so we always scan fresh
        today_str = date.today().isoformat()
        conn.execute(
            "DELETE FROM fischer_recommendations "
            "WHERE report_date = ? AND scan_slot = ? AND intent = ?",
            (today_str, scan_slot, intent)
        )
        conn.commit()

        recs_to_store = []
        for ticker in SCAN_TICKERS:
            log.info(f"Fischer {slot}/{intent}/{dte_label}: scanning {ticker}...")

            # Use bulk-loaded data from OptionChains when available
            preloaded = None
            if intent == "covered_put" and ticker in bulk_put_chains:
                preloaded = bulk_put_chains[ticker]
            elif intent == "covered_call" and ticker in bulk_call_chains:
                preloaded = bulk_call_chains[ticker]

            try:
                ranked = _scan_ticker(ticker, dte_range=dte_range, intent=intent, chain_data=preloaded)
                if not ranked:
                    log.info(f"  {ticker}: no chain data")
                    failed.add(ticker)
                    continue

                result = _select_best_candidate(ranked, ticker, intent=intent)
                if result:
                    ev, flag = result
                    # Min profit threshold: 1% of stock value
                    if ev.spot and ev.max_profit_per_share / ev.spot < 0.01:
                        log.info(f"  {ticker}: max profit {ev.max_profit_per_share:.2f}/"
                                 f"{ev.spot:.2f} = {ev.max_profit_per_share/ev.spot:.2%}"
                                 f" < 1% threshold, skipping")
                        continue
                    rec = {
                        "report_date": today_str,
                        "ticker": ticker,
                        "strike": ev.strike,
                        "expiry": str(ev.expiry),
                        "option_type": ev.option_type,
                        "bid": ev.bid,
                        "ask": ev.ask,
                        "iv": ev.iv,
                        "delta": ev.delta,
                        "p_otm": ev.p_expire_worthless,
                        "p_win": ev.p_profit,
                        "max_profit_per_share": ev.max_profit_per_share,
                        "net_ev_per_contract": ev.net_ev_per_contract,
                        "nenner_score": ev.nenner_score,
                        "spot_at_recommend": ev.spot,
                        "entry_price": ev.entry_used,
                        "premium_per_share": ev.bid,
                        "premium_ratio": ev.premium_ratio,
                        "theta_per_share": ev.theta,
                        "flag": flag,
                    }
                    results[(ticker, intent, dte_label)] = rec
                    recs_to_store.append(rec)

            except Exception as e:
                log.error(f"  {ticker}: scan failed: {e}", exc_info=True)
                failed.add(ticker)

        # Store in DB
        if recs_to_store:
            recs_to_store.sort(key=lambda r: r["max_profit_per_share"], reverse=True)
            for rank, rec in enumerate(recs_to_store, 1):
                conn.execute("""
                    INSERT INTO fischer_recommendations
                    (report_date, ticker, strike, expiry, option_type, bid, ask, iv,
                     delta, p_otm, p_win, max_profit_per_share, net_ev_per_contract,
                     nenner_score, spot_at_recommend, entry_price, premium_per_share,
                     rank, scan_slot, intent, premium_ratio, theta_per_share)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rec["report_date"], rec["ticker"], rec["strike"], rec["expiry"],
                    rec["option_type"], rec["bid"], rec["ask"], rec["iv"],
                    rec["delta"], rec["p_otm"], rec["p_win"], rec["max_profit_per_share"],
                    rec["net_ev_per_contract"], rec["nenner_score"], rec["spot_at_recommend"],
                    rec["entry_price"], rec["premium_per_share"], rank, scan_slot, intent,
                    rec.get("premium_ratio"), rec.get("theta_per_share"),
                ))
            conn.commit()
            log.info(f"Fischer {slot}/{intent}/{dte_label}: stored {len(recs_to_store)} recs")

    return results, list(failed)


def _assemble_sections(
    all_results: dict,
) -> dict:
    """For each of 4 report sections, pick 10 always + 5 best macro tickers.

    Returns dict keyed by ReportSection -> list of rec dicts (up to 15).
    """
    sections_data = {}
    for section in REPORT_SECTIONS:
        intent = section.intent
        dte_label = f"{section.dte_range[0]}-{section.dte_range[1]}"

        # Always tickers (10)
        always_recs = []
        for t in ALWAYS_TICKERS:
            key = (t, intent, dte_label)
            if key in all_results:
                always_recs.append(all_results[key])

        # Macro selection (7 → pick 5)
        macro_candidates = {}
        for t in MACRO_POOL:
            key = (t, intent, dte_label)
            if key in all_results:
                macro_candidates[t] = all_results[key]

        selected_macro = _select_macro_tickers(macro_candidates, intent=intent)
        macro_recs = [macro_candidates[t] for t in selected_macro]

        sections_data[section] = always_recs + macro_recs

    return sections_data


def _send_to_subscribers(conn: sqlite3.Connection, subject: str, html: str):
    """Send the same scan HTML to all active Fischer subscribers."""
    try:
        from .fischer_subscribers import get_all_active_subscribers
        from .postmaster import send_email as _send

        subscribers = get_all_active_subscribers(conn)
        for sub in subscribers:
            try:
                _send(subject, html, to_addr=sub["email"])
                log.info(f"Fischer scan sent to subscriber {sub['email']}")
            except Exception as e:
                log.error(f"Fischer scan send to {sub['email']} failed: {e}")
    except Exception as e:
        log.error(f"Fischer subscriber distribution failed: {e}", exc_info=True)


def send_scan_report(db_path: str, slot: ScanSlot = "opening"):
    """Generate unified Fischer report: 4 sections, one email.

    Scans all 17 tickers for both intents at both DTE ranges,
    assembles sections with macro selection, and sends a single email.

    Opens its own DB connection. Safe to call from scheduler thread.
    """
    from .db import init_db, migrate_db
    from .postmaster import send_email

    config = SCAN_SLOTS[slot]

    # Check result cache (if reliability layer active)
    rel = None
    try:
        from .fischer_reliability import FischerReliability
        rel = FischerReliability.get_instance()
    except ImportError:
        pass

    if rel and rel.cache:
        today_str = date.today().isoformat()
        cache_key = f"scan_{slot}"
        cached = rel.cache.get(cache_key, today_str)
        if cached:
            log.info(f"Fischer {slot}: serving from cache")
            today_fmt = date.today().strftime('%b %d')
            subject = f"Fischer Daily Scan \u2014 {config.label} \u2014 {today_fmt}"
            send_email(subject, cached.result_html, to_addr=REPORT_TO)
            from .db import init_db
            cache_conn = init_db(db_path)
            _send_to_subscribers(cache_conn, subject, cached.result_html)
            cache_conn.close()
            return

    conn = None
    try:
        conn = init_db(db_path)
        migrate_db(conn)

        # Phase 1: Scan all tickers
        all_results, failed_tickers = _scan_all_tickers(conn, slot)

        if not all_results:
            log.warning(f"Fischer {slot}: no results — all tickers failed")
            _send_offline_alert(send_email, config)
            return

        # Scan guard: abort if too many tickers failed
        if rel and rel.scan_guard:
            if rel.scan_guard.check_abort(failed_tickers, slot):
                return

        # Phase 2: Assemble 4 sections with macro selection
        sections_data = _assemble_sections(all_results)

        # Phase 3: Build and send single email
        html = _build_unified_email(sections_data, failed_tickers, config.label)
        today_fmt = date.today().strftime('%b %d')
        subject = f"Fischer Daily Scan \u2014 {config.label} \u2014 {today_fmt}"
        send_email(subject, html, to_addr=REPORT_TO)

        # Phase 4: Send to active subscribers
        _send_to_subscribers(conn, subject, html)

        # Store in cache
        if rel and rel.cache:
            rel.cache.put(f"scan_{slot}", date.today().isoformat(), html)

        total_recs = sum(len(recs) for recs in sections_data.values())
        log.info(f"Fischer {slot}: unified report sent ({total_recs} recs across 4 sections)")

    except Exception as e:
        log.error(f"Fischer {slot} report failed: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


def send_daily_report(db_path: str):
    """Legacy entry point — runs the opening scan."""
    send_scan_report(db_path, slot="opening")




def _generate_with_ticker_tracking(
    conn: sqlite3.Connection,
    slot: ScanSlot = "opening",
    dte_range: tuple[int, int] | None = None,
    intent: str = "covered_put",
    tickers: tuple[str, ...] | None = None,
) -> tuple[list[dict], list[str]]:
    """Scan tickers for a given scan slot and intent, store recs in DB.

    If tickers is provided, scans only those tickers (for group-based reports).
    """
    scan_list = tickers or tuple(SCAN_TICKERS)
    today_str = date.today().isoformat()

    # Clear any prior results for this slot/intent so we always scan fresh
    conn.execute(
        "DELETE FROM fischer_recommendations "
        "WHERE report_date = ? AND scan_slot = ? AND intent = ?",
        (today_str, slot, intent)
    )
    conn.commit()

    all_candidates = []  # list of (ticker, EVResult, flag)
    failed_tickers = []  # tickers where pricing was unavailable
    # flag: "clean" = 0DTE in band, "deferred" = later expiry in band, "near_miss" = closest to band

    for ticker in scan_list:
        log.info(f"Fischer {slot}/{intent}: scanning {ticker}...")
        try:
            ranked = _scan_ticker(ticker, dte_range=dte_range, intent=intent)
            if not ranked:
                log.info(f"  {ticker}: no chain data")
                failed_tickers.append(ticker)
                continue

            result = _select_best_candidate(ranked, ticker, intent=intent)
            if result:
                best, flag = result
                if flag == "clean":
                    log.info(f"  {ticker}: 0DTE K={best.strike} "
                             f"MaxP=${best.max_profit_per_share:.2f} "
                             f"P(Win)={best.p_profit:.1%}")
                elif flag == "deferred":
                    dte = (best.expiry - date.today()).days
                    log.info(f"  {ticker}: deferred to {dte}DTE K={best.strike} "
                             f"MaxP=${best.max_profit_per_share:.2f} "
                             f"P(Win)={best.p_profit:.1%}")
                else:
                    log.info(f"  {ticker}: near miss K={best.strike} "
                             f"P(Win)={best.p_profit:.1%} (outside band)")
                all_candidates.append((ticker, best, flag))

        except Exception as e:
            log.error(f"  {ticker}: scan failed: {e}", exc_info=True)
            failed_tickers.append(ticker)

    if not all_candidates:
        return [], failed_tickers

    # Rank across tickers by max profit per share
    all_candidates.sort(key=lambda t: t[1].max_profit_per_share, reverse=True)
    top = all_candidates[:TOP_N]

    # Store in DB
    recs = []
    for rank, (ticker, ev, flag) in enumerate(top, 1):
        rec = {
            "report_date": today_str,
            "ticker": ticker,
            "strike": ev.strike,
            "expiry": str(ev.expiry),
            "option_type": ev.option_type,
            "bid": ev.bid,
            "ask": ev.ask,
            "iv": ev.iv,
            "delta": ev.delta,
            "p_otm": ev.p_expire_worthless,
            "p_win": ev.p_profit,
            "max_profit_per_share": ev.max_profit_per_share,
            "net_ev_per_contract": ev.net_ev_per_contract,
            "nenner_score": ev.nenner_score,
            "spot_at_recommend": ev.spot,
            "entry_price": ev.entry_used,
            "premium_per_share": ev.bid,
            "premium_ratio": ev.premium_ratio,
            "theta_per_share": ev.theta,
            "rank": rank,
            "flag": flag,
        }

        conn.execute("""
            INSERT INTO fischer_recommendations
            (report_date, ticker, strike, expiry, option_type, bid, ask, iv,
             delta, p_otm, p_win, max_profit_per_share, net_ev_per_contract,
             nenner_score, spot_at_recommend, entry_price, premium_per_share,
             rank, scan_slot, intent, premium_ratio, theta_per_share)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rec["report_date"], rec["ticker"], rec["strike"], rec["expiry"],
            rec["option_type"], rec["bid"], rec["ask"], rec["iv"],
            rec["delta"], rec["p_otm"], rec["p_win"], rec["max_profit_per_share"],
            rec["net_ev_per_contract"], rec["nenner_score"], rec["spot_at_recommend"],
            rec["entry_price"], rec["premium_per_share"], rec["rank"], slot, intent,
            rec.get("premium_ratio"), rec.get("theta_per_share"),
        ))
        recs.append(rec)

    conn.commit()
    log.info(f"Fischer {slot}/{intent}: stored {len(recs)} short-term recs for {today_str}")
    return recs, failed_tickers


def _generate_weekly_recs(
    conn: sqlite3.Connection | None = None,
    slot: ScanSlot = "opening",
    intent: str = "covered_put",
    tickers: tuple[str, ...] | None = None,
) -> tuple[list[dict], list[str]]:
    """Scan tickers for weekly (7-14 DTE) strikes.

    When conn is provided, stores recs in fischer_recommendations with scan_slot.
    Otherwise returns them as informational-only dicts.
    If tickers is provided, scans only those tickers (for group-based reports).
    """
    scan_list = tickers or tuple(SCAN_TICKERS)
    weekly_candidates = []
    failed_tickers = []

    for ticker in scan_list:
        log.info(f"Fischer {slot}/{intent} weekly: scanning {ticker}...")
        try:
            ranked = _scan_ticker(ticker, dte_range=(7, 14), intent=intent)
            if not ranked:
                log.info(f"  {ticker}: no weekly chain data")
                failed_tickers.append(ticker)
                continue

            R = _rules(intent)
            in_band = [r for r in ranked
                       if R["min_p_win"] <= r.p_profit <= R["max_p_win"]
                       and r.p_expire_worthless <= R["max_p_otm"]
                       and r.premium_ratio is not None
                       and R["min_ratio"] <= r.premium_ratio <= R["max_ratio"]]

            if in_band:
                best = max(in_band, key=lambda r: r.max_profit_per_share)
                flag = "clean"
                dte = (best.expiry - date.today()).days
                log.info(f"  {ticker}: weekly {dte}DTE K={best.strike} "
                         f"MaxP=${best.max_profit_per_share:.2f} "
                         f"P(Win)={best.p_profit:.1%}")
            else:
                log.info(f"  {ticker}: weekly no strike in ratio band, skipping")
                continue

            # Min profit threshold: 1% of stock value
            if best.spot and best.max_profit_per_share / best.spot < 0.01:
                log.info(f"  {ticker}: weekly max profit "
                         f"{best.max_profit_per_share/best.spot:.2%}"
                         f" < 1% threshold, skipping")
                continue

            rec = {
                "ticker": ticker,
                "strike": best.strike,
                "expiry": str(best.expiry),
                "option_type": best.option_type,
                "bid": best.bid,
                "ask": best.ask,
                "iv": best.iv,
                "delta": best.delta,
                "p_otm": best.p_expire_worthless,
                "p_win": best.p_profit,
                "max_profit_per_share": best.max_profit_per_share,
                "net_ev_per_contract": best.net_ev_per_contract,
                "nenner_score": best.nenner_score,
                "spot_at_recommend": best.spot,
                "entry_price": best.entry_used,
                "premium_per_share": best.bid,
                "premium_ratio": best.premium_ratio,
                "theta_per_share": best.theta,
                "flag": flag,
            }
            weekly_candidates.append(rec)

        except Exception as e:
            log.error(f"  {ticker}: weekly scan failed: {e}", exc_info=True)
            failed_tickers.append(ticker)

    # Sort by max profit per share
    weekly_candidates.sort(key=lambda r: r["max_profit_per_share"], reverse=True)

    # Persist weekly recs when conn is provided
    if conn is not None and weekly_candidates:
        today_str = date.today().isoformat()
        for rank, rec in enumerate(weekly_candidates, 1):
            conn.execute("""
                INSERT INTO fischer_recommendations
                (report_date, ticker, strike, expiry, option_type, bid, ask, iv,
                 delta, p_otm, p_win, max_profit_per_share, net_ev_per_contract,
                 nenner_score, spot_at_recommend, entry_price, premium_per_share,
                 rank, scan_slot, intent, premium_ratio, theta_per_share)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                today_str, rec["ticker"], rec["strike"], rec["expiry"],
                rec["option_type"], rec["bid"], rec["ask"], rec["iv"],
                rec["delta"], rec["p_otm"], rec["p_win"],
                rec["max_profit_per_share"], rec["net_ev_per_contract"],
                rec["nenner_score"], rec["spot_at_recommend"],
                rec["entry_price"], rec["premium_per_share"],
                rank, f"{slot}_weekly", intent,
                rec.get("premium_ratio"), rec.get("theta_per_share"),
            ))
        conn.commit()
        log.info(f"Fischer {slot}/{intent}: stored {len(weekly_candidates)} weekly recs")

    log.info(f"Fischer {slot}/{intent} weekly: {len(weekly_candidates)} results")
    return weekly_candidates, failed_tickers


# ---------------------------------------------------------------------------
# On-Demand Fresh Scan (no DB dedup, no DB storage)
# ---------------------------------------------------------------------------

def _select_best_candidate(ranked: list, ticker: str, intent: str = "covered_put") -> tuple | None:
    """Pick the best EVResult from ranked results using P(Win) band filtering.

    Returns (ev, flag) or None if no results.
    Flag: "clean" = 0DTE in band, "deferred" = later expiry in band,
          "near_miss" = closest to band.
    """
    if not ranked:
        return None

    _log_debug_strikes(ticker, intent, ranked)
    R = _rules(intent)
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


def generate_fresh_scan(
    tickers: tuple[str, ...],
    share_alloc: dict[str, int] | None = None,
    show_conviction: bool = False,
) -> tuple[list[dict], list[dict], list[dict], list[dict], list[str]]:
    """Generate a FRESH live scan for arbitrary tickers — no DB dedup or storage.

    Used for on-demand subscriber refreshes.  Runs both covered puts and calls,
    plus weekly 7-14 DTE for each.

    Parameters
    ----------
    tickers : tuple of equity/ETF tickers to scan
    share_alloc : optional custom {ticker: shares} mapping (falls back to _calc_shares)
    show_conviction : whether the report should include Nenner conviction column

    Returns
    -------
    (put_recs, call_recs, put_weekly, call_weekly, failed_tickers)
    """
    alloc = share_alloc or {}

    def _scan_intent(intent: str, dte_range=None) -> tuple[list[dict], list[str]]:
        candidates = []
        failed = []
        for ticker in tickers:
            log.info(f"Fischer refresh/{intent}: scanning {ticker}...")
            try:
                ranked = _scan_ticker(ticker, dte_range=dte_range, intent=intent)
                if not ranked:
                    log.info(f"  {ticker}: no chain data")
                    failed.append(ticker)
                    continue

                result = _select_best_candidate(ranked, ticker, intent=intent)
                if result:
                    ev, flag = result
                    # Min profit threshold: 1% of stock value
                    if ev.spot and ev.max_profit_per_share / ev.spot < 0.01:
                        log.info(f"  {ticker}: max profit {ev.max_profit_per_share/ev.spot:.2%}"
                                 f" < 1% threshold, skipping")
                        continue
                    candidates.append((ticker, ev, flag))
            except Exception as e:
                log.error(f"  {ticker}: scan failed: {e}", exc_info=True)
                failed.append(ticker)

        candidates.sort(key=lambda t: t[1].max_profit_per_share, reverse=True)

        recs = []
        for rank, (ticker, ev, flag) in enumerate(candidates, 1):
            recs.append({
                "report_date": date.today().isoformat(),
                "ticker": ticker,
                "strike": ev.strike,
                "expiry": str(ev.expiry),
                "option_type": ev.option_type,
                "bid": ev.bid,
                "ask": ev.ask,
                "iv": ev.iv,
                "delta": ev.delta,
                "p_otm": ev.p_expire_worthless,
                "p_win": ev.p_profit,
                "max_profit_per_share": ev.max_profit_per_share,
                "net_ev_per_contract": ev.net_ev_per_contract,
                "nenner_score": ev.nenner_score,
                "spot_at_recommend": ev.spot,
                "entry_price": ev.entry_used,
                "premium_per_share": ev.bid,
                "premium_ratio": ev.premium_ratio,
                "theta_per_share": ev.theta,
                "rank": rank,
                "flag": flag,
                "shares": alloc.get(ticker) or _calc_shares(ev.spot),
            })
        return recs, failed

    # Short-term puts and calls
    put_recs, put_failed = _scan_intent("covered_put")
    call_recs, call_failed = _scan_intent("covered_call")

    # Weekly 7-14 DTE puts and calls (no DB storage — pass conn=None)
    put_weekly, pw_failed = _generate_weekly_recs(
        conn=None, slot="refresh", intent="covered_put", tickers=tickers)
    call_weekly, cw_failed = _generate_weekly_recs(
        conn=None, slot="refresh", intent="covered_call", tickers=tickers)

    # Merge failed tickers (unique)
    all_failed = list(dict.fromkeys(put_failed + call_failed + pw_failed + cw_failed))

    return put_recs, call_recs, put_weekly, call_weekly, all_failed


def send_settlement_report(db_path: str):
    """Settle expired trades and email the results.

    Opens its own DB connection. Safe to call from scheduler thread.
    """
    from .db import init_db, migrate_db
    from .postmaster import send_email

    conn = None
    try:
        conn = init_db(db_path)
        migrate_db(conn)

        settlements = settle_expired_trades(conn)
        if not settlements:
            return

        html = _build_settlement_email(settlements)
        subject = f"Fischer Settlement: {len(settlements)} trades — {date.today().strftime('%b %d')}"
        send_email(subject, html, to_addr=REPORT_TO)
        log.info(f"Fischer settlement report sent to {REPORT_TO}")

    except Exception as e:
        log.error(f"Fischer settlement report failed: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
