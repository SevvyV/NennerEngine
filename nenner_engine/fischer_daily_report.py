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
    "AAPL", "AMZN", "AVGO", "GOOGL", "IWM",
    "META", "MSFT", "NVDA", "QQQ", "TSLA",
)

# Macro pool — top 5 selected per section by premium:directional ratio (7 tickers)
MACRO_POOL: tuple[str, ...] = (
    "GLD", "IBIT", "SLV", "SPY", "TLT", "UNG", "USO",
)

MACRO_PICKS = 5

# Flat list of all tickers (used by settlement, scanning, etc.)
SCAN_TICKERS: list[str] = list(ALWAYS_TICKERS) + list(MACRO_POOL)

CAPITAL = 500_000
TOP_N = 99  # show all tickers per group (no limit)
MIN_P_WIN = 0.55   # minimum P(Win) to qualify
MAX_P_WIN = 0.70   # maximum P(Win) — exclude deep ITM / penny premium
MAX_P_OTM = 0.55   # maximum P(OTM) — reject if too likely to expire worthless

# Premium:Directional ratio band for macro ranking
MIN_RATIO = 1.0    # below 1:1 = just shorting stock with extra steps
MAX_RATIO = 3.0    # above 3:1 = pure premium harvesting, no directional edge

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
    ReportSection("covered_call", (0, 7),
                  "Covered Calls 0\u20137 DTE",
                  "Entry assumes long stock at current spot", "call"),
    ReportSection("covered_call", (8, 14),
                  "Covered Calls 8\u201314 DTE",
                  "Entry assumes long stock at current spot", "call"),
)

# Share allocations per ticker — ~$500K notional, rounded to nearest 100
# (contracts = shares / 100, creating a true covered put/call)
# Exception: UNG reduced (contango vehicle)
SHARE_ALLOC: dict[str, int] = {
    # --- Always Shown (10) ---
    "AAPL":  1_800,   # ~$265 × 1800  = $477K
    "AMZN":  2_100,   # ~$230 × 2100  = $483K
    "AVGO":  2_200,   # ~$220 × 2200  = $484K
    "GOOGL": 1_600,   # ~$307 × 1600  = $491K
    "IWM":   2_200,   # ~$225 × 2200  = $495K
    "META":    700,   # ~$685 × 700   = $480K
    "MSFT":  1_200,   # ~$393 × 1200  = $472K
    "NVDA":  2_800,   # ~$179 × 2800  = $501K
    "QQQ":     900,   # ~$520 × 900   = $468K
    "TSLA":  1_200,   # ~$399 × 1200  = $479K
    # --- Macro Pool (7) ---
    "GLD":   1_800,   # ~$268 × 1800  = $482K
    "IBIT":  9_000,   # ~$55  × 9000  = $495K
    "SLV":  16_000,   # ~$30  × 16000 = $480K
    "SPY":     800,   # ~$590 × 800   = $472K
    "TLT":   5_500,   # ~$90  × 5500  = $495K
    "UNG":  20_000,   # ~$12  × 20000 = $240K  (reduced — contango)
    "USO":   6_000,   # ~$82  × 6000  = $492K
}


def _select_macro_tickers(
    macro_results: dict[str, dict],
    n: int = MACRO_PICKS,
) -> list[str]:
    """Rank macro pool tickers by premium_ratio quality, return top N.

    Filters to ratio within [MIN_RATIO, MAX_RATIO] and P(Win) >= MIN_P_WIN.
    If fewer than N qualify, returns all that qualify.
    """
    eligible = []
    for ticker, rec in macro_results.items():
        ratio = rec.get("premium_ratio")
        if ratio is None:
            continue
        if not (MIN_RATIO <= ratio <= MAX_RATIO):
            continue
        if rec.get("p_win", 0) < MIN_P_WIN:
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
) -> list:
    """Run the Fischer EV pipeline for one ticker, return ranked EVResults.

    Parameters
    ----------
    ticker : equity/ETF ticker to scan
    dte_range : optional (min_dte, max_dte) inclusive filter.
                If None, uses conviction-based max_dte with min=0.
    intent : "covered_put" or "covered_call"
    """
    from .fischer_engine import (
        compute_ev, implied_volatility, rank_strikes, time_to_expiry,
    )
    from .fischer_chain import read_chain, StaleChainError, _chain_cache
    from .fischer_signals import compute_conviction

    opt_type = "P" if intent == "covered_put" else "C"

    chain_df = None
    meta = None
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


def _table_header(show_conviction: bool = True) -> str:
    """Build HTML table header, optionally including the Conviction column."""
    conviction_th = (
        '<th style="padding:10px 12px; text-align:left;">Conviction</th>'
        if show_conviction else ""
    )
    return f"""
      <thead>
        <tr style="background:{_CLR_ROW_ALT}; border-bottom:2px solid {_CLR_BORDER};">
          <th style="padding:10px 12px; text-align:left;">Ticker</th>
          <th style="padding:10px 12px; text-align:left;">Spot</th>
          <th style="padding:10px 12px; text-align:left;">Strike</th>
          <th style="padding:10px 12px; text-align:left;">Expiry</th>
          <th style="padding:10px 12px; text-align:left;">Shares</th>
          <th style="padding:10px 12px; text-align:left;">Bid</th>
          <th style="padding:10px 12px; text-align:left;">OPT($)</th>
          <th style="padding:10px 12px; text-align:left;">IV</th>
          <th style="padding:10px 12px; text-align:left;">P(OTM)</th>
          <th style="padding:10px 12px; text-align:left;">P(Win)</th>
          <th style="padding:10px 12px; text-align:left;">Theta($)</th>
          <th style="padding:10px 12px; text-align:left;">Dir($)</th>
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
    shares = r.get("shares") or SHARE_ALLOC.get(ticker, 2_000)
    opt_income = r["bid"] * shares  # total option premium collected
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
            <td style="padding:10px 12px; font-weight:600;">${opt_income:,.0f}</td>
            <td style="padding:10px 12px;">{iv_pct:.1f}%</td>
            <td style="padding:10px 12px;">{potm_pct:.1f}%</td>
            <td style="{pwin_style}">{pwin_pct:.1f}%</td>
            <td style="padding:10px 12px; font-weight:700; color:{_CLR_GREEN};">
                ${theta_total:,.0f}</td>
            <td style="padding:10px 12px; font-weight:700; color:{_CLR_GREEN};">
                ${dir_total:,.0f}</td>
            {conviction_td}
        </tr>"""


def _sort_recs(recs: list[dict], display_order: tuple[str, ...] | None = None) -> list[dict]:
    """Sort recs by display_order (group ticker order). Falls back to alpha."""
    if display_order:
        order_map = {t: i for i, t in enumerate(display_order)}
        return sorted(recs, key=lambda r: order_map.get(r["ticker"], 99))
    return sorted(recs, key=lambda r: r["ticker"])


def _build_section(
    recs: list[dict],
    weekly_recs: list[dict] | None,
    section_title: str,
    entry_note: str,
    strike_label: str,
    header: str,
    show_conviction: bool = True,
    display_order: tuple[str, ...] | None = None,
) -> str:
    """Build HTML for one intent section (short-term + weekly tables)."""
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
      P(Win) band: {MIN_P_WIN:.1%} &ndash; {MAX_P_WIN:.0%}
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


def _build_recommendation_email(
    put_recs: list[dict],
    call_recs: list[dict] | None = None,
    put_weekly: list[dict] | None = None,
    call_weekly: list[dict] | None = None,
    failed_tickers: list[str] | None = None,
    slot_label: str = "Daily",
    group_label: str = "",
    show_conviction: bool = True,
    display_order: tuple[str, ...] | None = None,
) -> str:
    """Build combined HTML email with covered put and covered call sections."""
    today = date.today().strftime("%B %d, %Y")

    header = _table_header(show_conviction)

    sections = ""

    # --- Covered Puts ---
    if put_recs:
        sections += _build_section(
            recs=put_recs,
            weekly_recs=put_weekly,
            section_title="Covered Put Recommendations",
            entry_note="Entry assumes short sale at current spot",
            strike_label="put",
            header=header,
            show_conviction=show_conviction,
            display_order=display_order,
        )

    # --- Covered Calls ---
    if call_recs:
        if put_recs:
            sections += f"""
    <hr style="border:none; border-top:2px solid {_CLR_BORDER}; margin:24px 0 8px 0;">"""

        sections += _build_section(
            recs=call_recs,
            weekly_recs=call_weekly,
            section_title="Covered Call Ideas",
            entry_note="Entry assumes long stock at current spot",
            strike_label="call",
            header=header,
            show_conviction=show_conviction,
            display_order=display_order,
        )

    # --- Failed tickers notice ---
    failed_notice = ""
    if failed_tickers:
        failed_notice = f"""
    <p style="color:{_CLR_RED}; font-size:13px; font-weight:600; margin:12px 0 4px 0;">
      Unable to retrieve pricing for: {", ".join(failed_tickers)}
    </p>"""

    content = f"""
    {sections}

    {failed_notice}"""

    conviction_legend = (
        '<strong>Conviction</strong> = Signal conviction score (0&ndash;100)<br>'
        if show_conviction else ""
    )

    legend = f"""
    <div style="margin-top:16px; padding:14px 20px; font-size:13px;
                color:{_CLR_TEXT}; line-height:1.6;">
      <strong style="font-size:14px;">Legend</strong><br>
      <strong>Spot</strong> = price at scan &nbsp;|&nbsp;
      <strong>Strike</strong> = option strike &nbsp;|&nbsp;
      <strong>Shares</strong> = position size &nbsp;|&nbsp;
      <strong>Bid</strong> = option bid per share &nbsp;|&nbsp;
      <strong>OPT($)</strong> = total option income (bid &times; shares)<br>
      <strong>IV</strong> = implied volatility &nbsp;|&nbsp;
      <strong>P(OTM)</strong> = probability expires worthless (&le;55%) &nbsp;|&nbsp;
      <strong>P(Win)</strong> = probability of profit at expiry<br>
      <strong>Theta($)</strong> = extrinsic (time-decay) income &nbsp;|&nbsp;
      <strong>Dir($)</strong> = directional profit if assigned<br>
      {conviction_legend}
      <span style="background:{CLR_YELLOW}; padding:1px 5px;">Yellow row</span> = no 0DTE strike in band, deferred to later expiry &nbsp;|&nbsp;
      <span style="background:{CLR_YELLOW}; padding:1px 5px;">Yellow P(Win)</span> = no strike in band, showing closest match
    </div>"""

    title_prefix = f"Fischer {group_label}" if group_label else f"Fischer {slot_label}"

    return _wrap_fischer_document(
        body_html=content,
        title=title_prefix,
        subtitle=f"{slot_label} &mdash; {today}",
        footer_text="Generated by Fischer Options Engine",
        notes_html=legend,
        max_width=960,
    )


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
    header = _table_header(show_conviction=False)

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

        sorted_recs = _sort_recs(recs)
        rows_html = "".join(_build_rec_row(r, show_conviction=False) for r in sorted_recs)

        sections_html += f"""
    <h2 style="color:{_CLR_HEADER}; font-size:18px; margin:20px 0 4px 0;">
      {section.title}</h2>
    <p style="color:{_CLR_MUTED}; font-size:12px; margin:0 0 6px 0;">
      {section.entry_note} | Strike = {section.strike_label} strike |
      P(Win) floor: {MIN_P_WIN:.0%}
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
      <strong>Bid</strong> = option bid per share &nbsp;|&nbsp;
      <strong>OPT($)</strong> = total option income (bid &times; shares)<br>
      <strong>IV</strong> = implied volatility &nbsp;|&nbsp;
      <strong>P(OTM)</strong> = probability expires worthless (&le;55%) &nbsp;|&nbsp;
      <strong>P(Win)</strong> = probability of profit at expiry<br>
      <strong>Theta($)</strong> = extrinsic (time-decay) income &nbsp;|&nbsp;
      <strong>Dir($)</strong> = directional profit if assigned<br>
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
    results = {}
    failed = set()

    for section in REPORT_SECTIONS:
        intent = section.intent
        dte_range = section.dte_range
        dte_label = f"{dte_range[0]}-{dte_range[1]}"
        scan_slot = slot if dte_range[1] <= 7 else f"{slot}_weekly"

        # Check DB dedup for this slot/intent combo
        today_str = date.today().isoformat()
        tickers_tuple = tuple(SCAN_TICKERS)
        placeholders = ",".join("?" * len(tickers_tuple))
        existing = conn.execute(
            f"SELECT COUNT(*) FROM fischer_recommendations "
            f"WHERE report_date = ? AND scan_slot = ? AND intent = ? "
            f"AND ticker IN ({placeholders})",
            (today_str, scan_slot, intent, *tickers_tuple)
        ).fetchone()[0]

        if existing > 0:
            log.info(f"Fischer {slot}/{intent}/{dte_label}: already generated, loading from DB")
            loaded = _load_slot_recs(conn, today_str, scan_slot, intent)
            for rec in loaded:
                results[(rec["ticker"], intent, dte_label)] = rec
            continue

        recs_to_store = []
        for ticker in SCAN_TICKERS:
            log.info(f"Fischer {slot}/{intent}/{dte_label}: scanning {ticker}...")
            try:
                ranked = _scan_ticker(ticker, dte_range=dte_range, intent=intent)
                if not ranked:
                    log.info(f"  {ticker}: no chain data")
                    failed.add(ticker)
                    continue

                result = _select_best_candidate(ranked, ticker)
                if result:
                    ev, flag = result
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

        selected_macro = _select_macro_tickers(macro_candidates)
        macro_recs = [macro_candidates[t] for t in selected_macro]

        sections_data[section] = always_recs + macro_recs

    return sections_data


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


def send_subscriber_scan_reports(db_path: str, slot: ScanSlot = "opening"):
    """After a scheduled scan slot, distribute reports to active subscribers.

    - fischer_daily portfolio subscribers: load cached recs from DB, build email, send
    - Custom portfolio subscribers: generate fresh scan with their tickers
    """
    from .db import init_db, migrate_db
    from .postmaster import send_email
    from .fischer_subscribers import get_all_active_subscribers

    config = SCAN_SLOTS[slot]
    conn = None
    try:
        conn = init_db(db_path)
        migrate_db(conn)

        subscribers = get_all_active_subscribers(conn)
        if not subscribers:
            return

        # Group subscribers by portfolio for efficiency
        from collections import defaultdict
        by_portfolio: dict[str, list[dict]] = defaultdict(list)
        for sub in subscribers:
            by_portfolio[sub["portfolio_name"]].append(sub)

        for portfolio_name, subs in by_portfolio.items():
            try:
                _distribute_to_portfolio_subscribers(
                    conn, config, slot, portfolio_name, subs, send_email
                )
            except Exception as e:
                log.error(f"Fischer {slot} subscriber distribution for "
                          f"'{portfolio_name}' failed: {e}", exc_info=True)

    except Exception as e:
        log.error(f"Fischer {slot} subscriber distribution failed: {e}",
                  exc_info=True)
    finally:
        if conn:
            conn.close()


def _distribute_to_portfolio_subscribers(
    conn: sqlite3.Connection,
    config: ScanSlotConfig,
    slot: ScanSlot,
    portfolio_name: str,
    subscribers: list[dict],
    send_email,
):
    """Send scheduled scan results to all subscribers on a given portfolio."""
    from .fischer_subscribers import get_portfolio

    portfolio = get_portfolio(conn, portfolio_name)
    if not portfolio:
        log.error(f"Fischer subscriber dist: portfolio '{portfolio_name}' not found")
        return

    tickers = portfolio["tickers"]
    show_conviction = portfolio["show_conviction"]
    label = portfolio["label"]

    # If portfolio tickers are all in the scan universe, load cached recs from DB
    scan_set = set(SCAN_TICKERS)
    if set(tickers).issubset(scan_set):
        today_str = date.today().isoformat()
        put_recs = _load_slot_recs(conn, today_str, slot, "covered_put", tickers)
        call_recs = _load_slot_recs(conn, today_str, slot, "covered_call", tickers)
        put_weekly = _load_slot_recs(conn, today_str, f"{slot}_weekly", "covered_put", tickers)
        call_weekly = _load_slot_recs(conn, today_str, f"{slot}_weekly", "covered_call", tickers)
        failed = []
    else:
        # Custom portfolio with tickers outside the universe — generate fresh scan
        put_recs, call_recs, put_weekly, call_weekly, failed = generate_fresh_scan(
            tickers=tickers,
            share_alloc=portfolio["share_alloc"],
            show_conviction=show_conviction,
        )

    if not put_recs and not call_recs and not put_weekly and not call_weekly:
        log.info(f"Fischer subscriber dist: no results for '{portfolio_name}', skipping")
        return

    html = _build_recommendation_email(
        put_recs=put_recs,
        call_recs=call_recs,
        put_weekly=put_weekly,
        call_weekly=call_weekly,
        failed_tickers=failed,
        slot_label=config.label,
        group_label=label,
        show_conviction=show_conviction,
        display_order=tickers,
    )
    today_fmt = date.today().strftime('%b %d')
    subject = f"Fischer {label} — {config.label} — {today_fmt}"

    for sub in subscribers:
        try:
            send_email(subject, html, to_addr=sub["email"])
            log.info(f"Fischer {slot} subscriber report sent to {sub['email']} "
                     f"({portfolio_name})")
        except Exception as e:
            log.error(f"Fischer {slot} subscriber send to {sub['email']} failed: {e}")


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

    # Dedup: check if these specific tickers already have recs for (date, slot, intent)
    placeholders = ",".join("?" * len(scan_list))
    existing = conn.execute(
        f"SELECT COUNT(*) FROM fischer_recommendations "
        f"WHERE report_date = ? AND scan_slot = ? AND intent = ? "
        f"AND ticker IN ({placeholders})",
        (today_str, slot, intent, *scan_list)
    ).fetchone()[0]
    if existing > 0:
        log.info(f"Fischer {slot}/{intent}: already generated for {today_str} "
                 f"({existing} recs for {len(scan_list)} tickers)")
        return _load_slot_recs(conn, today_str, slot, intent, tickers), []

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

            result = _select_best_candidate(ranked, ticker)
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

            in_band = [r for r in ranked
                       if MIN_P_WIN <= r.p_profit <= MAX_P_WIN
                       and r.p_expire_worthless <= MAX_P_OTM]

            if in_band:
                best = max(in_band, key=lambda r: r.max_profit_per_share)
                flag = "clean"
                dte = (best.expiry - date.today()).days
                log.info(f"  {ticker}: weekly {dte}DTE K={best.strike} "
                         f"MaxP=${best.max_profit_per_share:.2f} "
                         f"P(Win)={best.p_profit:.1%}")
            else:
                best = min(ranked, key=lambda r: min(
                    abs(r.p_profit - MIN_P_WIN), abs(r.p_profit - MAX_P_WIN)))
                flag = "near_miss"
                log.info(f"  {ticker}: weekly near miss K={best.strike} "
                         f"P(Win)={best.p_profit:.1%} (outside band)")

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

def _select_best_candidate(ranked: list, ticker: str) -> tuple | None:
    """Pick the best EVResult from ranked results using P(Win) band filtering.

    Returns (ev, flag) or None if no results.
    Flag: "clean" = 0DTE in band, "deferred" = later expiry in band,
          "near_miss" = closest to band.
    """
    if not ranked:
        return None

    in_band = [r for r in ranked
               if MIN_P_WIN <= r.p_profit <= MAX_P_WIN
               and r.p_expire_worthless <= MAX_P_OTM]

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
        best = min(ranked, key=lambda r: min(
            abs(r.p_profit - MIN_P_WIN), abs(r.p_profit - MAX_P_WIN)))
        return (best, "near_miss")


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
    share_alloc : optional custom {ticker: shares} mapping (falls back to global SHARE_ALLOC)
    show_conviction : whether the report should include Nenner conviction column

    Returns
    -------
    (put_recs, call_recs, put_weekly, call_weekly, failed_tickers)
    """
    alloc = share_alloc or SHARE_ALLOC

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

                result = _select_best_candidate(ranked, ticker)
                if result:
                    ev, flag = result
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
                "shares": alloc.get(ticker, SHARE_ALLOC.get(ticker, 2000)),
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
