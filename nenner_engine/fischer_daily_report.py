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

import hashlib
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

from .config import REPORT_RECIPIENT
from .fischer_scanner import (
    SCAN_TICKERS, CAPITAL, TOP_PICKS,
    ScanSlot, ScanSlotConfig, SCAN_SLOTS,
    scan_ticker, select_best_candidate, select_top_trades,
    assemble_sections, calc_shares, get_rules,
)

log = logging.getLogger("nenner")

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


# ---------------------------------------------------------------------------
# Scan Pipeline
# ---------------------------------------------------------------------------


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
    shares = r.get("shares") or calc_shares(r["spot_at_recommend"])
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
        shares = calc_shares(r.get("spot_at_recommend", 0))
        return r.get("max_profit_per_share", 0) * shares
    return sorted(recs, key=_total_max_profit, reverse=True)


# ---------------------------------------------------------------------------
# Single-Instrument Drill-Down Report
# ---------------------------------------------------------------------------

def build_instrument_drilldown(
    ticker: str,
    ev_results: list,
    spot: float,
    intent: str = "covered_put",
) -> str:
    """Build HTML report showing all strikes for one instrument.

    Shows how risk/reward changes across every strike in the chain,
    grouped by expiry. Designed for subscriber "Refresh TSLA" requests.

    Parameters
    ----------
    ticker : instrument ticker (e.g. "TSLA")
    ev_results : list of EVResult objects from scan_ticker() — ALL strikes,
                 not just the best one
    spot : current spot price
    intent : "covered_put" or "covered_call"
    """
    from itertools import groupby

    is_put = intent == "covered_put"
    today = date.today()
    shares = calc_shares(spot)
    action = "Covered Put" if is_put else "Covered Call"
    assign_label = "if Assigned" if is_put else "if Called"

    # Group by expiry, sort strikes within each group
    ev_results = sorted(ev_results, key=lambda e: (e.expiry, e.strike))

    sections_html = ""
    for expiry, group in groupby(ev_results, key=lambda e: e.expiry):
        strikes = list(group)
        dte = (expiry - today).days
        exp_fmt = _format_expiry(str(expiry))

        rows_html = ""
        for ev in strikes:
            if is_put:
                intrinsic = max(0, ev.strike - spot)
                dir_ps = spot - ev.strike
            else:
                intrinsic = max(0, spot - ev.strike)
                dir_ps = ev.strike - spot
            extrinsic = ev.bid - intrinsic
            max_prof_ps = dir_ps + ev.bid
            max_prof_d = max_prof_ps * shares
            prem_d = ev.bid * shares
            theta_d = extrinsic * shares
            dir_d = dir_ps * shares
            pwin_pct = ev.p_profit * 100
            potm_pct = ev.p_expire_worthless * 100

            rows_html += f"""
            <tr style="border-bottom:1px solid {_CLR_BORDER};">
                <td style="padding:8px 10px; font-weight:600;">${ev.strike:.2f}</td>
                <td style="padding:8px 10px;">${ev.bid:.2f}</td>
                <td style="padding:8px 10px;">{potm_pct:.1f}%</td>
                <td style="padding:8px 10px;">{pwin_pct:.1f}%</td>
                <td style="padding:8px 10px;">${prem_d:,.0f}</td>
                <td style="padding:8px 10px;">${theta_d:,.0f}</td>
                <td style="padding:8px 10px;">${dir_d:,.0f}</td>
                <td style="padding:8px 10px; font-weight:700; color:{_CLR_GREEN};">
                    ${max_prof_d:,.0f}</td>
                <td style="padding:8px 10px;">{ev.delta:+.3f}</td>
                <td style="padding:8px 10px;">{ev.iv * 100:.1f}%</td>
            </tr>"""

        if sections_html:
            sections_html += (
                f'<hr style="border:none; border-top:1px solid {_CLR_BORDER};'
                f' margin:16px 0 8px 0;">'
            )

        sections_html += f"""
        <h3 style="color:{_CLR_HEADER}; font-size:16px; margin:12px 0 4px 0;">
          {exp_fmt} &mdash; {dte} DTE</h3>
        <div style="overflow-x:auto;">
        <table style="width:100%; border-collapse:collapse; font-size:13px;
                      color:{_CLR_TEXT};">
          <thead>
            <tr style="background:{_CLR_ROW_ALT}; border-bottom:2px solid {_CLR_BORDER};">
              <th style="padding:8px 10px; text-align:left;">Strike</th>
              <th style="padding:8px 10px; text-align:left;">Bid</th>
              <th style="padding:8px 10px; text-align:left;">P(OTM)</th>
              <th style="padding:8px 10px; text-align:left;">P(Win)</th>
              <th style="padding:8px 10px; text-align:left;">Premium $</th>
              <th style="padding:8px 10px; text-align:left;">Theta $</th>
              <th style="padding:8px 10px; text-align:left;">Dir $</th>
              <th style="padding:8px 10px; text-align:left;">Max Profit<br>{assign_label}</th>
              <th style="padding:8px 10px; text-align:left;">Delta</th>
              <th style="padding:8px 10px; text-align:left;">IV</th>
            </tr>
          </thead>
          <tbody>{rows_html}</tbody>
        </table>
        </div>"""

    # Summary header
    summary = f"""
    <div style="margin-bottom:12px; font-size:14px; color:{_CLR_TEXT}; line-height:1.8;">
      <strong>Spot:</strong> ${spot:.2f} &nbsp;|&nbsp;
      <strong>Shares:</strong> {shares:,} &nbsp;|&nbsp;
      <strong>Strategy:</strong> {action}
    </div>"""

    legend = f"""
    <div style="margin-top:16px; padding:14px 20px; font-size:13px;
                color:{_CLR_TEXT}; line-height:1.6;">
      <strong style="font-size:14px;">Legend</strong><br>
      <strong>Premium $</strong> = bid &times; shares (total premium collected) &nbsp;|&nbsp;
      <strong>Theta $</strong> = extrinsic value &times; shares (time-decay income)<br>
      <strong>Dir $</strong> = directional profit if assigned &nbsp;|&nbsp;
      <strong>Max Profit</strong> = theta + directional combined<br>
      <strong>P(OTM)</strong> = probability option expires worthless &nbsp;|&nbsp;
      <strong>P(Win)</strong> = probability of profit at expiry
    </div>"""

    return _wrap_fischer_document(
        body_html=f"{summary}{sections_html}",
        title=f"{ticker} {action} Analysis",
        subtitle=f"Strike Drill-Down &mdash; {today.strftime('%B %d, %Y')}",
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
      P(Win) floor: {get_rules(section.intent)["min_p_win"]:.0%}
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
    send_email(subject, body, to_addr=REPORT_RECIPIENT, from_name="Fischer")
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
    # Phase 0: bulk pre-load put + call chains from OptionChains_Beta.xlsm
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
                ranked = scan_ticker(ticker, dte_range=dte_range, intent=intent, chain_data=preloaded)
                if not ranked:
                    log.info(f"  {ticker}: no chain data")
                    failed.add(ticker)
                    continue

                result = select_best_candidate(ranked, ticker, intent=intent)
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


def _send_to_subscribers(conn: sqlite3.Connection, subject: str, html: str,
                         dedup_key: str | None = None):
    """Send the same scan HTML to all active Fischer subscribers.

    Uses SendDeduplicator (S5) to prevent duplicate sends if the scan
    fires more than once for the same slot.

    Args:
        dedup_key: Stable key (e.g. "2026-03-04_opening") used for dedup.
                   If None, dedup is skipped.
    """
    try:
        from .fischer_subscribers import get_all_active_subscribers
        from .postmaster import send_email as _send

        # Get dedup guard if reliability layer is active
        dedup = None
        try:
            from .fischer_reliability import FischerReliability
            rel = FischerReliability.get_instance()
            if rel:
                dedup = rel.dedup
        except ImportError:
            pass

        subscribers = get_all_active_subscribers(conn)
        for sub in subscribers:
            email = sub["email"]
            try:
                if dedup and dedup_key:
                    job_id = hashlib.sha256(
                        f"{email.lower()}|{dedup_key}".encode()
                    ).hexdigest()[:12]
                    if not dedup.check_and_mark(email, subject, job_id):
                        log.info(f"Fischer scan dedup: skipping {email} (already sent)")
                        continue
                _send(subject, html, to_addr=email, from_name="Fischer")
                log.info(f"Fischer scan sent to subscriber {email}")
            except Exception as e:
                log.error(f"Fischer scan send to {email} failed: {e}")
    except Exception as e:
        log.error(f"Fischer subscriber distribution failed: {e}", exc_info=True)


def send_scan_report(db_path: str, slot: ScanSlot = "opening",
                     send_emails: bool = True):
    """Generate unified Fischer report: 4 sections, one email.

    Scans all 17 tickers for both intents at both DTE ranges,
    assembles sections with macro selection, and sends a single email.

    Args:
        db_path: Path to nenner_signals.db
        slot: Scan slot (opening/midday/closing)
        send_emails: If False, scan and commit to DB only (no email delivery)

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

    if send_emails and rel and rel.cache:
        today_str = date.today().isoformat()
        cache_key = f"scan_{slot}"
        cached = rel.cache.get(cache_key, today_str)
        if cached:
            log.info(f"Fischer {slot}: serving from cache")
            today_fmt = date.today().strftime('%b %d')
            time_fmt = datetime.now().strftime('%I:%M %p').lstrip('0')
            subject = f"Fischer Daily Scan \u2014 {time_fmt} \u2014 {today_fmt}"
            # Dedup admin copy using stable date+slot key
            dedup_key = f"{today_str}_{slot}"
            admin_sent = True
            if rel.dedup:
                admin_job = hashlib.sha256(
                    f"{REPORT_RECIPIENT.lower()}|{dedup_key}".encode()
                ).hexdigest()[:12]
                if not rel.dedup.check_and_mark(
                        REPORT_RECIPIENT, subject, admin_job):
                    log.info(f"Fischer {slot}: admin copy dedup, skipping")
                    admin_sent = False
            if admin_sent:
                send_email(subject, cached.result_html,
                           to_addr=REPORT_RECIPIENT, from_name="Fischer")
            from .db import init_db
            cache_conn = init_db(db_path)
            _send_to_subscribers(cache_conn, subject, cached.result_html,
                                 dedup_key=dedup_key)
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
            if send_emails:
                _send_offline_alert(send_email, config)
            return

        # Scan guard: abort if too many tickers failed
        if rel and rel.scan_guard:
            if rel.scan_guard.check_abort(failed_tickers, slot):
                return

        # Phase 2: Assemble 4 sections with macro selection
        sections_data = assemble_sections(all_results, REPORT_SECTIONS)

        # Phase 3: Build report (and optionally send email)
        html = _build_unified_email(sections_data, failed_tickers, config.label)
        today_fmt = date.today().strftime('%b %d')
        time_fmt = datetime.now().strftime('%I:%M %p').lstrip('0')
        subject = f"Fischer Daily Scan \u2014 {time_fmt} \u2014 {today_fmt}"

        if send_emails:
            send_email(subject, html, to_addr=REPORT_RECIPIENT, from_name="Fischer")
            # Phase 4: Send to active subscribers
            dedup_key = f"{date.today().isoformat()}_{slot}"
            _send_to_subscribers(conn, subject, html, dedup_key=dedup_key)

        # Store in cache
        if rel and rel.cache:
            rel.cache.put(f"scan_{slot}", date.today().isoformat(), html)

        total_recs = sum(len(recs) for recs in sections_data.values())
        action = "sent" if send_emails else "committed to DB (no email)"
        log.info(f"Fischer {slot}: unified report {action} ({total_recs} recs across 4 sections)")

    except Exception as e:
        log.error(f"Fischer {slot} report failed: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()





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
            ranked = scan_ticker(ticker, dte_range=dte_range, intent=intent)
            if not ranked:
                log.info(f"  {ticker}: no chain data")
                failed_tickers.append(ticker)
                continue

            result = select_best_candidate(ranked, ticker, intent=intent)
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
    top = all_candidates[:TOP_PICKS]

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
            ranked = scan_ticker(ticker, dte_range=(7, 14), intent=intent)
            if not ranked:
                log.info(f"  {ticker}: no weekly chain data")
                failed_tickers.append(ticker)
                continue

            R = get_rules(intent)
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
    share_alloc : optional custom {ticker: shares} mapping (falls back to calc_shares)
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
                ranked = scan_ticker(ticker, dte_range=dte_range, intent=intent)
                if not ranked:
                    log.info(f"  {ticker}: no chain data")
                    failed.append(ticker)
                    continue

                result = select_best_candidate(ranked, ticker, intent=intent)
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
                "shares": alloc.get(ticker) or calc_shares(ev.spot),
            })
        return recs, failed

    # Short-term puts and calls (0-7 DTE, matching REPORT_SECTIONS[0])
    put_recs, put_failed = _scan_intent("covered_put", dte_range=(0, 7))
    call_recs, call_failed = _scan_intent("covered_call", dte_range=(0, 7))

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
        send_email(subject, html, to_addr=REPORT_RECIPIENT, from_name="Fischer")
        log.info(f"Fischer settlement report sent to {REPORT_RECIPIENT}")

    except Exception as e:
        log.error(f"Fischer settlement report failed: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
