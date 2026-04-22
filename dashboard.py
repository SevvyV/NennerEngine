"""
Nenner Signal Dashboard
========================
Plotly Dash dashboard for monitoring Nenner cycle research signals.
Run: python dashboard.py [--port PORT] [--db PATH]

Single-process architecture: the dashboard hosts the Dash web UI AND runs
the background monitor threads (alert evaluator + email scheduler) in the
same process.

Background threads started at boot:
  - AlertMonitorThread (60s): reads yFinance prices (5-min TTL cache),
    evaluates cancel proximity and custom price alerts, dispatches via Telegram
  - EmailScheduler (30s tick): checks for new Nenner emails, sends stock
    reports at 8:30 AM, runs auto-cancel at 4:30 PM, Nenner watchdog at noon

Signal data:
  - Served via NennerEngine Signals API (port 8051) for Excel Power Query
"""

import argparse
import logging
import math
import os
import sqlite3
import threading

import dash

log = logging.getLogger("nenner_engine")
from dash import dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

from nenner_engine.trade_stats import compute_instrument_stats

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = r"E:\Workspace\DataCenter\nenner_signals.db"
WATCHLIST_ROW1 = ["TSLA", "BAC", "MSFT", "AAPL", "GOOG", "NVDA"]
WATCHLIST_ROW2 = ["GDXJ", "GLD", "SLV", "USO", "UNG", "SOYB", "NEM"]
WATCHLIST_ROW3 = ["ES", "NQ", "GBTC", "ETHE"]
WATCHLIST_TICKERS = WATCHLIST_ROW1 + WATCHLIST_ROW2 + WATCHLIST_ROW3
REFRESH_INTERVAL_MS = 30_000  # 30 seconds


# Color palette
COLOR_BUY = "#00bc8c"     # green
COLOR_SELL = "#e74c3c"    # red
COLOR_NEUTRAL = "#6c757d" # gray
COLOR_IMPLIED = "#f39c12"  # amber for implied reversals
COLOR_NTC = "#e74c3c"     # note-the-change highlight
COLOR_CARD_BG = "#2b3035"
COLOR_HEADER = "#adb5bd"

# Market Data page config
MD_REFRESH_INTERVAL_MS = 900_000  # 15 minutes
_DISPLAY_ALIAS = {"GOOGL": "GOOG"}  # DataBento → display ticker

# Previous close cache (refreshed once per day)
_prev_close_cache: dict[str, float] = {}
_prev_close_date: str = ""


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Data Queries
# ---------------------------------------------------------------------------

def fetch_current_state():
    """Fetch current signal states, excluding stale signals (>3 months old)."""
    conn = get_db()
    rows = conn.execute("""
        SELECT ticker, instrument, asset_class, effective_signal,
               origin_price, cancel_direction, cancel_level,
               trigger_level, implied_reversal, last_signal_date
        FROM current_state
        WHERE last_signal_date >= date('now', '-3 months')
        ORDER BY asset_class, instrument
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]




def fetch_recent_changes(days=7):
    """Fetch recent signal changes from signals table."""
    conn = get_db()
    rows = conn.execute("""
        SELECT s.date, s.instrument, s.ticker, s.signal_type, s.signal_status,
               s.origin_price, s.cancel_level, s.note_the_change
        FROM signals s
        WHERE s.date >= date('now', ?)
        ORDER BY s.date DESC, s.id DESC
        LIMIT 50
    """, (f"-{days} days",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fetch_watchlist():
    """Fetch watchlist instrument states enriched with live prices."""
    try:
        from nenner_engine.prices import get_prices_with_signal_context
        conn = get_db()
        rows = get_prices_with_signal_context(conn, tickers=WATCHLIST_TICKERS, try_t1=True)
        conn.close()
        return rows
    except Exception as e:
        log.error("fetch_watchlist price enrichment failed: %s", e, exc_info=True)
        # Fallback: signal-only (no prices)
        conn = get_db()
        placeholders = ",".join("?" for _ in WATCHLIST_TICKERS)
        rows = conn.execute(f"""
            SELECT ticker, instrument, asset_class, effective_signal,
                   origin_price, cancel_direction, cancel_level,
                   trigger_level, implied_reversal, last_signal_date
            FROM current_state
            WHERE ticker IN ({placeholders})
            ORDER BY instrument
        """, WATCHLIST_TICKERS).fetchall()
        conn.close()
        return [dict(r) for r in rows]


def fetch_positions():
    """Fetch live positions from Excel and enrich with Nenner signals."""
    try:
        from nenner_engine.positions import get_positions_with_signal_context
        conn = get_db()
        enriched = get_positions_with_signal_context(conn)
        conn.close()
        return enriched
    except Exception:
        return []


def fetch_db_stats():
    """Fetch database summary stats."""
    conn = get_db()
    active_filter = "WHERE last_signal_date >= date('now', '-3 months')"
    stats = {
        "emails": conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0],
        "signals": conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0],
        "cycles": conn.execute("SELECT COUNT(*) FROM cycles").fetchone()[0],
        "targets": conn.execute("SELECT COUNT(*) FROM price_targets").fetchone()[0],
        "instruments": conn.execute(f"SELECT COUNT(*) FROM current_state {active_filter}").fetchone()[0],
    }
    buys = conn.execute(f"SELECT COUNT(*) FROM current_state {active_filter} AND effective_signal='BUY'").fetchone()[0]
    sells = conn.execute(f"SELECT COUNT(*) FROM current_state {active_filter} AND effective_signal='SELL'").fetchone()[0]
    stats["buys"] = buys
    stats["sells"] = sells
    date_range = conn.execute("SELECT MIN(date_sent), MAX(date_sent) FROM emails").fetchone()
    stats["date_min"] = date_range[0] or "N/A"
    stats["date_max"] = date_range[1] or "N/A"
    conn.close()
    return stats


# ---------------------------------------------------------------------------
# UI Components
# ---------------------------------------------------------------------------

def signal_color(signal):
    if signal == "BUY":
        return COLOR_BUY
    elif signal == "SELL":
        return COLOR_SELL
    return COLOR_NEUTRAL


def make_watchlist_card(row):
    """Build a single watchlist instrument card with live price and P/L."""
    sig = row.get("effective_signal", "")
    color = signal_color(sig)
    implied = row.get("implied_reversal", 0)

    # --- Live price ---
    price = row.get("price")
    price_source = row.get("price_source", "")
    price_str = f"{price:,.2f}" if price else "—"
    source_label = f" ({price_source})" if price_source else ""

    # --- P/L ---
    pnl_pct = row.get("pnl_pct")
    if pnl_pct is not None:
        pnl_color = COLOR_BUY if pnl_pct >= 0 else COLOR_SELL
        pnl_str = f"{pnl_pct:+.1f}%"
    else:
        pnl_color = COLOR_NEUTRAL
        pnl_str = ""

    # --- Cancel distance ---
    cancel_dist = row.get("cancel_dist_pct")
    cancel_level = row.get("cancel_level")
    if cancel_level and cancel_dist is not None:
        direction = row.get("cancel_direction", "")
        cancel_text = f"Cancel {direction} {cancel_level:,.2f} ({abs(cancel_dist):.1f}% away)"
    elif cancel_level:
        direction = row.get("cancel_direction", "")
        cancel_text = f"Cancel {direction} {cancel_level:,.2f}"
    else:
        cancel_text = ""

    # --- Signal badge ---
    badge_children = [sig] if sig else ["—"]
    if implied:
        badge_children = [sig, " ", html.Small("(impl)", style={"color": COLOR_IMPLIED})]

    return dbc.Col(
        dbc.Card([
            dbc.CardHeader(
                html.Div([
                    html.Span(row.get("ticker", ""), style={"fontWeight": "bold", "fontSize": "1.3rem"}),
                    html.Span(
                        row.get("instrument", ""),
                        className="ms-2",
                        style={"fontSize": "0.85rem", "color": COLOR_HEADER},
                    ),
                ]),
                style={"backgroundColor": COLOR_CARD_BG, "borderBottom": f"3px solid {color}"},
            ),
            dbc.CardBody([
                # Price row: large price + source tag
                html.Div([
                    html.Span(
                        price_str,
                        style={"fontSize": "1.5rem", "fontWeight": "bold", "color": "#fff"},
                    ),
                    html.Span(
                        source_label,
                        style={"fontSize": "0.7rem", "color": "#666", "marginLeft": "4px"},
                    ),
                    html.Span(
                        f"  {pnl_str}",
                        style={"fontSize": "1.0rem", "fontWeight": "bold",
                               "color": pnl_color, "marginLeft": "8px"},
                    ) if pnl_str else None,
                ], style={"marginBottom": "0.4rem"}),
                # Signal badge
                html.Div(
                    badge_children,
                    style={
                        "fontSize": "1.2rem",
                        "fontWeight": "bold",
                        "color": color,
                        "marginBottom": "0.3rem",
                    },
                ),
                html.Div(f"From {row['origin_price']:,.2f}" if row.get("origin_price") else "",
                         style={"fontSize": "0.85rem", "color": COLOR_HEADER}),
                html.Div(cancel_text,
                         style={"fontSize": "0.8rem", "color": "#888"}),
                # --- Price target ---
                html.Div(
                    f"Target {row['target_price']:,.2f} ({row['target_dist_pct']:+.1f}%)"
                    if row.get("target_price") and row.get("target_dist_pct") is not None
                    else f"Target {row['target_price']:,.2f}"
                    if row.get("target_price")
                    else "",
                    style={"fontSize": "0.8rem", "color": "#5bc0de"}
                ),
                html.Div(row.get("last_signal_date", ""),
                         style={"fontSize": "0.75rem", "color": "#666", "marginTop": "0.2rem"}),
            ], style={"backgroundColor": "#1e2226"}),
        ], className="h-100", style={"border": "1px solid #444"}),
        xs=12, sm=6, md=2, lg=2,
        className="mb-3",
    )


def make_stats_bar(stats):
    """Build the top stats summary bar."""
    items = [
        ("Instruments", stats["instruments"]),
        ("BUY", stats["buys"]),
        ("SELL", stats["sells"]),
        ("Signals", stats["signals"]),
        ("Emails", stats["emails"]),
    ]
    cols = []
    for label, value in items:
        color = COLOR_BUY if label == "BUY" else COLOR_SELL if label == "SELL" else "#fff"
        cols.append(
            dbc.Col(
                html.Div([
                    html.Div(str(value), style={"fontSize": "1.5rem", "fontWeight": "bold", "color": color}),
                    html.Div(label, style={"fontSize": "0.75rem", "color": COLOR_HEADER, "textTransform": "uppercase"}),
                ], className="text-center"),
                width="auto",
            )
        )
    return dbc.Row(cols, className="g-4 justify-content-center py-2")


def make_position_card(pos):
    """Build a single position card showing P/L and Nenner signal."""
    underlying = pos["underlying"]
    strategy = pos["strategy"].replace("_", " ").title()
    total_pnl = pos["total_pnl_dollar"]
    stock_pnl = pos["stock_pnl_dollar"]
    opt_pnl = pos["option_pnl_dollar"]
    signal = pos.get("nenner_signal")
    current = pos.get("current_price")
    cancel_dist = pos.get("cancel_dist_pct")
    near_expiry = pos.get("near_expiry")

    # P/L color
    pnl_color = COLOR_BUY if total_pnl >= 0 else COLOR_SELL
    sig_color = signal_color(signal) if signal else COLOR_NEUTRAL

    # Stock shares (sum across stock legs)
    stock_shares = sum(
        leg["shares"] for leg in pos.get("legs", []) if not leg["is_option"]
    )

    detail_lines = []
    if stock_pnl:
        detail_lines.append(f"Stock: ${stock_pnl:+,.0f}")
    if opt_pnl:
        detail_lines.append(f"Options: ${opt_pnl:+,.0f}")

    return dbc.Col(
        dbc.Card([
            dbc.CardHeader(
                html.Div([
                    html.Span(underlying, style={"fontWeight": "bold", "fontSize": "1.3rem"}),
                    html.Span(
                        strategy,
                        className="ms-2",
                        style={"fontSize": "0.85rem", "color": COLOR_HEADER},
                    ),
                ]),
                style={"backgroundColor": COLOR_CARD_BG, "borderBottom": f"3px solid {pnl_color}"},
            ),
            dbc.CardBody([
                html.Div(
                    f"${total_pnl:+,.0f}",
                    style={
                        "fontSize": "1.6rem",
                        "fontWeight": "bold",
                        "color": pnl_color,
                        "marginBottom": "0.3rem",
                    },
                ),
                html.Div(
                    " | ".join(detail_lines),
                    style={"fontSize": "0.8rem", "color": COLOR_HEADER},
                ) if detail_lines else None,
                html.Div([
                    html.Span(f"{signal}", style={"color": sig_color, "fontWeight": "bold"}),
                    html.Span(
                        f"  Cancel: {cancel_dist:+.1f}%" if cancel_dist is not None else "",
                        style={"color": "#888", "fontSize": "0.85rem"},
                    ),
                ], style={"marginTop": "0.4rem", "fontSize": "0.9rem"}) if signal else None,
                html.Div(
                    f"{int(stock_shares):,} shares @ {current:,.2f}" if current and stock_shares else
                    (f"Price: {current:,.2f}" if current else ""),
                    style={"fontSize": "0.8rem", "color": "#666", "marginTop": "0.2rem"},
                ),
                html.Div(
                    f"Exp: {near_expiry}" if near_expiry else "",
                    style={"fontSize": "0.75rem", "color": "#555", "marginTop": "0.1rem"},
                ),
            ], style={"backgroundColor": "#1e2226"}),
        ], className="h-100", style={"border": "1px solid #444"}),
        xs=12, sm=6, md=3,
        className="mb-3",
    )


# ---------------------------------------------------------------------------
# DataTable Style Helpers
# ---------------------------------------------------------------------------

SIGNAL_TABLE_STYLE_HEADER = {
    "backgroundColor": "#2b3035",
    "color": COLOR_HEADER,
    "fontWeight": "bold",
    "border": "1px solid #444",
    "fontSize": "1.45rem",
}

SIGNAL_TABLE_STYLE_CELL = {
    "backgroundColor": "#1e2226",
    "color": "#e0e0e0",
    "border": "1px solid #333",
    "fontSize": "1.45rem",
    "padding": "10px 14px",
}

COLOR_PF_GOOD = "#00bc8c"    # green — PF >= 2.0
COLOR_PF_OK = "#f39c12"      # amber — PF 1.0–2.0
COLOR_PF_BAD = "#e74c3c"     # red   — PF < 1.0

SIGNAL_TABLE_STYLE_DATA_CONDITIONAL = [
    {
        "if": {"filter_query": '{effective_signal} = "BUY"', "column_id": "effective_signal"},
        "color": COLOR_BUY,
        "fontWeight": "bold",
    },
    {
        "if": {"filter_query": '{effective_signal} = "SELL"', "column_id": "effective_signal"},
        "color": COLOR_SELL,
        "fontWeight": "bold",
    },
    {
        "if": {"filter_query": '{implied_reversal} = 1'},
        "fontStyle": "italic",
    },
    {
        "if": {"filter_query": '{implied_reversal} = 1', "column_id": "implied_reversal"},
        "color": COLOR_IMPLIED,
    },
    # PF conditional coloring: green >= 2.0, amber 1.0-2.0, red < 1.0
    {
        "if": {"filter_query": '{pf} >= 2.0', "column_id": "pf"},
        "color": COLOR_PF_GOOD,
        "fontWeight": "bold",
    },
    {
        "if": {"filter_query": '{pf} >= 1.0 && {pf} < 2.0', "column_id": "pf"},
        "color": COLOR_PF_OK,
        "fontWeight": "bold",
    },
    {
        "if": {"filter_query": '{pf} < 1.0 && {pf} > 0', "column_id": "pf"},
        "color": COLOR_PF_BAD,
        "fontWeight": "bold",
    },
    # Win% conditional coloring
    {
        "if": {"filter_query": '{win_pct} >= 60', "column_id": "win_pct"},
        "color": COLOR_PF_GOOD,
    },
    {
        "if": {"filter_query": '{win_pct} >= 40 && {win_pct} < 60', "column_id": "win_pct"},
        "color": COLOR_PF_OK,
    },
    {
        "if": {"filter_query": '{win_pct} < 40 && {win_pct} > 0', "column_id": "win_pct"},
        "color": COLOR_PF_BAD,
    },
    # Sharpe conditional coloring: >= 0.5 green, 0.2-0.5 amber, < 0.2 red
    {
        "if": {"filter_query": '{sharpe} >= 0.5', "column_id": "sharpe"},
        "color": COLOR_PF_GOOD,
        "fontWeight": "bold",
    },
    {
        "if": {"filter_query": '{sharpe} >= 0.2 && {sharpe} < 0.5', "column_id": "sharpe"},
        "color": COLOR_PF_OK,
    },
    {
        "if": {"filter_query": '{sharpe} < 0.2 && {sharpe} != ""', "column_id": "sharpe"},
        "color": COLOR_PF_BAD,
    },
    # P/L% conditional coloring: green positive, red negative
    {
        "if": {"filter_query": '{pnl_pct} contains "+"', "column_id": "pnl_pct"},
        "color": COLOR_BUY,
        "fontWeight": "bold",
    },
    {
        "if": {"filter_query": '{pnl_pct} contains "-"', "column_id": "pnl_pct"},
        "color": COLOR_SELL,
        "fontWeight": "bold",
    },
    # Score conditional coloring: >= 50 green, 30-50 amber, < 30 red
    {
        "if": {"filter_query": '{score} >= 50', "column_id": "score"},
        "color": COLOR_PF_GOOD,
        "fontWeight": "bold",
    },
    {
        "if": {"filter_query": '{score} >= 30 && {score} < 50', "column_id": "score"},
        "color": COLOR_PF_OK,
    },
    {
        "if": {"filter_query": '{score} < 30 && {score} > 0', "column_id": "score"},
        "color": COLOR_PF_BAD,
    },
]

CHANGELOG_TABLE_STYLE_DATA_CONDITIONAL = [
    {
        "if": {"filter_query": '{signal_type} = "BUY"', "column_id": "signal_type"},
        "color": COLOR_BUY,
        "fontWeight": "bold",
    },
    {
        "if": {"filter_query": '{signal_type} = "SELL"', "column_id": "signal_type"},
        "color": COLOR_SELL,
        "fontWeight": "bold",
    },
    {
        "if": {"filter_query": '{signal_status} = "CANCELLED"', "column_id": "signal_status"},
        "color": COLOR_IMPLIED,
        "fontWeight": "bold",
    },
    {
        "if": {"filter_query": '{note_the_change} = 1'},
        "backgroundColor": "#3d2020",
    },
    {
        "if": {"filter_query": '{note_the_change} = 1', "column_id": "note_the_change"},
        "color": COLOR_NTC,
        "fontWeight": "bold",
    },
]


# ---------------------------------------------------------------------------
# Previous Close Helper (for Market Data page change calculation)
# ---------------------------------------------------------------------------

def _get_prev_closes(tickers: list[str]) -> dict[str, float]:
    """Fetch previous session close prices (cached once per day via yfinance)."""
    global _prev_close_cache, _prev_close_date
    from datetime import date
    today = date.today().isoformat()
    if _prev_close_date == today and _prev_close_cache:
        return dict(_prev_close_cache)

    try:
        import yfinance as yf
        data = yf.download(tickers, period="5d", progress=False, threads=True)
        if data is not None and not data.empty:
            closes = data["Close"]
            # Filter to dates strictly before today
            prev_data = closes[closes.index.strftime("%Y-%m-%d") < today]
            if not prev_data.empty:
                last_row = prev_data.iloc[-1]
                result = {}
                for t in tickers:
                    col = t if t in last_row.index else None
                    if col and not math.isnan(last_row[col]):
                        result[t] = float(last_row[col])
                _prev_close_cache = result
                _prev_close_date = today
    except Exception as e:
        log.warning("Failed to fetch prev closes via yfinance: %s", e)

    return dict(_prev_close_cache)


# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------

def _build_nav():
    return dbc.Navbar(
        dbc.Container([
            dbc.NavbarBrand(
                "NENNER ENGINE",
                href="/",
                style={"letterSpacing": "0.15em", "fontWeight": "700", "fontSize": "1.1rem"},
            ),
            dbc.Nav([
                dbc.NavItem(dbc.NavLink("Signals", href="/",
                    style={"letterSpacing": "0.05em", "fontSize": "0.9rem"})),
                dbc.NavItem(dbc.NavLink("Market Data", href="/market-data",
                    style={"letterSpacing": "0.05em", "fontSize": "0.9rem"})),
            ], navbar=True),
        ], fluid=True),
        color="#1a1d21",
        dark=True,
        className="mb-0",
        style={"borderBottom": "1px solid #444"},
    )


# ---------------------------------------------------------------------------
# Page: Signals (existing dashboard)
# ---------------------------------------------------------------------------

def _signals_page():
    return dbc.Container([
        # Auto-refresh interval
        dcc.Interval(id="refresh-interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),

        # Header with Refresh button
        dbc.Row([
            dbc.Col(
                html.Div([
                    html.H3("SIGNAL DASHBOARD", className="mb-0",
                             style={"letterSpacing": "0.15em", "fontWeight": "700"}),
                    html.Small("Vartanian Capital Management",
                               style={"color": COLOR_HEADER, "letterSpacing": "0.05em"}),
                ], className="text-center py-3"),
                width=True,
            ),
            dbc.Col(
                dbc.Button(
                    [html.I(className="fas fa-sync-alt me-2"), "Refresh"],
                    id="refresh-button",
                    color="outline-light",
                    size="sm",
                    className="mt-3",
                    style={"letterSpacing": "0.05em"},
                ),
                width="auto",
                className="d-flex align-items-center",
            ),
        ], className="mb-2 align-items-center"),

        # Stats bar
        html.Div(id="stats-bar"),

        html.Hr(style={"borderColor": "#444"}),

        # Panel E: Watchlist Focus
        html.Div([
            html.H5("WATCHLIST", className="mb-3",
                     style={"color": COLOR_HEADER, "letterSpacing": "0.1em", "fontWeight": "600"}),
            html.Div(id="watchlist-cards"),
        ], className="mb-4"),

        html.Hr(style={"borderColor": "#444"}),

        # Panel: Positions
        html.Div([
            html.H5("POSITIONS", className="mb-3",
                     style={"color": COLOR_HEADER, "letterSpacing": "0.1em", "fontWeight": "600"}),
            dbc.Row(id="positions-cards"),
        ], className="mb-4"),

        html.Hr(style={"borderColor": "#444"}),

        # Panel A1: Single Stocks
        html.Div([
            html.H5("SINGLE STOCKS", className="mb-3",
                     style={"color": COLOR_HEADER, "letterSpacing": "0.1em", "fontWeight": "600"}),
            html.Div(id="stocks-table-container"),
        ], className="mb-4"),

        html.Hr(style={"borderColor": "#444"}),

        # Panel A2: Macro Signals
        html.Div([
            html.H5("MACRO SIGNALS", className="mb-3",
                     style={"color": COLOR_HEADER, "letterSpacing": "0.1em", "fontWeight": "600"}),
            html.Div(id="macro-table-container"),
        ], className="mb-4"),

        html.Hr(style={"borderColor": "#444"}),

        # Panel B: Change Log
        html.Div([
            html.H5("CHANGE LOG (7 DAYS)", className="mb-3",
                     style={"color": COLOR_HEADER, "letterSpacing": "0.1em", "fontWeight": "600"}),
            html.Div(id="changelog-table-container"),
        ], className="mb-4"),

        # Footer
        html.Div(
            html.Small(id="footer-text", style={"color": "#666"}),
            className="text-center py-3",
        ),

    ], fluid=True, className="px-4")


# ---------------------------------------------------------------------------
# Page: Live Market Data
# ---------------------------------------------------------------------------

def _market_data_page():
    return dbc.Container([
        dcc.Interval(id="md-refresh-interval", interval=MD_REFRESH_INTERVAL_MS, n_intervals=0),

        dbc.Row([
            dbc.Col(
                html.Div([
                    html.H4("LIVE MARKET DATA", className="mb-0",
                             style={"color": COLOR_HEADER, "letterSpacing": "0.1em",
                                    "fontWeight": "600"}),
                    html.Small("DataBento EQUS.MINI  |  Pre-market + Regular session",
                               style={"color": "#666"}),
                ]),
                width=True,
            ),
            dbc.Col(
                dbc.Button(
                    [html.I(className="fas fa-bolt me-2"), "Live Refresh"],
                    id="md-refresh-button",
                    color="success",
                    size="sm",
                    className="mt-1",
                    style={"letterSpacing": "0.05em", "fontWeight": "600"},
                ),
                width="auto",
                className="d-flex align-items-center",
            ),
        ], className="mb-3 mt-3 align-items-center"),

        html.Div(id="md-table-container"),

        html.Div(
            html.Small(id="md-footer", style={"color": "#666"}),
            className="text-center py-3",
        ),
    ], fluid=True, className="px-4")


# ---------------------------------------------------------------------------
# Layout (multi-page router)
# ---------------------------------------------------------------------------

def build_layout():
    return html.Div([
        dcc.Location(id="url", refresh=False),
        _build_nav(),
        html.Div(id="page-content"),
    ])


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.DARKLY,
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css",
    ],
    title="Nenner Signal Engine",
    suppress_callback_exceptions=True,
)

app.layout = build_layout

# ---------------------------------------------------------------------------
# Health endpoint — returns thread status for external watchdog
# ---------------------------------------------------------------------------

# Module-level refs set by main() so the health endpoint can inspect them
_alert_monitor = None
_email_sched = None
_equity_stream = None


@app.server.route("/health")
def health():
    """Return JSON thread health for the external watchdog."""
    import json
    from flask import Response

    threads = {}
    if _alert_monitor and hasattr(_alert_monitor, "_thread"):
        threads["alert_monitor"] = _alert_monitor._thread.is_alive() if _alert_monitor._thread else False
    if _email_sched and hasattr(_email_sched, "_thread"):
        threads["email_scheduler"] = _email_sched._thread.is_alive() if _email_sched._thread else False
    if _equity_stream:
        threads["equity_stream"] = _equity_stream.is_alive()

    all_healthy = all(threads.values()) if threads else True
    status_code = 200 if all_healthy else 503

    body = json.dumps({"healthy": all_healthy, "threads": threads})
    return Response(body, status=status_code, content_type="application/json")


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def route_page(pathname):
    if pathname == "/market-data":
        return _market_data_page()
    return _signals_page()


@app.callback(
    Output("stats-bar", "children"),
    Output("watchlist-cards", "children"),
    Output("positions-cards", "children"),
    Output("stocks-table-container", "children"),
    Output("macro-table-container", "children"),
    Output("changelog-table-container", "children"),
    Output("footer-text", "children"),
    Input("refresh-interval", "n_intervals"),
    Input("refresh-button", "n_clicks"),
)
def refresh_dashboard(_n, _btn):
    # Stats
    stats = fetch_db_stats()
    stats_bar = make_stats_bar(stats)

    # Watchlist cards – single flowing grid, 6 per row
    wl = fetch_watchlist()
    wl_by_ticker = {r.get("ticker"): r for r in wl}
    wl_all = [make_watchlist_card(wl_by_ticker[t])
              for t in WATCHLIST_TICKERS if t in wl_by_ticker]
    wl_cards = dbc.Row(wl_all)

    # Position cards
    pos_data = fetch_positions()
    pos_cards = [make_position_card(p) for p in pos_data] if pos_data else [
        dbc.Col(html.Div("No positions available (workbook may not be open)",
                         style={"color": "#666", "fontStyle": "italic"}))
    ]

    # Signal board — split into Single Stocks vs Macro
    try:
        from nenner_engine.prices import get_prices_with_signal_context
        sig_conn = get_db()
        state_data = get_prices_with_signal_context(sig_conn, try_t1=True)
        sig_conn.close()
        # Filter to last 3 months (same as old fetch_current_state)
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        state_data = [r for r in state_data if (r.get("last_signal_date") or "") >= cutoff]
    except Exception:
        state_data = fetch_current_state()

    # Merge Profit Factor + Win% from trade_stats
    try:
        conn = get_db()
        instrument_stats = compute_instrument_stats(conn)
        conn.close()
    except Exception:
        instrument_stats = {}

    for row in state_data:
        # Merge PF, Win%, and quant metrics from trade stats
        ts = instrument_stats.get(row["ticker"])
        row["pf"] = ts["profit_factor"] if ts else ""
        row["win_pct"] = ts["win_rate"] if ts else ""
        row["sharpe"] = ts["sharpe"] if ts else ""
        row["score"] = round(ts["composite"] * 100, 1) if ts else ""  # 0-100 scale

        row["origin_price"] = f"{row['origin_price']:,.2f}" if row.get("origin_price") else ""
        row["cancel_level"] = f"{row['cancel_level']:,.2f}" if row.get("cancel_level") else ""
        row["trigger_level"] = f"{row['trigger_level']:,.2f}" if row.get("trigger_level") else ""
        row["implied_reversal"] = 1 if row.get("implied_reversal") else 0

        # Format live price fields (from get_prices_with_signal_context)
        row["price"] = f"{row['price']:,.2f}" if row.get("price") else ""
        pnl = row.get("pnl_pct")
        row["pnl_pct"] = f"{pnl:+.1f}%" if pnl is not None else ""
        cdist = row.get("cancel_dist_pct")
        row["cancel_dist_pct"] = f"{abs(cdist):.1f}%" if cdist is not None else ""

    stocks_data = [r for r in state_data if r.get("asset_class") == "Single Stock"]
    macro_data = [r for r in state_data if r.get("asset_class") != "Single Stock"]

    signal_columns = [
        {"name": "Ticker", "id": "ticker"},
        {"name": "Instrument", "id": "instrument"},
        {"name": "Class", "id": "asset_class"},
        {"name": "Signal", "id": "effective_signal"},
        {"name": "From", "id": "origin_price"},
        {"name": "Cancel", "id": "cancel_level"},
        {"name": "Price", "id": "price"},
        {"name": "P/L%", "id": "pnl_pct", "type": "numeric"},
        {"name": "Dist%", "id": "cancel_dist_pct", "type": "numeric"},
        {"name": "Impl", "id": "implied_reversal"},
        {"name": "Date", "id": "last_signal_date"},
        {"name": "PF", "id": "pf", "type": "numeric"},
        {"name": "Win%", "id": "win_pct", "type": "numeric"},
        {"name": "Sharpe", "id": "sharpe", "type": "numeric"},
        {"name": "Score", "id": "score", "type": "numeric"},
    ]

    stocks_table = dash_table.DataTable(
        id="stocks-board",
        columns=signal_columns,
        data=stocks_data,
        sort_action="native",
        filter_action="native",
        page_size=20,
        style_header=SIGNAL_TABLE_STYLE_HEADER,
        style_cell=SIGNAL_TABLE_STYLE_CELL,
        style_data_conditional=SIGNAL_TABLE_STYLE_DATA_CONDITIONAL,
        style_table={"overflowX": "auto"},
        style_as_list_view=True,
    )

    macro_table = dash_table.DataTable(
        id="macro-board",
        columns=signal_columns,
        data=macro_data,
        sort_action="native",
        filter_action="native",
        page_size=60,
        style_header=SIGNAL_TABLE_STYLE_HEADER,
        style_cell=SIGNAL_TABLE_STYLE_CELL,
        style_data_conditional=SIGNAL_TABLE_STYLE_DATA_CONDITIONAL,
        style_table={"overflowX": "auto"},
        style_as_list_view=True,
    )

    # Change log table
    changes = fetch_recent_changes(days=7)
    for row in changes:
        row["origin_price"] = f"{row['origin_price']:,.2f}" if row.get("origin_price") else ""
        row["cancel_level"] = f"{row['cancel_level']:,.2f}" if row.get("cancel_level") else ""

    changelog_table = dash_table.DataTable(
        id="changelog",
        columns=[
            {"name": "Date", "id": "date"},
            {"name": "Instrument", "id": "instrument"},
            {"name": "Ticker", "id": "ticker"},
            {"name": "Signal", "id": "signal_type"},
            {"name": "Status", "id": "signal_status"},
            {"name": "From", "id": "origin_price"},
            {"name": "Cancel", "id": "cancel_level"},
            {"name": "NTC", "id": "note_the_change"},
        ],
        data=changes,
        sort_action="native",
        filter_action="native",
        page_size=25,
        style_header=SIGNAL_TABLE_STYLE_HEADER,
        style_cell=SIGNAL_TABLE_STYLE_CELL,
        style_data_conditional=CHANGELOG_TABLE_STYLE_DATA_CONDITIONAL,
        style_table={"overflowX": "auto"},
        style_as_list_view=True,
    )

    # Footer with email check status
    from datetime import datetime
    footer_parts = [
        f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Data: {stats['date_min']} to {stats['date_max']}",
        f"Auto-refresh: {REFRESH_INTERVAL_MS // 1000}s",
    ]

    footer = "  |  ".join(footer_parts)


    return stats_bar, wl_cards, pos_cards, stocks_table, macro_table, changelog_table, footer


# ---------------------------------------------------------------------------
# Market Data callback
# ---------------------------------------------------------------------------

MD_TABLE_STYLE_DATA_CONDITIONAL = [
    # Positive change — green
    {"if": {"filter_query": "{chg} > 0", "column_id": "chg"},
     "color": COLOR_BUY, "fontWeight": "bold"},
    {"if": {"filter_query": "{chg_pct} > 0", "column_id": "chg_pct"},
     "color": COLOR_BUY, "fontWeight": "bold"},
    # Negative change — red
    {"if": {"filter_query": "{chg} < 0", "column_id": "chg"},
     "color": COLOR_SELL, "fontWeight": "bold"},
    {"if": {"filter_query": "{chg_pct} < 0", "column_id": "chg_pct"},
     "color": COLOR_SELL, "fontWeight": "bold"},
]


@app.callback(
    Output("md-table-container", "children"),
    Output("md-footer", "children"),
    Input("md-refresh-interval", "n_intervals"),
    Input("md-refresh-button", "n_clicks"),
)
def refresh_market_data(_n, _btn):
    from datetime import datetime

    if _equity_stream is None:
        return (
            html.Div(
                "Equity stream not running. Ensure DataBento credentials are configured.",
                style={"color": "#888", "fontStyle": "italic", "padding": "2rem"},
            ),
            "",
        )

    snapshot = _equity_stream.get_snapshot()
    if not snapshot:
        return (
            html.Div(
                "Waiting for market data...",
                style={"color": "#888", "fontStyle": "italic", "padding": "2rem"},
            ),
            "Stream is connected but no quotes received yet",
        )

    # Get previous session closes for change calculation
    from nenner_engine.equity_stream import STREAM_TICKERS
    prev_closes = _get_prev_closes(STREAM_TICKERS)

    rows = []
    for ticker, quote in snapshot.items():
        display_ticker = _DISPLAY_ALIAS.get(ticker, ticker)
        bid = quote["bid"]
        ask = quote["ask"]
        mid = quote["mid"]
        prev = prev_closes.get(ticker)

        chg = mid - prev if prev else None
        chg_pct = (chg / prev * 100) if prev and prev != 0 else None

        rows.append({
            "ticker": display_ticker,
            "bid": bid if bid > 0 else None,
            "ask": ask if ask > 0 else None,
            "last": mid,
            "chg": round(chg, 2) if chg is not None else None,
            "chg_pct": round(chg_pct, 2) if chg_pct is not None else None,
            "_abs_chg_pct": abs(chg_pct) if chg_pct is not None else 0,
        })

    # Pre-sort by absolute change % descending (most volatile first)
    rows.sort(key=lambda r: r["_abs_chg_pct"], reverse=True)
    # Remove sort key from data
    for r in rows:
        del r["_abs_chg_pct"]

    md_columns = [
        {"name": "Ticker", "id": "ticker"},
        {"name": "Bid", "id": "bid", "type": "numeric",
         "format": {"specifier": ",.2f"}},
        {"name": "Ask", "id": "ask", "type": "numeric",
         "format": {"specifier": ",.2f"}},
        {"name": "Last", "id": "last", "type": "numeric",
         "format": {"specifier": ",.2f"}},
        {"name": "Chg", "id": "chg", "type": "numeric",
         "format": {"specifier": "+,.2f"}},
        {"name": "Chg%", "id": "chg_pct", "type": "numeric",
         "format": {"specifier": "+.2f"}},
    ]

    table = dash_table.DataTable(
        id="md-board",
        columns=md_columns,
        data=rows,
        sort_action="native",
        page_size=30,
        style_header=SIGNAL_TABLE_STYLE_HEADER,
        style_cell={
            **SIGNAL_TABLE_STYLE_CELL,
            "fontSize": "1.5rem",
            "padding": "12px 16px",
        },
        style_data_conditional=MD_TABLE_STYLE_DATA_CONDITIONAL,
        style_table={"overflowX": "auto"},
        style_as_list_view=True,
    )

    now = datetime.now()
    healthy = _equity_stream.is_healthy
    status = "LIVE" if healthy else "STALE"
    status_color = COLOR_BUY if healthy else COLOR_SELL
    footer = html.Span([
        f"Last refresh: {now.strftime('%H:%M:%S')}  |  {len(rows)} instruments  |  Stream: ",
        html.Span(status, style={"color": status_color, "fontWeight": "bold"}),
        f"  |  Auto-refresh: {MD_REFRESH_INTERVAL_MS // 60_000} min",
    ])

    return table, footer


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Nenner Signal Dashboard")
    parser.add_argument("--port", type=int, default=8050, help="Port (default: 8050)")
    parser.add_argument("--db", type=str, default=None, help="Database path")
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    args = parser.parse_args()

    if args.db:
        global DB_PATH
        DB_PATH = args.db

    # Configure logging for monitor threads (logger "nenner" used by all NE modules)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print(f"Starting Nenner Signal Dashboard on http://127.0.0.1:{args.port}")
    print(f"Database: {DB_PATH}")

    # Start background monitor threads (alert evaluator + email scheduler).
    # Only in non-debug mode — Werkzeug's reloader forks a child process
    # and we don't want duplicate threads.
    global _alert_monitor, _email_sched, _equity_stream

    if not args.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        import atexit
        from nenner_engine.alerts import AlertMonitorThread, AlertConfig
        from nenner_engine.email_scheduler import EmailScheduler
        from nenner_engine.equity_stream import EquityStreamThread

        _alert_monitor = AlertMonitorThread(
            db_path=DB_PATH, interval=60, config=AlertConfig(),
        )
        _alert_monitor.start()
        log.info("Alert monitor thread started (60s interval)")

        # Email scheduler disabled in dashboard — NennerEngineMonitor owns it.
        # Running it in both processes caused duplicate stock reports (race on
        # alert_log dedup guard, both fire at 8:30 AM before either writes).
        # _email_sched = EmailScheduler(
        #     db_path=DB_PATH, check_on_start=True, daily_check=True,
        # )
        # _email_sched.start()
        log.info("Email scheduler disabled (owned by NennerEngineMonitor)")

        # DataBento equity stream — live spot prices for watchlist ETFs/equities
        _eq_stop = threading.Event()
        _equity_stream = EquityStreamThread(
            stop_event=_eq_stop, db_path=DB_PATH,
        )
        _equity_stream.start()
        log.info("Equity stream thread started (DataBento EQUS.MINI)")

        def _shutdown():
            log.info("Dashboard shutting down — stopping background threads")
            if _alert_monitor:
                _alert_monitor.stop()
            if _email_sched:
                _email_sched.stop()
            if _equity_stream:
                _eq_stop.set()
                _equity_stream.join(timeout=10)

        atexit.register(_shutdown)

    app.run(debug=args.debug, port=args.port)


if __name__ == "__main__":
    main()
