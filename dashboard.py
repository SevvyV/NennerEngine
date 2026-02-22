"""
Nenner Signal Dashboard
========================
Plotly Dash dashboard for monitoring Nenner cycle research signals.
Run: python dashboard.py [--port PORT] [--db PATH]

Automatically checks for new Nenner emails:
  - On dashboard startup (immediate)
  - Every 30 minutes from 8:00–11:00 AM Eastern Time
  - On manual refresh button click
"""

import argparse
import os
import sqlite3

import dash
from dash import dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

from nenner_engine.trade_stats import compute_instrument_stats

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nenner_signals.db")
WATCHLIST_TICKERS = ["GC", "SI", "TSLA", "MSFT", "BAC", "SOYB"]
REFRESH_INTERVAL_MS = 30_000  # 30 seconds

# Email scheduler singleton (initialized in main())
_email_scheduler = None

# Color palette
COLOR_BUY = "#00bc8c"     # green
COLOR_SELL = "#e74c3c"    # red
COLOR_NEUTRAL = "#6c757d" # gray
COLOR_IMPLIED = "#f39c12"  # amber for implied reversals
COLOR_NTC = "#e74c3c"     # note-the-change highlight
COLOR_CARD_BG = "#2b3035"
COLOR_HEADER = "#adb5bd"


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
                html.Div(row.get("last_signal_date", ""),
                         style={"fontSize": "0.75rem", "color": "#666", "marginTop": "0.2rem"}),
            ], style={"backgroundColor": "#1e2226"}),
        ], className="h-100", style={"border": "1px solid #444"}),
        xs=12, sm=6, md=4, lg=True,
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
    "fontSize": "0.85rem",
}

SIGNAL_TABLE_STYLE_CELL = {
    "backgroundColor": "#1e2226",
    "color": "#e0e0e0",
    "border": "1px solid #333",
    "fontSize": "0.85rem",
    "padding": "6px 10px",
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
# Layout
# ---------------------------------------------------------------------------

def build_layout():
    return dbc.Container([
        # Auto-refresh interval
        dcc.Interval(id="refresh-interval", interval=REFRESH_INTERVAL_MS, n_intervals=0),

        # Header with Refresh button
        dbc.Row([
            dbc.Col(
                html.Div([
                    html.H3("NENNER SIGNAL ENGINE", className="mb-0",
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
            dbc.Row(id="watchlist-cards"),
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
# App
# ---------------------------------------------------------------------------

app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.DARKLY,
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css",
    ],
    title="Nenner Signal Engine",
)

app.layout = build_layout


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

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
    # Trigger email check on manual refresh button click
    ctx = dash.callback_context
    if ctx.triggered and ctx.triggered[0]["prop_id"] == "refresh-button.n_clicks":
        if _email_scheduler and _btn:
            try:
                _email_scheduler.trigger_now()
            except Exception as e:
                print(f"Warning: Manual email check failed: {e}")

    # Stats
    stats = fetch_db_stats()
    stats_bar = make_stats_bar(stats)

    # Watchlist cards
    wl = fetch_watchlist()
    wl_cards = [make_watchlist_card(r) for r in wl]

    # Position cards
    pos_data = fetch_positions()
    pos_cards = [make_position_card(p) for p in pos_data] if pos_data else [
        dbc.Col(html.Div("No positions available (workbook may not be open)",
                         style={"color": "#666", "fontStyle": "italic"}))
    ]

    # Signal board — split into Single Stocks vs Macro
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

    stocks_data = [r for r in state_data if r.get("asset_class") == "Single Stock"]
    macro_data = [r for r in state_data if r.get("asset_class") != "Single Stock"]

    signal_columns = [
        {"name": "Ticker", "id": "ticker"},
        {"name": "Instrument", "id": "instrument"},
        {"name": "Class", "id": "asset_class"},
        {"name": "Signal", "id": "effective_signal"},
        {"name": "From", "id": "origin_price"},
        {"name": "Cancel", "id": "cancel_level"},
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

    # Add email scheduler status
    if _email_scheduler and _email_scheduler.last_result:
        er = _email_scheduler.last_result
        ts = er.get("timestamp", "?")
        try:
            ts_short = datetime.fromisoformat(ts).strftime("%H:%M:%S")
        except Exception:
            ts_short = ts
        trigger = er.get("trigger", "?")
        new_ct = er.get("new_emails", 0)
        err = er.get("error")
        if err:
            footer_parts.append(f"Email check: ERROR at {ts_short}")
        else:
            footer_parts.append(
                f"Email check: {new_ct} new @ {ts_short} ({trigger})"
            )

    footer = "  |  ".join(footer_parts)

    return stats_bar, wl_cards, pos_cards, stocks_table, macro_table, changelog_table, footer


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Nenner Signal Dashboard")
    parser.add_argument("--port", type=int, default=8050, help="Port (default: 8050)")
    parser.add_argument("--db", type=str, default=None, help="Database path")
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    parser.add_argument("--no-email-check", action="store_true",
                        help="Disable automatic email checking")
    args = parser.parse_args()

    if args.db:
        global DB_PATH
        DB_PATH = args.db

    # --- Start email scheduler (checks on launch + daily at 8 AM ET) ---
    global _email_scheduler
    if not args.no_email_check:
        try:
            from nenner_engine.email_scheduler import EmailScheduler
            _email_scheduler = EmailScheduler(
                db_path=DB_PATH,
                check_on_start=True,
                daily_check=True,
                interval_minutes=30,
                interval_window=(8, 11),  # 8:00-11:00 AM ET
            )
            _email_scheduler.start()
            print("Email scheduler started (on launch + every 30min 8-11AM ET + manual refresh)")
        except Exception as e:
            print(f"Warning: Email scheduler failed to start: {e}")
            print("Dashboard will run without automatic email checking.")
    else:
        print("Email checking disabled (--no-email-check)")

    print(f"Starting Nenner Signal Dashboard on http://127.0.0.1:{args.port}")
    print(f"Database: {DB_PATH}")
    app.run(debug=args.debug, port=args.port)


if __name__ == "__main__":
    main()
