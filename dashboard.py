"""
Nenner Signal Dashboard
========================
Plotly Dash dashboard for monitoring Nenner cycle research signals.
Run: python dashboard.py [--port PORT] [--db PATH]
"""

import argparse
import os
import sqlite3

import dash
from dash import dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nenner_signals.db")
WATCHLIST_TICKERS = ["GC", "SI", "TSLA", "MSFT", "BAC"]
REFRESH_INTERVAL_MS = 30_000  # 30 seconds

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
    """Fetch all current signal states."""
    conn = get_db()
    rows = conn.execute("""
        SELECT ticker, instrument, asset_class, effective_signal,
               origin_price, cancel_direction, cancel_level,
               trigger_level, implied_reversal, last_signal_date
        FROM current_state
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
    """Fetch watchlist instrument states."""
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


def fetch_db_stats():
    """Fetch database summary stats."""
    conn = get_db()
    stats = {
        "emails": conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0],
        "signals": conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0],
        "cycles": conn.execute("SELECT COUNT(*) FROM cycles").fetchone()[0],
        "targets": conn.execute("SELECT COUNT(*) FROM price_targets").fetchone()[0],
        "instruments": conn.execute("SELECT COUNT(*) FROM current_state").fetchone()[0],
    }
    buys = conn.execute("SELECT COUNT(*) FROM current_state WHERE effective_signal='BUY'").fetchone()[0]
    sells = conn.execute("SELECT COUNT(*) FROM current_state WHERE effective_signal='SELL'").fetchone()[0]
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
    """Build a single watchlist instrument card."""
    sig = row["effective_signal"]
    color = signal_color(sig)
    implied = row.get("implied_reversal", 0)

    cancel_text = ""
    if row.get("cancel_level"):
        direction = row.get("cancel_direction", "")
        cancel_text = f"Cancel {direction} {row['cancel_level']:,.2f}"

    badge_children = [sig]
    if implied:
        badge_children = [sig, " ", html.Small("(impl)", style={"color": COLOR_IMPLIED})]

    return dbc.Col(
        dbc.Card([
            dbc.CardHeader(
                html.Div([
                    html.Span(row["ticker"], style={"fontWeight": "bold", "fontSize": "1.3rem"}),
                    html.Span(
                        row["instrument"],
                        className="ms-2",
                        style={"fontSize": "0.85rem", "color": COLOR_HEADER},
                    ),
                ]),
                style={"backgroundColor": COLOR_CARD_BG, "borderBottom": f"3px solid {color}"},
            ),
            dbc.CardBody([
                html.Div(
                    badge_children,
                    style={
                        "fontSize": "1.6rem",
                        "fontWeight": "bold",
                        "color": color,
                        "marginBottom": "0.5rem",
                    },
                ),
                html.Div(f"From {row['origin_price']:,.2f}" if row.get("origin_price") else "",
                         style={"fontSize": "0.9rem", "color": COLOR_HEADER}),
                html.Div(cancel_text,
                         style={"fontSize": "0.85rem", "color": "#888"}),
                html.Div(row.get("last_signal_date", ""),
                         style={"fontSize": "0.8rem", "color": "#666", "marginTop": "0.3rem"}),
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

        # Header
        dbc.Row(
            dbc.Col(
                html.Div([
                    html.H3("NENNER SIGNAL ENGINE", className="mb-0",
                             style={"letterSpacing": "0.15em", "fontWeight": "700"}),
                    html.Small("Vartanian Capital Management",
                               style={"color": COLOR_HEADER, "letterSpacing": "0.05em"}),
                ], className="text-center py-3"),
            ),
            className="mb-2",
        ),

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

        # Panel A: Active Signal Board
        html.Div([
            html.H5("ACTIVE SIGNAL BOARD", className="mb-3",
                     style={"color": COLOR_HEADER, "letterSpacing": "0.1em", "fontWeight": "600"}),
            html.Div(id="signal-table-container"),
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
    external_stylesheets=[dbc.themes.DARKLY],
    title="Nenner Signal Engine",
)

app.layout = build_layout


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

@app.callback(
    Output("stats-bar", "children"),
    Output("watchlist-cards", "children"),
    Output("signal-table-container", "children"),
    Output("changelog-table-container", "children"),
    Output("footer-text", "children"),
    Input("refresh-interval", "n_intervals"),
)
def refresh_dashboard(_n):
    # Stats
    stats = fetch_db_stats()
    stats_bar = make_stats_bar(stats)

    # Watchlist cards
    wl = fetch_watchlist()
    wl_cards = [make_watchlist_card(r) for r in wl]

    # Signal board table
    state_data = fetch_current_state()
    for row in state_data:
        row["origin_price"] = f"{row['origin_price']:,.2f}" if row.get("origin_price") else ""
        row["cancel_level"] = f"{row['cancel_level']:,.2f}" if row.get("cancel_level") else ""
        row["trigger_level"] = f"{row['trigger_level']:,.2f}" if row.get("trigger_level") else ""
        row["implied_reversal"] = 1 if row.get("implied_reversal") else 0

    signal_table = dash_table.DataTable(
        id="signal-board",
        columns=[
            {"name": "Ticker", "id": "ticker"},
            {"name": "Instrument", "id": "instrument"},
            {"name": "Class", "id": "asset_class"},
            {"name": "Signal", "id": "effective_signal"},
            {"name": "From", "id": "origin_price"},
            {"name": "Cancel Dir", "id": "cancel_direction"},
            {"name": "Cancel", "id": "cancel_level"},
            {"name": "Trigger", "id": "trigger_level"},
            {"name": "Impl", "id": "implied_reversal"},
            {"name": "Date", "id": "last_signal_date"},
        ],
        data=state_data,
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

    # Footer
    from datetime import datetime
    footer = f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  Data: {stats['date_min']} to {stats['date_max']}  |  Auto-refresh: {REFRESH_INTERVAL_MS // 1000}s"

    return stats_bar, wl_cards, signal_table, changelog_table, footer


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

    print(f"Starting Nenner Signal Dashboard on http://127.0.0.1:{args.port}")
    print(f"Database: {DB_PATH}")
    app.run(debug=args.debug, port=args.port)


if __name__ == "__main__":
    main()
