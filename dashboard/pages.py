"""Dashboard page layouts — nav, signals page, market data page, router.

Each `_*_page` function returns a Dash component tree. Callbacks (in app.py)
target component IDs declared here, so the IDs are effectively part of the
public contract — don't rename without updating callbacks.
"""

from dash import dcc, html
import dash_bootstrap_components as dbc

from nenner_engine.config import DASHBOARD_REFRESH_MS

from .components import COLOR_HEADER

# Market Data page config
MD_DASHBOARD_REFRESH_MS = 900_000  # 15 minutes


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
        dcc.Interval(id="refresh-interval", interval=DASHBOARD_REFRESH_MS, n_intervals=0),

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
        dcc.Interval(id="md-refresh-interval", interval=MD_DASHBOARD_REFRESH_MS, n_intervals=0),

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
