"""Nenner Signal Dashboard.

Plotly Dash dashboard for monitoring Nenner cycle research signals.
Run via the root `dashboard.py` shim: `python dashboard.py [--port PORT] [--db PATH]`.

Single-process architecture: the dashboard hosts the Dash web UI AND runs
the background monitor threads (alert evaluator + equity stream) in the
same process. The email scheduler is owned by the external
NennerEngineMonitor service, not this process.

Module layout:
  data.py       — DB queries, watchlist config, prev-close cache
  components.py — UI builders (cards, stats bar) + color palette + table styles
  pages.py      — Page layouts (nav, signals, market data) + URL router
  app.py        — Dash app instance + /health endpoint + callbacks
  lifecycle.py  — main() entry point: argparse, migrations, threads, app.run
"""

# Re-export the public surface so tests, scripts, and external callers
# can `import dashboard` and find every symbol that used to live in the
# monolithic dashboard.py.
from .data import (  # noqa: F401
    DB_PATH,
    WATCHLIST_ROW1,
    WATCHLIST_ROW2,
    WATCHLIST_ROW3,
    WATCHLIST_TICKERS,
    fetch_current_state,
    fetch_db_stats,
    fetch_positions,
    fetch_recent_changes,
    fetch_watchlist,
    get_db,
)
from .components import (  # noqa: F401
    CHANGELOG_TABLE_STYLE_DATA_CONDITIONAL,
    COLOR_BUY,
    COLOR_CARD_BG,
    COLOR_HEADER,
    COLOR_IMPLIED,
    COLOR_NEUTRAL,
    COLOR_NTC,
    COLOR_PF_BAD,
    COLOR_PF_GOOD,
    COLOR_PF_OK,
    COLOR_SELL,
    SIGNAL_TABLE_STYLE_CELL,
    SIGNAL_TABLE_STYLE_DATA_CONDITIONAL,
    SIGNAL_TABLE_STYLE_HEADER,
    make_position_card,
    make_stats_bar,
    make_watchlist_card,
    signal_color,
)
from .pages import (  # noqa: F401
    MD_DASHBOARD_REFRESH_MS,
    _build_nav,
    _market_data_page,
    _signals_page,
    build_layout,
)
from .app import (  # noqa: F401
    MD_TABLE_STYLE_DATA_CONDITIONAL,
    app,
    refresh_dashboard,
    refresh_market_data,
    route_page,
)
from .lifecycle import main  # noqa: F401
