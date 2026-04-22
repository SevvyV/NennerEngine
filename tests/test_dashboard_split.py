"""Characterization tests for the dashboard module — pin behavior across
the Phase 3.2 split (monolith dashboard.py → dashboard/ package).

The most valuable test here is the bare import smoke test: `import dashboard`
exercises the entire import graph, every Dash @callback decorator, every
top-level constant. If any submodule can't be found, any callback fails to
register, or any helper function disappears from the public API, this test
fails immediately.

The component / layout snapshots are pinned via to_plotly_json() — Dash's
canonical serialization of a component tree. Comparing JSON trees catches
any structural drift from the split.
"""

import json
from datetime import date

import pytest


# ---------------------------------------------------------------------------
# Smoke: dashboard imports cleanly + exposes the symbols the launcher uses
# ---------------------------------------------------------------------------

def test_dashboard_import_smoke():
    """`import dashboard` must succeed — exercises every callback decorator,
    every submodule import, every module-level constant. Single most valuable
    test for catching refactor breakage."""
    import dashboard  # noqa: F401


def test_dashboard_exposes_app_and_main():
    """The launcher (install_service.ps1, restart_dashboard.bat) invokes
    `python dashboard.py`, which runs main(). The /health probe expects
    `app.server` to exist. Both must remain available from the top-level
    module."""
    import dashboard
    assert hasattr(dashboard, "main"), "dashboard.main() is the entry point"
    assert hasattr(dashboard, "app"), "dashboard.app is the Dash instance"
    # Dash's app.server is the underlying Flask app — used by the /health route
    assert hasattr(dashboard.app, "server")


def test_dashboard_exposes_data_helpers():
    """Tests and other callers import these directly. Public API surface
    that must not regress through the split."""
    import dashboard
    for name in ("get_db", "fetch_current_state", "fetch_recent_changes",
                 "fetch_watchlist", "fetch_positions", "fetch_db_stats",
                 "signal_color", "make_watchlist_card", "make_stats_bar",
                 "make_position_card", "build_layout"):
        assert hasattr(dashboard, name), f"dashboard.{name} missing"


# ---------------------------------------------------------------------------
# Pure-function pins
# ---------------------------------------------------------------------------

def test_signal_color_buy():
    from dashboard import signal_color, COLOR_BUY
    assert signal_color("BUY") == COLOR_BUY


def test_signal_color_sell():
    from dashboard import signal_color, COLOR_SELL
    assert signal_color("SELL") == COLOR_SELL


def test_signal_color_neutral():
    from dashboard import signal_color, COLOR_NEUTRAL
    assert signal_color("") == COLOR_NEUTRAL
    assert signal_color(None) == COLOR_NEUTRAL
    assert signal_color("UNKNOWN") == COLOR_NEUTRAL


# ---------------------------------------------------------------------------
# Layout snapshot — pin the page structure JSON
# ---------------------------------------------------------------------------

def _component_to_json(component):
    """Serialize a Dash component tree to a stable JSON string for diffing.

    `to_plotly_json` returns the canonical dict representation Dash uses on
    the wire — comparing it catches any structural change to the layout.
    Dash only serializes one level deep; nested children show up as their
    repr (e.g. "Interval(id='refresh-interval', ...)").
    """
    return json.dumps(component.to_plotly_json(), sort_keys=True, default=str)


def _collect_ids(component) -> set[str]:
    """Walk a Dash component tree and collect every `id` attribute.

    Dash's to_plotly_json only serializes one level; we have to recurse via
    each component's children attribute to find the rest.
    """
    ids = set()

    def _visit(node):
        # A component or a list of children
        if hasattr(node, "id") and getattr(node, "id", None):
            ids.add(node.id)
        children = getattr(node, "children", None)
        if children is None:
            return
        if isinstance(children, (list, tuple)):
            for c in children:
                _visit(c)
        else:
            _visit(children)

    _visit(component)
    return ids


def test_build_layout_structure_stable():
    """build_layout() returns the top-level multi-page wrapper. The URL
    router and page-content placeholder are how every Dash callback finds
    its targets — both must remain present pre/post split."""
    from dashboard import build_layout
    layout = build_layout()
    ids = _collect_ids(layout)

    assert "url" in ids, "URL location component missing"
    assert "page-content" in ids, "page-content placeholder missing"


def test_signals_page_has_required_callback_targets():
    """Every Output id referenced by the refresh_dashboard callback must
    exist as a component in _signals_page() — otherwise the dashboard
    renders blank panels at runtime."""
    import dashboard
    ids = _collect_ids(dashboard._signals_page())

    required = {
        "refresh-interval", "refresh-button", "stats-bar", "watchlist-cards",
        "positions-cards", "stocks-table-container", "macro-table-container",
        "changelog-table-container", "footer-text",
    }
    missing = required - ids
    assert not missing, (
        f"Signals page missing component ids: {missing} — "
        f"refresh_dashboard callback would write to nothing"
    )


def test_market_data_page_has_required_callback_targets():
    """Same check for the Market Data page callback."""
    import dashboard
    ids = _collect_ids(dashboard._market_data_page())

    required = {"md-refresh-interval", "md-refresh-button",
                "md-table-container", "md-footer"}
    missing = required - ids
    assert not missing, f"Market Data page missing component ids: {missing}"


# ---------------------------------------------------------------------------
# Component snapshots — pin card structures with fixed input
# ---------------------------------------------------------------------------

def test_make_watchlist_card_with_buy_signal():
    """make_watchlist_card output structure for a BUY signal with full data."""
    from dashboard import make_watchlist_card
    row = {
        "ticker": "GC", "instrument": "Gold",
        "effective_signal": "BUY", "implied_reversal": 0,
        "origin_price": 4380.0, "cancel_level": 4350.0,
        "cancel_direction": "BELOW", "cancel_dist_pct": -0.7,
        "price": 4400.0, "price_source": "yfinance",
        "pnl_pct": 0.46,
        "target_price": 4500.0, "target_dist_pct": 2.27,
        "last_signal_date": "2026-04-15",
    }
    card = make_watchlist_card(row)
    js = _component_to_json(card)
    # Key visible content
    assert "GC" in js
    assert "Gold" in js
    assert "BUY" in js
    assert "4,400.00" in js              # price formatted
    assert "+0.5%" in js                  # pnl rounded to 1 dp
    assert "Cancel BELOW 4,350.00" in js


def test_make_watchlist_card_implied_reversal_shows_marker():
    """Implied reversals must show the (impl) marker — visible signal that
    the position was inferred, not directly given by Nenner."""
    from dashboard import make_watchlist_card
    row = {
        "ticker": "ES", "instrument": "S&P 500", "effective_signal": "SELL",
        "implied_reversal": 1,
        "origin_price": 5400.0, "cancel_level": 5450.0,
        "cancel_direction": "ABOVE", "cancel_dist_pct": 0.92,
        "price": 5400.0, "price_source": "yfinance",
        "pnl_pct": 0.0,
        "target_price": None, "target_dist_pct": None,
        "last_signal_date": "2026-04-20",
    }
    card = make_watchlist_card(row)
    js = _component_to_json(card)
    assert "(impl)" in js


def test_make_stats_bar_displays_counts():
    from dashboard import make_stats_bar
    stats = {
        "instruments": 53, "buys": 32, "sells": 21,
        "signals": 1247, "emails": 489,
        "date_min": "2024-01-01", "date_max": "2026-04-22",
    }
    bar = make_stats_bar(stats)
    js = _component_to_json(bar)
    for n in ("53", "32", "21", "1247", "489"):
        assert n in js, f"stats bar missing count {n}"


# ---------------------------------------------------------------------------
# Data layer — exercise fetchers against a seeded in-memory DB
# ---------------------------------------------------------------------------

def test_fetch_current_state_filters_to_recent(monkeypatch, test_db):
    """fetch_current_state filters out signals older than 3 months."""
    from conftest import seed_current_state

    today = date.today().isoformat()
    seed_current_state(test_db, ticker="GC", signal="BUY",
                       last_signal_date=today)
    seed_current_state(test_db, ticker="OLD", instrument="Old",
                       signal="BUY", last_signal_date="2024-01-01")

    # fetch_current_state lives in dashboard.data and looks up get_db in
    # its own module's namespace — patch there, not on the top-level shim.
    import dashboard
    from dashboard import data as _data
    monkeypatch.setattr(_data, "get_db", lambda: test_db)

    rows = dashboard.fetch_current_state()
    tickers = {r["ticker"] for r in rows}
    assert "GC" in tickers
    assert "OLD" not in tickers, "expected 3-month filter to drop ancient signal"


def test_fetch_db_stats_counts_buys_and_sells(monkeypatch, test_db):
    from conftest import seed_current_state

    today = date.today().isoformat()
    seed_current_state(test_db, ticker="GC", signal="BUY", last_signal_date=today)
    seed_current_state(test_db, ticker="SI", instrument="Silver",
                       signal="BUY", last_signal_date=today)
    seed_current_state(test_db, ticker="ES", instrument="S&P 500",
                       signal="SELL", last_signal_date=today)

    import dashboard
    from dashboard import data as _data
    monkeypatch.setattr(_data, "get_db", lambda: test_db)

    stats = dashboard.fetch_db_stats()
    assert stats["instruments"] == 3
    assert stats["buys"] == 2
    assert stats["sells"] == 1
