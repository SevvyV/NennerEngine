"""Dash app instance, /health endpoint, and all callbacks.

This module owns the singleton Dash `app`, the Flask /health route, and
the @callback-decorated functions. Importing it triggers callback
registration as a side effect — that's intentional so `import dashboard`
fully wires up the UI.
"""

import logging
from datetime import datetime, timedelta

import dash
from dash import dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

from nenner_engine.config import DASHBOARD_REFRESH_MS
from nenner_engine.trade_stats import compute_instrument_stats

from . import data as _data
from .components import (
    COLOR_BUY,
    COLOR_HEADER,
    COLOR_IMPLIED,
    COLOR_NTC,
    COLOR_SELL,
    SIGNAL_TABLE_STYLE_CELL,
    SIGNAL_TABLE_STYLE_DATA_CONDITIONAL,
    SIGNAL_TABLE_STYLE_HEADER,
    CHANGELOG_TABLE_STYLE_DATA_CONDITIONAL,
    make_position_card,
    make_stats_bar,
    make_watchlist_card,
)
from .pages import (
    MD_DASHBOARD_REFRESH_MS,
    _market_data_page,
    _signals_page,
    build_layout,
)

log = logging.getLogger("nenner_engine")


# DataBento → display ticker (Market Data only — used inside the callback)
_DISPLAY_ALIAS = {"GOOGL": "GOOG"}


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
    """Return JSON thread + ingestion health for the external watchdog.

    This dashboard intentionally does NOT own the email scheduler (that's
    the external NennerEngineMonitor process). A missing scheduler inside
    this process is normal — but the watchdog still needs to know the
    ingestion pipeline is alive, so we probe the `emails` table for the
    age of the most recent Nenner email and fail hard if it's stale.

    Staleness thresholds:
      - Mon/Wed/Fri after 3 PM ET: 72h (Nenner sends Mon/Wed/Fri)
      - Otherwise: 120h (5-day cap)
    """
    import json

    from flask import Response

    from nenner_engine.tz import ET as et

    threads: dict[str, object] = {}
    if _alert_monitor is not None:
        t = getattr(_alert_monitor, "_thread", None)
        threads["alert_monitor"] = bool(t and t.is_alive())
    else:
        threads["alert_monitor"] = "not_started"

    if _email_sched is not None:
        t = getattr(_email_sched, "_thread", None)
        threads["email_scheduler"] = bool(t and t.is_alive())
    else:
        # Expected: external monitor owns scheduling. Don't claim healthy,
        # don't claim dead — just report the arrangement.
        threads["email_scheduler"] = "external"

    if _equity_stream is not None:
        threads["equity_stream"] = _equity_stream.is_alive()
    else:
        threads["equity_stream"] = "not_started"

    ingestion: dict[str, object] = {}
    try:
        conn = _data.get_db()
        try:
            row = conn.execute(
                "SELECT MAX(date_sent) AS last_date FROM emails"
            ).fetchone()
        finally:
            conn.close()

        last_date_str = row["last_date"] if row else None
        now_et = datetime.now(et)
        ingestion["last_email_date"] = last_date_str

        if last_date_str:
            last_dt = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=et)
            age_hours = (now_et - last_dt).total_seconds() / 3600.0
            ingestion["last_email_age_hours"] = round(age_hours, 1)

            is_nenner_day_pm = now_et.weekday() in (0, 2, 4) and now_et.hour >= 15
            threshold = 72.0 if is_nenner_day_pm else 120.0
            ingestion["threshold_hours"] = threshold
            ingestion["stale"] = age_hours > threshold
        else:
            ingestion["stale"] = True
            ingestion["reason"] = "no emails ingested"
    except Exception as e:
        ingestion["error"] = str(e)
        ingestion["stale"] = True

    thread_ok = all(v is True or v == "external" for v in threads.values())
    ingestion_ok = not ingestion.get("stale", False)
    all_healthy = thread_ok and ingestion_ok
    status_code = 200 if all_healthy else 503

    body = json.dumps({
        "healthy": all_healthy,
        "threads": threads,
        "ingestion": ingestion,
    })
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
    stats = _data.fetch_db_stats()
    stats_bar = make_stats_bar(stats)

    # Watchlist cards – single flowing grid, 6 per row
    wl = _data.fetch_watchlist()
    wl_by_ticker = {r.get("ticker"): r for r in wl}
    wl_all = [make_watchlist_card(wl_by_ticker[t])
              for t in _data.WATCHLIST_TICKERS if t in wl_by_ticker]
    wl_cards = dbc.Row(wl_all)

    # Position cards
    pos_data = _data.fetch_positions()
    pos_cards = [make_position_card(p) for p in pos_data] if pos_data else [
        dbc.Col(html.Div("No positions available (workbook may not be open)",
                         style={"color": "#666", "fontStyle": "italic"}))
    ]

    # Signal board — split into Single Stocks vs Macro
    try:
        from nenner_engine.prices import get_prices_with_signal_context
        sig_conn = _data.get_db()
        state_data = get_prices_with_signal_context(sig_conn, try_t1=True)
        sig_conn.close()
        # Filter to last 3 months (same as old fetch_current_state)
        cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        state_data = [r for r in state_data if (r.get("last_signal_date") or "") >= cutoff]
    except Exception:
        state_data = _data.fetch_current_state()

    # Merge Profit Factor + Win% from trade_stats
    try:
        conn = _data.get_db()
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
    changes = _data.fetch_recent_changes(days=7)
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
    footer_parts = [
        f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Data: {stats['date_min']} to {stats['date_max']}",
        f"Auto-refresh: {DASHBOARD_REFRESH_MS // 1000}s",
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
    prev_closes = _data._get_prev_closes(STREAM_TICKERS)

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
        f"  |  Auto-refresh: {MD_DASHBOARD_REFRESH_MS // 60_000} min",
    ])

    return table, footer
