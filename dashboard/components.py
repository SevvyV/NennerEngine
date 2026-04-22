"""Dashboard UI components — card builders, color palette, table styles.

Pure functions: take dicts, return Dash components. No DB, no callbacks.
Style constants live here too so anything visual is in one module.
"""

from dash import html
import dash_bootstrap_components as dbc


# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------

COLOR_BUY = "#00bc8c"     # green
COLOR_SELL = "#e74c3c"    # red
COLOR_NEUTRAL = "#6c757d" # gray
COLOR_IMPLIED = "#f39c12"  # amber for implied reversals
COLOR_NTC = "#e74c3c"     # note-the-change highlight
COLOR_CARD_BG = "#2b3035"
COLOR_HEADER = "#adb5bd"

COLOR_PF_GOOD = "#00bc8c"    # green — PF >= 2.0
COLOR_PF_OK = "#f39c12"      # amber — PF 1.0–2.0
COLOR_PF_BAD = "#e74c3c"     # red   — PF < 1.0


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
