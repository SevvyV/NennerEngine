"""
Nenner Signal Performance Report
==================================
Analyzes all historical signals from the NennerEngine database to determine:
  - Per-instrument profitability (round-trip trades using Nenner's origin prices)
  - Win rate by instrument and asset class
  - Signal quality patterns (NTC, CANCELLED, BUY vs SELL)
  - Time-based trends (yearly, monthly)
  - Actionable takeaways for a trader

Methodology:
  - A "trade" is a completed round-trip: entry at one signal's origin price,
    exit when the direction flips (next opposing signal's origin price).
  - Only signals with valid origin prices are used.
  - Duplicate/confirmation signals (same direction, same instrument, same date)
    are collapsed -only the first signal per direction change is used.
  - CANCELLED signals are tracked separately as a quality indicator.

Output: PDF report saved to E:\\AI_Workspace\\NennerEngine\\performance_reports\\
"""

import sqlite3
import os
from datetime import datetime
from collections import defaultdict

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from fpdf import FPDF

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nenner_signals.db")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "performance_reports")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
PDF_PATH = os.path.join(OUTPUT_DIR, f"nenner_signal_performance_{TIMESTAMP}.pdf")
CHART_DIR = os.path.join(OUTPUT_DIR, "charts")
os.makedirs(CHART_DIR, exist_ok=True)

# Minimum trades to include in per-instrument analysis
MIN_TRADES = 3

# Date filters - only analyze recent, relevant data
MACRO_CUTOFF = "2023-02-21"       # 3 years back from today
SINGLE_STOCK_CUTOFF = "2025-11-01"  # Nov 2025 - when the service began

# Tradeable universe: ETFs, single stocks, and VIX (excludable futures/FX)
TRADEABLE_ASSET_CLASSES = {
    "Agriculture ETF", "Crypto ETF", "Currency ETF", "Energy ETF",
    "Fixed Income ETF", "Precious Metals ETF", "Precious Metals Stock",
    "Single Stock", "Volatility",
}

# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_signals():
    """Load signals from DB with date and tradeable-universe filters applied.

    - Only ETFs, single stocks, precious metals stocks, and VIX
    - Single Stock signals: since Nov 2025 (when subscription began)
    - All other tradeable signals: last 3 years
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT id, date, instrument, ticker, asset_class, signal_type, signal_status,
               origin_price, cancel_direction, cancel_level, note_the_change, email_id
        FROM signals
        WHERE (asset_class = 'Single Stock' AND date >= :stock_cutoff)
           OR (asset_class != 'Single Stock' AND date >= :macro_cutoff)
        ORDER BY ticker, date ASC, id ASC
    """, conn, params={"stock_cutoff": SINGLE_STOCK_CUTOFF, "macro_cutoff": MACRO_CUTOFF})
    conn.close()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Filter to tradeable universe only (ETFs, stocks, VIX)
    df = df[df["asset_class"].isin(TRADEABLE_ASSET_CLASSES)].copy()

    return df


# ---------------------------------------------------------------------------
# Trade Extraction -Round-Trip Matching
# ---------------------------------------------------------------------------

def extract_trades(df):
    """
    For each ticker, walk through signals chronologically. When direction
    changes (BUY→SELL or SELL→BUY), that's a trade exit / new entry.

    A trade = {ticker, instrument, asset_class, entry_date, entry_price,
               entry_signal, exit_date, exit_price, exit_signal, pnl_pct,
               holding_days, entry_ntc, cancelled_during}
    """
    # Maximum plausible single-trade return (%). Anything beyond this is
    # almost certainly a misparse (e.g., origin_price=74.7 for FTSE ~7500).
    MAX_SINGLE_TRADE_PCT = 200.0

    trades = []

    for ticker, group in df.groupby("ticker"):
        # Filter to signals with valid origin prices
        g = group[group["origin_price"].notna() & (group["origin_price"] > 0)].copy()
        if len(g) < 2:
            continue

        # Collapse to direction changes only -take first signal when direction flips
        prev_direction = None
        direction_changes = []

        for _, row in g.iterrows():
            sig = row["signal_type"]
            if sig not in ("BUY", "SELL"):
                continue
            if sig != prev_direction:
                direction_changes.append(row)
                prev_direction = sig

        if len(direction_changes) < 2:
            continue

        # Count cancellations between direction changes for quality metric
        all_statuses = group["signal_status"].values
        all_dates = group["date"].values

        # Build trades from consecutive direction changes
        for i in range(len(direction_changes) - 1):
            entry = direction_changes[i]
            exit_ = direction_changes[i + 1]

            entry_price = entry["origin_price"]
            exit_price = exit_["origin_price"]
            entry_signal = entry["signal_type"]

            # P&L: BUY entry → profit if exit > entry; SELL entry → profit if exit < entry
            if entry_signal == "BUY":
                pnl_pct = ((exit_price - entry_price) / entry_price) * 100
            else:  # SELL
                pnl_pct = ((entry_price - exit_price) / entry_price) * 100

            # Skip obvious misparses (e.g., origin_price off by 100x)
            if abs(pnl_pct) > MAX_SINGLE_TRADE_PCT:
                continue

            # Count cancellations between entry and exit dates
            mask = (group["date"] >= entry["date"]) & (group["date"] <= exit_["date"])
            cancelled_count = (group.loc[mask, "signal_status"] == "CANCELLED").sum()

            holding_days = (exit_["date"] - entry["date"]).days

            trades.append({
                "ticker": ticker,
                "instrument": entry["instrument"],
                "asset_class": entry["asset_class"],
                "entry_date": entry["date"],
                "entry_price": entry_price,
                "entry_signal": entry_signal,
                "exit_date": exit_["date"],
                "exit_price": exit_price,
                "exit_signal": exit_["signal_type"],
                "pnl_pct": pnl_pct,
                "holding_days": holding_days,
                "entry_ntc": entry["note_the_change"],
                "cancelled_during": cancelled_count,
                "entry_year": entry["date"].year,
            })

    return pd.DataFrame(trades)


# ---------------------------------------------------------------------------
# Analysis Functions
# ---------------------------------------------------------------------------

def analyze_by_instrument(trades_df):
    """Per-instrument performance summary."""
    results = []
    for ticker, group in trades_df.groupby("ticker"):
        n = len(group)
        if n < MIN_TRADES:
            continue
        wins = (group["pnl_pct"] > 0).sum()
        losses = (group["pnl_pct"] <= 0).sum()
        avg_pnl = group["pnl_pct"].mean()
        median_pnl = group["pnl_pct"].median()
        total_pnl = group["pnl_pct"].sum()
        avg_win = group.loc[group["pnl_pct"] > 0, "pnl_pct"].mean() if wins > 0 else 0
        avg_loss = group.loc[group["pnl_pct"] <= 0, "pnl_pct"].mean() if losses > 0 else 0
        median_win = group.loc[group["pnl_pct"] > 0, "pnl_pct"].median() if wins > 0 else 0
        median_loss = group.loc[group["pnl_pct"] <= 0, "pnl_pct"].median() if losses > 0 else 0
        best = group["pnl_pct"].max()
        worst = group["pnl_pct"].min()
        avg_hold = group["holding_days"].mean()
        win_rate = wins / n * 100
        # Profit factor = gross gains / abs(gross losses)
        gross_gains = group.loc[group["pnl_pct"] > 0, "pnl_pct"].sum()
        gross_losses = abs(group.loc[group["pnl_pct"] <= 0, "pnl_pct"].sum())
        profit_factor = gross_gains / gross_losses if gross_losses > 0 else float("inf")

        results.append({
            "ticker": ticker,
            "instrument": group["instrument"].iloc[0],
            "asset_class": group["asset_class"].iloc[0],
            "trades": n,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "avg_pnl_pct": avg_pnl,
            "median_pnl_pct": median_pnl,
            "total_pnl_pct": total_pnl,
            "avg_win_pct": avg_win,
            "avg_loss_pct": avg_loss,
            "median_win_pct": median_win,
            "median_loss_pct": median_loss,
            "best_pct": best,
            "worst_pct": worst,
            "profit_factor": profit_factor,
            "avg_holding_days": avg_hold,
        })

    return pd.DataFrame(results).sort_values("total_pnl_pct", ascending=False)


def analyze_by_asset_class(trades_df):
    """Performance by asset class."""
    results = []
    for ac, group in trades_df.groupby("asset_class"):
        n = len(group)
        wins = (group["pnl_pct"] > 0).sum()
        win_rate = wins / n * 100
        avg_pnl = group["pnl_pct"].mean()
        total_pnl = group["pnl_pct"].sum()
        tickers = group["ticker"].nunique()
        gross_gains = group.loc[group["pnl_pct"] > 0, "pnl_pct"].sum()
        gross_losses = abs(group.loc[group["pnl_pct"] <= 0, "pnl_pct"].sum())
        profit_factor = gross_gains / gross_losses if gross_losses > 0 else float("inf")

        results.append({
            "asset_class": ac,
            "tickers": tickers,
            "trades": n,
            "wins": wins,
            "win_rate": win_rate,
            "avg_pnl_pct": avg_pnl,
            "total_pnl_pct": total_pnl,
            "profit_factor": profit_factor,
        })

    return pd.DataFrame(results).sort_values("total_pnl_pct", ascending=False)


def analyze_by_year(trades_df):
    """Performance by entry year."""
    results = []
    for year, group in trades_df.groupby("entry_year"):
        n = len(group)
        wins = (group["pnl_pct"] > 0).sum()
        win_rate = wins / n * 100
        avg_pnl = group["pnl_pct"].mean()
        total_pnl = group["pnl_pct"].sum()

        results.append({
            "year": int(year),
            "trades": n,
            "wins": wins,
            "win_rate": win_rate,
            "avg_pnl_pct": avg_pnl,
            "total_pnl_pct": total_pnl,
        })

    return pd.DataFrame(results).sort_values("year")


def analyze_ntc_impact(trades_df):
    """Compare Note-the-Change signals vs regular signals."""
    ntc = trades_df[trades_df["entry_ntc"] == 1]
    regular = trades_df[trades_df["entry_ntc"] == 0]

    def stats(group, label):
        n = len(group)
        if n == 0:
            return None
        wins = (group["pnl_pct"] > 0).sum()
        return {
            "type": label,
            "trades": n,
            "win_rate": wins / n * 100,
            "avg_pnl_pct": group["pnl_pct"].mean(),
            "median_pnl_pct": group["pnl_pct"].median(),
            "total_pnl_pct": group["pnl_pct"].sum(),
        }

    rows = [stats(ntc, "Note-the-Change"), stats(regular, "Regular Signal")]
    return pd.DataFrame([r for r in rows if r is not None])


def analyze_buy_vs_sell(trades_df):
    """Compare BUY entries vs SELL entries."""
    buys = trades_df[trades_df["entry_signal"] == "BUY"]
    sells = trades_df[trades_df["entry_signal"] == "SELL"]

    def stats(group, label):
        n = len(group)
        if n == 0:
            return None
        wins = (group["pnl_pct"] > 0).sum()
        return {
            "direction": label,
            "trades": n,
            "win_rate": wins / n * 100,
            "avg_pnl_pct": group["pnl_pct"].mean(),
            "total_pnl_pct": group["pnl_pct"].sum(),
        }

    rows = [stats(buys, "BUY entries"), stats(sells, "SELL entries")]
    return pd.DataFrame([r for r in rows if r is not None])


def analyze_cancellation_impact(trades_df):
    """Do trades with cancellations during the hold perform differently?"""
    has_cancel = trades_df[trades_df["cancelled_during"] > 0]
    no_cancel = trades_df[trades_df["cancelled_during"] == 0]

    def stats(group, label):
        n = len(group)
        if n == 0:
            return None
        wins = (group["pnl_pct"] > 0).sum()
        return {
            "type": label,
            "trades": n,
            "win_rate": wins / n * 100,
            "avg_pnl_pct": group["pnl_pct"].mean(),
            "total_pnl_pct": group["pnl_pct"].sum(),
        }

    rows = [stats(has_cancel, "Had Cancellations"), stats(no_cancel, "No Cancellations")]
    return pd.DataFrame([r for r in rows if r is not None])


def analyze_holding_period(trades_df):
    """Performance by holding period buckets."""
    bins = [0, 7, 14, 30, 60, 90, 180, 9999]
    labels = ["<1w", "1-2w", "2w-1m", "1-2m", "2-3m", "3-6m", "6m+"]
    trades_df = trades_df.copy()
    trades_df["hold_bucket"] = pd.cut(trades_df["holding_days"], bins=bins, labels=labels, right=True)

    results = []
    for bucket, group in trades_df.groupby("hold_bucket", observed=True):
        n = len(group)
        if n < 3:
            continue
        wins = (group["pnl_pct"] > 0).sum()
        results.append({
            "holding_period": str(bucket),
            "trades": n,
            "win_rate": wins / n * 100,
            "avg_pnl_pct": group["pnl_pct"].mean(),
        })

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Chart Generation
# ---------------------------------------------------------------------------

def set_chart_style():
    """Professional dark theme matching the dashboard."""
    plt.rcParams.update({
        "figure.facecolor": "#1e2226",
        "axes.facecolor": "#1e2226",
        "axes.edgecolor": "#444",
        "axes.labelcolor": "#adb5bd",
        "text.color": "#e0e0e0",
        "xtick.color": "#adb5bd",
        "ytick.color": "#adb5bd",
        "grid.color": "#333",
        "grid.alpha": 0.5,
        "font.size": 10,
    })


def chart_top_instruments(inst_df, n=20):
    """Bar chart of top N instruments by total P&L."""
    set_chart_style()
    top = inst_df.head(n)

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = ["#00bc8c" if x > 0 else "#e74c3c" for x in top["total_pnl_pct"]]
    bars = ax.barh(range(len(top)), top["total_pnl_pct"], color=colors)
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels([f"{r['ticker']} ({r['instrument']})" for _, r in top.iterrows()], fontsize=8)
    ax.set_xlabel("Cumulative Return (%)")
    ax.set_title("Top 20 Instruments by Cumulative P&L", fontsize=14, fontweight="bold")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    path = os.path.join(CHART_DIR, "top_instruments.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def chart_bottom_instruments(inst_df, n=15):
    """Bar chart of worst N instruments by total P&L."""
    set_chart_style()
    bottom = inst_df.tail(n).iloc[::-1]

    fig, ax = plt.subplots(figsize=(12, 5))
    colors = ["#00bc8c" if x > 0 else "#e74c3c" for x in bottom["total_pnl_pct"]]
    bars = ax.barh(range(len(bottom)), bottom["total_pnl_pct"], color=colors)
    ax.set_yticks(range(len(bottom)))
    ax.set_yticklabels([f"{r['ticker']} ({r['instrument']})" for _, r in bottom.iterrows()], fontsize=8)
    ax.set_xlabel("Cumulative Return (%)")
    ax.set_title("Bottom 15 Instruments by Cumulative P&L", fontsize=14, fontweight="bold")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    path = os.path.join(CHART_DIR, "bottom_instruments.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def chart_win_rate(inst_df, min_trades=5):
    """Scatter: win rate vs number of trades, sized by total P&L."""
    set_chart_style()
    data = inst_df[inst_df["trades"] >= min_trades].copy()

    fig, ax = plt.subplots(figsize=(10, 6))
    sizes = np.clip(np.abs(data["total_pnl_pct"]) * 2, 20, 500)
    colors = ["#00bc8c" if x > 0 else "#e74c3c" for x in data["total_pnl_pct"]]

    ax.scatter(data["trades"], data["win_rate"], s=sizes, c=colors, alpha=0.7, edgecolors="#666")

    for _, row in data.iterrows():
        ax.annotate(row["ticker"], (row["trades"], row["win_rate"]),
                    fontsize=6, ha="center", va="bottom", color="#adb5bd")

    ax.axhline(y=50, color="#f39c12", linestyle="--", alpha=0.5, label="50% Win Rate")
    ax.set_xlabel("Number of Trades")
    ax.set_ylabel("Win Rate (%)")
    ax.set_title("Win Rate vs Trade Count (size = |total P&L|)", fontsize=14, fontweight="bold")
    ax.legend(facecolor="#2b3035", edgecolor="#444")
    ax.grid(alpha=0.3)
    plt.tight_layout()
    path = os.path.join(CHART_DIR, "win_rate_scatter.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def chart_asset_class(ac_df):
    """Horizontal bar chart by asset class."""
    set_chart_style()
    data = ac_df.sort_values("total_pnl_pct")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Total P&L
    ax = axes[0]
    colors = ["#00bc8c" if x > 0 else "#e74c3c" for x in data["total_pnl_pct"]]
    ax.barh(range(len(data)), data["total_pnl_pct"], color=colors)
    ax.set_yticks(range(len(data)))
    ax.set_yticklabels(data["asset_class"], fontsize=8)
    ax.set_xlabel("Cumulative Return (%)")
    ax.set_title("Total P&L by Asset Class", fontsize=12, fontweight="bold")
    ax.grid(axis="x", alpha=0.3)

    # Win rate
    ax = axes[1]
    colors2 = ["#00bc8c" if x > 50 else "#f39c12" if x > 40 else "#e74c3c" for x in data["win_rate"]]
    ax.barh(range(len(data)), data["win_rate"], color=colors2)
    ax.set_yticks(range(len(data)))
    ax.set_yticklabels(data["asset_class"], fontsize=8)
    ax.set_xlabel("Win Rate (%)")
    ax.set_title("Win Rate by Asset Class", fontsize=12, fontweight="bold")
    ax.axvline(x=50, color="#f39c12", linestyle="--", alpha=0.5)
    ax.grid(axis="x", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(CHART_DIR, "asset_class.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def chart_yearly_performance(year_df):
    """Bar chart of yearly performance."""
    set_chart_style()
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Win rate by year
    ax = axes[0]
    colors = ["#00bc8c" if x > 50 else "#e74c3c" for x in year_df["win_rate"]]
    ax.bar(year_df["year"].astype(str), year_df["win_rate"], color=colors)
    ax.axhline(y=50, color="#f39c12", linestyle="--", alpha=0.5)
    ax.set_xlabel("Year")
    ax.set_ylabel("Win Rate (%)")
    ax.set_title("Win Rate by Year", fontsize=12, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)

    # Avg P&L by year
    ax = axes[1]
    colors2 = ["#00bc8c" if x > 0 else "#e74c3c" for x in year_df["avg_pnl_pct"]]
    ax.bar(year_df["year"].astype(str), year_df["avg_pnl_pct"], color=colors2)
    ax.axhline(y=0, color="#666", linestyle="-", alpha=0.5)
    ax.set_xlabel("Year")
    ax.set_ylabel("Avg P&L per Trade (%)")
    ax.set_title("Average Trade Return by Year", fontsize=12, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(CHART_DIR, "yearly_performance.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def chart_signal_quality(ntc_df, buy_sell_df, cancel_df):
    """Compare signal quality dimensions."""
    set_chart_style()
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    # NTC impact
    ax = axes[0]
    if len(ntc_df) > 0:
        colors = ["#f39c12", "#6c757d"]
        ax.bar(ntc_df["type"], ntc_df["win_rate"], color=colors[:len(ntc_df)])
        ax.axhline(y=50, color="#e74c3c", linestyle="--", alpha=0.5)
        for i, row in ntc_df.iterrows():
            ax.text(i, row["win_rate"] + 1, f'{row["win_rate"]:.1f}%', ha="center", fontsize=9, color="#e0e0e0")
    ax.set_ylabel("Win Rate (%)")
    ax.set_title("Note-the-Change Impact", fontsize=11, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)

    # BUY vs SELL
    ax = axes[1]
    if len(buy_sell_df) > 0:
        colors = ["#00bc8c", "#e74c3c"]
        ax.bar(buy_sell_df["direction"], buy_sell_df["win_rate"], color=colors[:len(buy_sell_df)])
        ax.axhline(y=50, color="#f39c12", linestyle="--", alpha=0.5)
        for i, row in buy_sell_df.iterrows():
            ax.text(i, row["win_rate"] + 1, f'{row["win_rate"]:.1f}%', ha="center", fontsize=9, color="#e0e0e0")
    ax.set_ylabel("Win Rate (%)")
    ax.set_title("BUY vs SELL Entries", fontsize=11, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)

    # Cancellation impact
    ax = axes[2]
    if len(cancel_df) > 0:
        colors = ["#e74c3c", "#00bc8c"]
        ax.bar(cancel_df["type"], cancel_df["win_rate"], color=colors[:len(cancel_df)])
        ax.axhline(y=50, color="#f39c12", linestyle="--", alpha=0.5)
        for i, row in cancel_df.iterrows():
            ax.text(i, row["win_rate"] + 1, f'{row["win_rate"]:.1f}%', ha="center", fontsize=9, color="#e0e0e0")
    ax.set_ylabel("Win Rate (%)")
    ax.set_title("Cancellation Impact", fontsize=11, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(CHART_DIR, "signal_quality.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def chart_profit_factor(inst_df, min_trades=3):
    """Full profit factor chart: ALL instruments sorted descending by PF."""
    set_chart_style()
    # Cap extreme PFs for display, keep all instruments
    data = inst_df[inst_df["trades"] >= min_trades].copy()
    data["pf_display"] = data["profit_factor"].clip(upper=10)
    data = data.sort_values("profit_factor", ascending=True)  # ascending so highest is at top of horizontal bar

    fig_height = max(6, len(data) * 0.35)
    fig, ax = plt.subplots(figsize=(12, fig_height))

    colors = []
    for pf in data["profit_factor"]:
        if pf >= 2.0:
            colors.append("#00bc8c")    # green - excellent
        elif pf >= 1.5:
            colors.append("#20c997")    # light green - strong
        elif pf >= 1.0:
            colors.append("#f39c12")    # amber - marginal
        else:
            colors.append("#e74c3c")    # red - losing

    bars = ax.barh(range(len(data)), data["pf_display"], color=colors)

    # Add actual PF value labels on bars
    for i, (_, row) in enumerate(data.iterrows()):
        pf = row["profit_factor"]
        label = f'{pf:.2f}' if pf < 10 else f'{pf:.1f}' if pf < 100 else f'{pf:.0f}'
        x_pos = min(row["pf_display"], 9.5)
        ax.text(x_pos + 0.1, i, label, va="center", fontsize=7, color="#e0e0e0")

    ax.set_yticks(range(len(data)))
    ax.set_yticklabels(
        [f"{r['ticker']} ({r['instrument'][:18]})" for _, r in data.iterrows()],
        fontsize=7,
    )
    ax.axvline(x=1.0, color="#e74c3c", linestyle="--", alpha=0.8, linewidth=1.5, label="Breakeven (1.0)")
    ax.axvline(x=1.5, color="#f39c12", linestyle=":", alpha=0.5, label="Strong (1.5)")
    ax.axvline(x=2.0, color="#00bc8c", linestyle=":", alpha=0.5, label="Excellent (2.0)")
    ax.set_xlabel("Profit Factor (capped at 10 for display)")
    ax.set_title("ALL Instruments Ranked by Profit Factor (Descending)", fontsize=14, fontweight="bold")
    ax.legend(facecolor="#2b3035", edgecolor="#444", fontsize=8, loc="lower right")
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    path = os.path.join(CHART_DIR, "profit_factor.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# PDF Report Generation
# ---------------------------------------------------------------------------

class NennerPDF(FPDF):
    """Custom PDF with professional formatting."""

    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(150, 150, 150)
        self.cell(0, 5, "NENNER SIGNAL PERFORMANCE REPORT  |  VARTANIAN CAPITAL MANAGEMENT", 0, 1, "C")
        self.ln(2)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(130, 130, 130)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}  |  Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}", 0, 0, "C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(0, 0, 0)
        self.cell(0, 10, title, 0, 1, "L")
        self.set_draw_color(0, 188, 140)
        self.set_line_width(0.5)
        self.line(self.get_x(), self.get_y(), self.get_x() + 190, self.get_y())
        self.ln(4)

    def subsection_title(self, title):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(50, 50, 50)
        self.cell(0, 8, title, 0, 1, "L")
        self.ln(1)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5, text)
        self.ln(2)

    def key_stat(self, label, value, color=None):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(80, 80, 80)
        self.cell(55, 6, label + ":", 0, 0)
        self.set_font("Helvetica", "B", 10)
        if color:
            self.set_text_color(*color)
        else:
            self.set_text_color(0, 0, 0)
        self.cell(0, 6, str(value), 0, 1)

    def add_table(self, headers, data, col_widths=None, highlight_col=None):
        """Add a formatted table."""
        if col_widths is None:
            col_widths = [190 / len(headers)] * len(headers)

        # Header row
        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(43, 48, 53)
        self.set_text_color(173, 181, 189)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, 1, 0, "C", True)
        self.ln()

        # Data rows
        self.set_font("Helvetica", "", 8)
        for row_idx, row in enumerate(data):
            if row_idx % 2 == 0:
                self.set_fill_color(245, 245, 245)
            else:
                self.set_fill_color(255, 255, 255)

            for col_idx, val in enumerate(row):
                # Color positive/negative for highlight column
                if highlight_col is not None and col_idx == highlight_col:
                    try:
                        num = float(str(val).replace("%", "").replace(",", ""))
                        if num > 0:
                            self.set_text_color(0, 140, 100)
                        elif num < 0:
                            self.set_text_color(200, 50, 50)
                        else:
                            self.set_text_color(100, 100, 100)
                    except:
                        self.set_text_color(30, 30, 30)
                else:
                    self.set_text_color(30, 30, 30)

                self.cell(col_widths[col_idx], 6, str(val), 1, 0, "C", True)
            self.ln()

        self.set_text_color(0, 0, 0)
        self.ln(3)


def build_pdf(trades_df, inst_df, ac_df, year_df, ntc_df, buy_sell_df, cancel_df, hold_df, charts):
    """Build the complete PDF report."""
    pdf = NennerPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # ===== TITLE PAGE =====
    pdf.add_page()
    pdf.ln(40)
    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 15, "NENNER SIGNAL", 0, 1, "C")
    pdf.cell(0, 15, "PERFORMANCE REPORT", 0, 1, "C")
    pdf.ln(10)
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 8, "Vartanian Capital Management", 0, 1, "C")
    pdf.ln(5)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 8, f"Analysis Period: {trades_df['entry_date'].min().strftime('%b %Y')} - {trades_df['exit_date'].max().strftime('%b %Y')}", 0, 1, "C")
    pdf.cell(0, 8, f"Generated: {datetime.now().strftime('%B %d, %Y')}", 0, 1, "C")
    pdf.ln(20)

    # Summary box
    total_trades = len(trades_df)
    total_wins = (trades_df["pnl_pct"] > 0).sum()
    overall_wr = total_wins / total_trades * 100
    overall_avg = trades_df["pnl_pct"].mean()
    overall_median = trades_df["pnl_pct"].median()
    unique_instruments = trades_df["ticker"].nunique()

    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 8, "EXECUTIVE SUMMARY", 0, 1, "C")
    pdf.ln(3)

    pdf.key_stat("Total Round-Trip Trades", f"{total_trades:,}")
    pdf.key_stat("Instruments Analyzed", f"{unique_instruments}")
    pdf.key_stat("Overall Win Rate", f"{overall_wr:.1f}%",
                 (0, 140, 100) if overall_wr > 50 else (200, 50, 50))
    pdf.key_stat("Average Return per Trade", f"{overall_avg:+.2f}%",
                 (0, 140, 100) if overall_avg > 0 else (200, 50, 50))
    pdf.key_stat("Median Return per Trade", f"{overall_median:+.2f}%",
                 (0, 140, 100) if overall_median > 0 else (200, 50, 50))

    # ===== METHODOLOGY =====
    pdf.add_page()
    pdf.section_title("METHODOLOGY")
    pdf.body_text(
        "This report evaluates Nenner Research's signal performance by constructing "
        "round-trip trades from the signal database. A trade begins when Nenner issues "
        "a directional signal (BUY or SELL) with an origin price, and ends when the "
        "direction reverses (the next opposing signal's origin price becomes the exit). "
        "Confirmation signals (repeated BUY or SELL on the same instrument) are collapsed "
        "so only true direction changes create entry/exit points.\n\n"
        "P&L is calculated as: for BUY entries, (exit - entry) / entry; for SELL entries, "
        "(entry - exit) / entry. Only signals with valid origin prices are included.\n\n"
        f"FILTERS APPLIED:\n"
        f"  - Tradeable universe only: ETFs, single stocks, and VIX "
        f"(excludes futures and direct FX pairs)\n"
        f"  - ETF/VIX signals: last 3 years ({MACRO_CUTOFF} onward)\n"
        f"  - Single Stock signals: since Nov 2025 ({SINGLE_STOCK_CUTOFF} onward, "
        f"when the subscription began)\n"
        f"  - Outlier filter: trades with >200% return excluded as likely misparses\n\n"
        f"After filtering, {total_trades:,} completed round-trip trades were extracted "
        f"from {trades_df['ticker'].nunique()} instruments.\n\n"
        "IMPORTANT: These returns represent signal-level performance only. Actual trading "
        "results would differ due to execution timing, slippage, position sizing, and "
        "the trader's discretion in following or filtering signals."
    )

    # ===== TOP INSTRUMENTS =====
    pdf.add_page()
    pdf.section_title("TOP PERFORMING INSTRUMENTS")
    if "top_instruments" in charts:
        pdf.image(charts["top_instruments"], x=5, w=200)
    pdf.ln(3)

    # Table: Top 20
    top20 = inst_df.head(20)
    headers = ["Ticker", "Instrument", "Trades", "Win%", "Avg P&L", "Total P&L", "PF", "Avg Hold"]
    widths = [15, 40, 15, 15, 22, 25, 15, 22]
    data = []
    for _, r in top20.iterrows():
        data.append([
            r["ticker"],
            r["instrument"][:22],
            str(int(r["trades"])),
            f'{r["win_rate"]:.1f}%',
            f'{r["avg_pnl_pct"]:+.2f}%',
            f'{r["total_pnl_pct"]:+.1f}%',
            f'{r["profit_factor"]:.2f}' if r["profit_factor"] < 99 else "99+",
            f'{r["avg_holding_days"]:.0f}d',
        ])

    pdf.add_page()
    pdf.subsection_title("Top 20 Instruments -Detail Table")
    pdf.add_table(headers, data, widths, highlight_col=5)

    # ===== WORST INSTRUMENTS =====
    pdf.section_title("WORST PERFORMING INSTRUMENTS")
    if "bottom_instruments" in charts:
        pdf.image(charts["bottom_instruments"], x=5, w=200)

    bottom15 = inst_df.tail(15).iloc[::-1]
    data = []
    for _, r in bottom15.iterrows():
        data.append([
            r["ticker"],
            r["instrument"][:22],
            str(int(r["trades"])),
            f'{r["win_rate"]:.1f}%',
            f'{r["avg_pnl_pct"]:+.2f}%',
            f'{r["total_pnl_pct"]:+.1f}%',
            f'{r["profit_factor"]:.2f}' if r["profit_factor"] < 99 else "99+",
            f'{r["avg_holding_days"]:.0f}d',
        ])

    pdf.add_page()
    pdf.subsection_title("Bottom 15 Instruments -Detail Table")
    pdf.add_table(headers, data, widths, highlight_col=5)

    # ===== COMPLETE INSTRUMENT TABLE =====
    pdf.add_page()
    pdf.section_title("COMPLETE INSTRUMENT SCOREBOARD")

    all_headers = ["Ticker", "Class", "Trades", "W", "L", "Win%", "Avg", "Total", "PF"]
    all_widths = [14, 35, 14, 12, 12, 16, 20, 22, 16]
    all_data = []
    for _, r in inst_df.iterrows():
        all_data.append([
            r["ticker"],
            str(r["asset_class"])[:20],
            str(int(r["trades"])),
            str(int(r["wins"])),
            str(int(r["losses"])),
            f'{r["win_rate"]:.1f}%',
            f'{r["avg_pnl_pct"]:+.2f}%',
            f'{r["total_pnl_pct"]:+.1f}%',
            f'{r["profit_factor"]:.2f}' if r["profit_factor"] < 99 else "99+",
        ])

    # Split into pages if needed
    rows_per_page = 35
    for i in range(0, len(all_data), rows_per_page):
        if i > 0:
            pdf.add_page()
        chunk = all_data[i:i + rows_per_page]
        pdf.add_table(all_headers, chunk, all_widths, highlight_col=7)

    # ===== WIN RATE SCATTER =====
    pdf.add_page()
    pdf.section_title("WIN RATE ANALYSIS")
    if "win_rate_scatter" in charts:
        pdf.image(charts["win_rate_scatter"], x=5, w=200)
    pdf.ln(3)
    pdf.body_text(
        "Bubble size reflects the absolute cumulative P&L. Green = profitable, "
        "red = unprofitable. Instruments in the upper-right quadrant (high win rate, "
        "many trades) are the most reliable. The dashed line marks 50% win rate."
    )

    # ===== PROFIT FACTOR =====
    if "profit_factor" in charts:
        pdf.add_page()
        pdf.section_title("PROFIT FACTOR (PF) - EXPLAINED")
        pdf.body_text(
            "Profit Factor is one of the most important metrics for evaluating a signal "
            "provider. It answers a simple question: for every dollar you lose following "
            "these signals, how many dollars do you make back?\n\n"
            "  PF = Sum of All Winning Trade Returns / |Sum of All Losing Trade Returns|\n\n"
            "INTERPRETATION:\n"
            "  PF < 1.0 (RED) - Net loser. The losses outweigh the gains. Avoid.\n"
            "  PF = 1.0 - Breakeven. You make exactly as much as you lose.\n"
            "  PF 1.0-1.5 (AMBER) - Marginal edge. Profitable, but thin. Slippage and "
            "commissions could erase the advantage.\n"
            "  PF 1.5-2.0 (LIGHT GREEN) - Strong edge. The gains meaningfully exceed the "
            "losses. Worth trading with conviction.\n"
            "  PF > 2.0 (GREEN) - Excellent. For every $1 lost, you make $2+. These are "
            "Nenner's strongest instruments.\n\n"
            "WHY PF MATTERS MORE THAN WIN RATE:\n"
            "A 90% win rate means nothing if your average loss is 10x your average win. "
            "PF captures both frequency AND magnitude. An instrument with a 50% win rate "
            "but a PF of 3.0 is far more profitable than one with an 80% win rate and a "
            "PF of 0.8. PF tells you whether the math works in your favor.\n\n"
            "The chart below shows EVERY tradeable instrument ranked from highest to lowest PF."
        )
        pdf.add_page()
        pdf.subsection_title("Profit Factor - All Instruments (Descending)")
        pdf.image(charts["profit_factor"], x=3, w=204)

        # Add PF detail table - all instruments sorted by PF descending
        pdf.add_page()
        pdf.subsection_title("Profit Factor - Complete Detail Table")
        pdf.body_text(
            "Comparing Average vs Median reveals skew. If the average win is much larger "
            "than the median win, a few outsized winners are inflating the results. If the "
            "average loss is much worse than the median loss, there are occasional blowup "
            "losses dragging performance down. When avg and median are close, the results "
            "are consistent and reliable."
        )
        pf_sorted = inst_df.sort_values("profit_factor", ascending=False)
        pf_headers = ["Ticker", "Trades", "Win%", "Avg Win", "Med Win", "Avg Loss", "Med Loss", "PF", "Total"]
        pf_widths = [14, 14, 16, 21, 21, 21, 21, 14, 22]
        pf_data = []
        for _, r in pf_sorted.iterrows():
            pf_data.append([
                r["ticker"],
                str(int(r["trades"])),
                f'{r["win_rate"]:.1f}%',
                f'{r["avg_win_pct"]:+.2f}%',
                f'{r["median_win_pct"]:+.2f}%',
                f'{r["avg_loss_pct"]:+.2f}%',
                f'{r["median_loss_pct"]:+.2f}%',
                f'{r["profit_factor"]:.2f}' if r["profit_factor"] < 99 else "99+",
                f'{r["total_pnl_pct"]:+.1f}%',
            ])
        pdf.add_table(pf_headers, pf_data, pf_widths, highlight_col=7)

    # ===== ASSET CLASS =====
    pdf.add_page()
    pdf.section_title("PERFORMANCE BY ASSET CLASS")
    if "asset_class" in charts:
        pdf.image(charts["asset_class"], x=5, w=200)
    pdf.ln(3)

    ac_headers = ["Asset Class", "Tickers", "Trades", "Win%", "Avg P&L", "Total P&L", "PF"]
    ac_widths = [45, 16, 16, 18, 25, 28, 18]
    ac_data = []
    for _, r in ac_df.iterrows():
        ac_data.append([
            str(r["asset_class"])[:25],
            str(int(r["tickers"])),
            str(int(r["trades"])),
            f'{r["win_rate"]:.1f}%',
            f'{r["avg_pnl_pct"]:+.2f}%',
            f'{r["total_pnl_pct"]:+.1f}%',
            f'{r["profit_factor"]:.2f}' if r["profit_factor"] < 99 else "99+",
        ])
    pdf.add_table(ac_headers, ac_data, ac_widths, highlight_col=5)

    # ===== YEARLY PERFORMANCE =====
    pdf.add_page()
    pdf.section_title("PERFORMANCE BY YEAR")
    if "yearly_performance" in charts:
        pdf.image(charts["yearly_performance"], x=5, w=200)
    pdf.ln(3)

    yr_headers = ["Year", "Trades", "Wins", "Win%", "Avg P&L", "Total P&L"]
    yr_widths = [25, 25, 25, 25, 35, 35]
    yr_data = []
    for _, r in year_df.iterrows():
        yr_data.append([
            str(int(r["year"])),
            str(int(r["trades"])),
            str(int(r["wins"])),
            f'{r["win_rate"]:.1f}%',
            f'{r["avg_pnl_pct"]:+.2f}%',
            f'{r["total_pnl_pct"]:+.1f}%',
        ])
    pdf.add_table(yr_headers, yr_data, yr_widths, highlight_col=5)

    # ===== SIGNAL QUALITY =====
    pdf.add_page()
    pdf.section_title("SIGNAL QUALITY ANALYSIS")
    if "signal_quality" in charts:
        pdf.image(charts["signal_quality"], x=5, w=200)
    pdf.ln(3)

    pdf.subsection_title("Note-the-Change Signals")
    if len(ntc_df) > 0:
        pdf.body_text(
            f"NTC signals: {ntc_df.iloc[0]['trades']} trades, "
            f"{ntc_df.iloc[0]['win_rate']:.1f}% win rate, "
            f"{ntc_df.iloc[0]['avg_pnl_pct']:+.2f}% avg return.\n"
            f"Regular signals: {ntc_df.iloc[1]['trades']} trades, "
            f"{ntc_df.iloc[1]['win_rate']:.1f}% win rate, "
            f"{ntc_df.iloc[1]['avg_pnl_pct']:+.2f}% avg return."
        )

    pdf.subsection_title("BUY vs SELL Performance")
    if len(buy_sell_df) > 0:
        for _, r in buy_sell_df.iterrows():
            pdf.body_text(
                f"{r['direction']}: {int(r['trades'])} trades, "
                f"{r['win_rate']:.1f}% win rate, "
                f"{r['avg_pnl_pct']:+.2f}% avg return."
            )

    pdf.subsection_title("Cancellation Impact")
    if len(cancel_df) > 0:
        pdf.body_text(
            "Trades where Nenner issued cancellation signals during the holding period "
            "vs clean trades with no cancellations:"
        )
        for _, r in cancel_df.iterrows():
            pdf.body_text(
                f"{r['type']}: {int(r['trades'])} trades, "
                f"{r['win_rate']:.1f}% win rate, "
                f"{r['avg_pnl_pct']:+.2f}% avg return."
            )

    # ===== HOLDING PERIOD =====
    if len(hold_df) > 0:
        pdf.subsection_title("Performance by Holding Period")
        hp_headers = ["Period", "Trades", "Win%", "Avg P&L"]
        hp_widths = [40, 40, 40, 40]
        hp_data = []
        for _, r in hold_df.iterrows():
            hp_data.append([
                r["holding_period"],
                str(int(r["trades"])),
                f'{r["win_rate"]:.1f}%',
                f'{r["avg_pnl_pct"]:+.2f}%',
            ])
        pdf.add_table(hp_headers, hp_data, hp_widths, highlight_col=3)

    # ===== TRADING INSIGHTS =====
    pdf.add_page()
    pdf.section_title("ACTIONABLE TRADING INSIGHTS")

    # Best instruments
    top5 = inst_df.head(5)
    pdf.subsection_title("Strongest Instruments (Follow Confidently)")
    for _, r in top5.iterrows():
        pdf.body_text(
            f"  {r['ticker']} ({r['instrument']}): {r['win_rate']:.0f}% win rate, "
            f"{r['avg_pnl_pct']:+.2f}% avg, {int(r['trades'])} trades, PF {r['profit_factor']:.1f}"
        )

    # Worst instruments
    worst5 = inst_df.tail(5).iloc[::-1]
    pdf.subsection_title("Weakest Instruments (Consider Ignoring)")
    for _, r in worst5.iterrows():
        pdf.body_text(
            f"  {r['ticker']} ({r['instrument']}): {r['win_rate']:.0f}% win rate, "
            f"{r['avg_pnl_pct']:+.2f}% avg, {int(r['trades'])} trades, PF {r['profit_factor']:.1f}"
        )

    # High win-rate instruments (min 10 trades)
    high_wr = inst_df[(inst_df["trades"] >= 10) & (inst_df["win_rate"] >= 55)].sort_values("win_rate", ascending=False)
    if len(high_wr) > 0:
        pdf.subsection_title(f"High Win-Rate Instruments (>55%, 10+ trades): {len(high_wr)} found")
        for _, r in high_wr.head(10).iterrows():
            pdf.body_text(
                f"  {r['ticker']}: {r['win_rate']:.1f}% ({int(r['trades'])} trades, "
                f"PF {r['profit_factor']:.1f})"
            )

    # Best asset classes
    best_ac = ac_df[ac_df["win_rate"] > 50].head(5)
    if len(best_ac) > 0:
        pdf.subsection_title("Strongest Asset Classes")
        for _, r in best_ac.iterrows():
            pdf.body_text(
                f"  {r['asset_class']}: {r['win_rate']:.1f}% win rate, "
                f"PF {r['profit_factor']:.1f}, {int(r['trades'])} trades"
            )

    # Key findings
    pdf.add_page()
    pdf.section_title("KEY FINDINGS & RECOMMENDATIONS")

    findings = []

    # Overall assessment
    if overall_wr > 50:
        findings.append(
            f"OVERALL EDGE: Nenner's signals show a positive edge with a {overall_wr:.1f}% "
            f"overall win rate and {overall_avg:+.2f}% average return per trade across "
            f"{total_trades:,} round-trips."
        )
    else:
        findings.append(
            f"OVERALL: Nenner's signals show a {overall_wr:.1f}% overall win rate with "
            f"{overall_avg:+.2f}% average return. Selective filtering is recommended."
        )

    # NTC finding
    if len(ntc_df) == 2:
        ntc_wr = ntc_df.iloc[0]["win_rate"]
        reg_wr = ntc_df.iloc[1]["win_rate"]
        if ntc_wr > reg_wr + 2:
            findings.append(
                f"NTC ADVANTAGE: Note-the-Change signals outperform regular signals "
                f"({ntc_wr:.1f}% vs {reg_wr:.1f}% win rate). Consider giving NTC signals "
                f"higher conviction."
            )
        elif reg_wr > ntc_wr + 2:
            findings.append(
                f"NTC CAUTION: Regular signals actually outperform NTC signals "
                f"({reg_wr:.1f}% vs {ntc_wr:.1f}% win rate). NTC designation does not "
                f"indicate higher quality."
            )

    # BUY vs SELL
    if len(buy_sell_df) == 2:
        buy_wr = buy_sell_df.iloc[0]["win_rate"]
        sell_wr = buy_sell_df.iloc[1]["win_rate"]
        if abs(buy_wr - sell_wr) > 3:
            better = "BUY" if buy_wr > sell_wr else "SELL"
            findings.append(
                f"DIRECTION BIAS: {better} signals are more reliable "
                f"(BUY: {buy_wr:.1f}%, SELL: {sell_wr:.1f}%). Consider applying "
                f"tighter risk management on the weaker side."
            )

    # Cancellation finding
    if len(cancel_df) == 2:
        cancel_wr = cancel_df.iloc[0]["win_rate"]
        clean_wr = cancel_df.iloc[1]["win_rate"]
        if clean_wr > cancel_wr + 3:
            findings.append(
                f"CANCELLATION WARNING: Trades with mid-hold cancellations underperform "
                f"({cancel_wr:.1f}% vs {clean_wr:.1f}%). When Nenner cancels during a "
                f"trade, consider reducing position or exiting."
            )

    # Concentration
    top3_pnl = inst_df.head(3)["total_pnl_pct"].sum()
    total_pnl = inst_df["total_pnl_pct"].sum()
    if total_pnl > 0 and top3_pnl / total_pnl > 0.4:
        top3_names = ", ".join(inst_df.head(3)["ticker"].tolist())
        findings.append(
            f"CONCENTRATION: The top 3 instruments ({top3_names}) account for "
            f"{top3_pnl/total_pnl*100:.0f}% of total profits. Performance is not "
            f"evenly distributed."
        )

    for i, finding in enumerate(findings, 1):
        pdf.body_text(f"{i}. {finding}")

    # ===== DISCLAIMER =====
    pdf.ln(10)
    pdf.set_font("Helvetica", "I", 8)
    pdf.set_text_color(130, 130, 130)
    pdf.multi_cell(0, 4,
        "DISCLAIMER: This analysis is based on signal-to-signal origin prices as reported "
        "by Nenner Research. Actual trading results will vary due to execution timing, "
        "slippage, commissions, position sizing, and trader discretion. Past performance "
        "does not guarantee future results. This report is for internal use only."
    )

    # Save
    pdf.output(PDF_PATH)
    return PDF_PATH


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("NENNER SIGNAL PERFORMANCE ANALYSIS")
    print("=" * 60)

    # Load data
    print("\n[1/6] Loading signals from database...")
    signals_df = load_signals()
    print(f"  Loaded {len(signals_df):,} signals, {signals_df['ticker'].nunique()} tickers")
    print(f"  Date range: {signals_df['date'].min().strftime('%Y-%m-%d')} to {signals_df['date'].max().strftime('%Y-%m-%d')}")

    # Extract trades
    print("\n[2/6] Extracting round-trip trades...")
    trades_df = extract_trades(signals_df)
    print(f"  Extracted {len(trades_df):,} completed round-trip trades")
    print(f"  Instruments with trades: {trades_df['ticker'].nunique()}")
    wins = (trades_df["pnl_pct"] > 0).sum()
    print(f"  Overall win rate: {wins/len(trades_df)*100:.1f}%")
    print(f"  Average return: {trades_df['pnl_pct'].mean():+.2f}%")

    # Run analyses
    print("\n[3/6] Running analyses...")
    inst_df = analyze_by_instrument(trades_df)
    ac_df = analyze_by_asset_class(trades_df)
    year_df = analyze_by_year(trades_df)
    ntc_df = analyze_ntc_impact(trades_df)
    buy_sell_df = analyze_buy_vs_sell(trades_df)
    cancel_df = analyze_cancellation_impact(trades_df)
    hold_df = analyze_holding_period(trades_df)

    print(f"  Instruments analyzed: {len(inst_df)}")
    print(f"  Asset classes: {len(ac_df)}")
    print(f"  Years covered: {len(year_df)}")

    # Generate charts
    print("\n[4/6] Generating charts...")
    charts = {}
    charts["top_instruments"] = chart_top_instruments(inst_df)
    charts["bottom_instruments"] = chart_bottom_instruments(inst_df)
    charts["win_rate_scatter"] = chart_win_rate(inst_df)
    charts["asset_class"] = chart_asset_class(ac_df)
    charts["yearly_performance"] = chart_yearly_performance(year_df)
    charts["signal_quality"] = chart_signal_quality(ntc_df, buy_sell_df, cancel_df)
    charts["profit_factor"] = chart_profit_factor(inst_df)
    print(f"  Generated {len(charts)} charts")

    # Build PDF
    print("\n[5/6] Building PDF report...")
    pdf_path = build_pdf(trades_df, inst_df, ac_df, year_df, ntc_df, buy_sell_df, cancel_df, hold_df, charts)
    print(f"  PDF saved to: {pdf_path}")

    # Summary
    print("\n[6/6] Summary")
    print("=" * 60)
    print(f"  Total trades:      {len(trades_df):,}")
    print(f"  Win rate:          {wins/len(trades_df)*100:.1f}%")
    print(f"  Avg return:        {trades_df['pnl_pct'].mean():+.2f}%")
    print(f"  Best instrument:   {inst_df.iloc[0]['ticker']} ({inst_df.iloc[0]['total_pnl_pct']:+.1f}%)")
    print(f"  Worst instrument:  {inst_df.iloc[-1]['ticker']} ({inst_df.iloc[-1]['total_pnl_pct']:+.1f}%)")
    print(f"\n  Report: {pdf_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
