"""
Comprehensive Nenner Track Record Analysis
============================================
Builds round-trip trades from the full signal history and computes
detailed performance metrics.
"""

import os
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nenner_signals.db")

# Price sanity: skip trades where entry/exit ratio > 5x (likely misparse)
PRICE_SANITY_MAX_RATIO = 5.0
# Skip trades with |PnL| > 200%
MAX_SINGLE_TRADE_PCT = 200.0


def extract_trades(conn, min_date=None):
    """Extract round-trip trades from consecutive direction changes.

    A trade = entry at signal N's origin_price -> exit at signal N+1's origin_price
    where N and N+1 are consecutive signals with DIFFERENT directions (BUY->SELL or SELL->BUY).
    """
    where = "WHERE signal_type IN ('BUY','SELL') AND origin_price IS NOT NULL AND origin_price > 0"
    if min_date:
        where += f" AND date >= '{min_date}'"

    rows = conn.execute(f"""
        SELECT ticker, instrument, asset_class, date, signal_type, origin_price
        FROM signals
        {where}
        ORDER BY ticker, date, id
    """).fetchall()

    # Group by ticker
    by_ticker = defaultdict(list)
    for r in rows:
        by_ticker[r[0]].append(r)

    trades = []
    for ticker, signals in by_ticker.items():
        # Deduplicate: keep only direction changes
        filtered = [signals[0]]
        for s in signals[1:]:
            if s[4] != filtered[-1][4]:  # different signal_type
                filtered.append(s)

        # Build round-trips
        for i in range(len(filtered) - 1):
            entry = filtered[i]
            exit_ = filtered[i + 1]

            entry_price = entry[5]
            exit_price = exit_[5]
            entry_signal = entry[4]  # BUY or SELL

            # Sanity check
            ratio = max(entry_price, exit_price) / min(entry_price, exit_price)
            if ratio > PRICE_SANITY_MAX_RATIO:
                continue

            # Calculate PnL
            if entry_signal == "BUY":
                pnl_pct = ((exit_price - entry_price) / entry_price) * 100
            else:
                pnl_pct = ((entry_price - exit_price) / entry_price) * 100

            # Skip extreme outliers
            if abs(pnl_pct) > MAX_SINGLE_TRADE_PCT:
                continue

            entry_date = entry[3]
            exit_date = exit_[3]
            try:
                d1 = datetime.strptime(entry_date, "%Y-%m-%d")
                d2 = datetime.strptime(exit_date, "%Y-%m-%d")
                duration = (d2 - d1).days
            except (ValueError, TypeError):
                duration = 0

            trades.append({
                "ticker": ticker,
                "instrument": entry[1] or ticker,
                "asset_class": entry[2] or "Unknown",
                "entry_signal": entry_signal,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl_pct": pnl_pct,
                "entry_date": entry_date,
                "exit_date": exit_date,
                "duration_days": duration,
                "year": entry_date[:4] if entry_date else "?",
            })

    return trades


def compute_stats(trades, label="ALL"):
    """Compute comprehensive stats for a list of trades."""
    if not trades:
        return None

    pnls = [t["pnl_pct"] for t in trades]
    winners = [p for p in pnls if p > 0]
    losers = [p for p in pnls if p <= 0]
    durations = [t["duration_days"] for t in trades if t["duration_days"] > 0]

    n = len(pnls)
    n_win = len(winners)
    n_loss = len(losers)
    win_rate = n_win / n * 100 if n else 0

    avg_return = statistics.mean(pnls) if pnls else 0
    med_return = statistics.median(pnls) if pnls else 0
    std_return = statistics.stdev(pnls) if len(pnls) > 1 else 0

    avg_win = statistics.mean(winners) if winners else 0
    med_win = statistics.median(winners) if winners else 0
    avg_loss = statistics.mean(losers) if losers else 0
    med_loss = statistics.median(losers) if losers else 0

    gross_gains = sum(winners)
    gross_losses = abs(sum(losers))
    profit_factor = gross_gains / gross_losses if gross_losses > 0 else 99.0

    # Sharpe (simple: mean / std)
    sharpe = avg_return / std_return if std_return > 0 else 0

    # Kelly criterion
    if n_win > 0 and n_loss > 0 and avg_win > 0 and avg_loss < 0:
        p = n_win / n
        w_l_ratio = avg_win / abs(avg_loss)
        kelly = p - (1 - p) / w_l_ratio
    else:
        kelly = 0

    # Max drawdown (worst single trade)
    max_dd = min(pnls) if pnls else 0

    # Best trade
    max_win = max(pnls) if pnls else 0

    # Duration stats
    avg_dur = statistics.mean(durations) if durations else 0
    med_dur = statistics.median(durations) if durations else 0

    # Cumulative return (geometric)
    cum = 1.0
    for p in pnls:
        cum *= (1 + p / 100)
    total_return = (cum - 1) * 100

    return {
        "label": label,
        "trades": n,
        "winners": n_win,
        "losers": n_loss,
        "win_rate": win_rate,
        "avg_return": avg_return,
        "median_return": med_return,
        "std_return": std_return,
        "avg_win": avg_win,
        "median_win": med_win,
        "avg_loss": avg_loss,
        "median_loss": med_loss,
        "profit_factor": min(profit_factor, 99.0),
        "sharpe": sharpe,
        "kelly": kelly,
        "max_drawdown": max_dd,
        "best_trade": max_win,
        "total_return_pct": total_return,
        "avg_duration": avg_dur,
        "median_duration": med_dur,
        "gross_gains": gross_gains,
        "gross_losses": gross_losses,
    }


def print_stats(s, indent=""):
    """Pretty-print a stats dict."""
    if s is None:
        print(f"{indent}  No trades")
        return

    print(f"{indent}  Trades: {s['trades']}  (W:{s['winners']} / L:{s['losers']})")
    print(f"{indent}  Win Rate:        {s['win_rate']:.1f}%")
    print(f"{indent}  Avg Return:      {s['avg_return']:+.2f}%")
    print(f"{indent}  Median Return:   {s['median_return']:+.2f}%")
    print(f"{indent}  Std Dev:         {s['std_return']:.2f}%")
    print(f"{indent}  Avg Win:         {s['avg_win']:+.2f}%    Median Win:  {s['median_win']:+.2f}%")
    print(f"{indent}  Avg Loss:        {s['avg_loss']:+.2f}%    Median Loss: {s['median_loss']:+.2f}%")
    print(f"{indent}  Profit Factor:   {s['profit_factor']:.2f}")
    print(f"{indent}  Sharpe:          {s['sharpe']:.3f}")
    print(f"{indent}  Kelly:           {s['kelly']:.3f}")
    print(f"{indent}  Best Trade:      {s['best_trade']:+.2f}%")
    print(f"{indent}  Worst Trade:     {s['max_drawdown']:+.2f}%")
    print(f"{indent}  Total Return:    {s['total_return_pct']:+.1f}% (compounded)")
    print(f"{indent}  Avg Duration:    {s['avg_duration']:.0f} days    Median: {s['median_duration']:.0f} days")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("Extracting trades from signal database...")
    all_trades = extract_trades(conn)
    print(f"Total round-trip trades: {len(all_trades)}")

    # =====================================================================
    # OVERALL TRACK RECORD
    # =====================================================================
    print("\n" + "=" * 80)
    print("OVERALL TRACK RECORD")
    print("=" * 80)
    overall = compute_stats(all_trades)
    print_stats(overall)

    # =====================================================================
    # BUY vs SELL
    # =====================================================================
    print("\n" + "=" * 80)
    print("BUY SIGNALS vs SELL SIGNALS")
    print("=" * 80)

    buys = [t for t in all_trades if t["entry_signal"] == "BUY"]
    sells = [t for t in all_trades if t["entry_signal"] == "SELL"]

    print(f"\n  --- BUY Signals ({len(buys)} trades) ---")
    print_stats(compute_stats(buys, "BUY"))
    print(f"\n  --- SELL Signals ({len(sells)} trades) ---")
    print_stats(compute_stats(sells, "SELL"))

    # =====================================================================
    # BY ASSET CLASS
    # =====================================================================
    print("\n" + "=" * 80)
    print("BY ASSET CLASS")
    print("=" * 80)

    # Group broad categories
    def broad_class(ac):
        ac = ac.lower()
        if "equity" in ac:
            return "Equity Indices"
        elif "currency" in ac:
            return "Currencies"
        elif "precious" in ac:
            return "Precious Metals"
        elif "energy" in ac:
            return "Energy"
        elif "agri" in ac:
            return "Agriculture"
        elif "fixed" in ac:
            return "Fixed Income"
        elif "crypto" in ac:
            return "Crypto"
        elif "volatility" in ac:
            return "Volatility"
        elif "single" in ac or "stock" in ac:
            return "Single Stocks"
        else:
            return "Other"

    by_class = defaultdict(list)
    for t in all_trades:
        by_class[broad_class(t["asset_class"])].append(t)

    class_stats = []
    for cls in sorted(by_class.keys()):
        s = compute_stats(by_class[cls], cls)
        if s and s["trades"] >= 3:
            class_stats.append(s)

    class_stats.sort(key=lambda x: x["avg_return"], reverse=True)
    for s in class_stats:
        print(f"\n  --- {s['label']} ({s['trades']} trades) ---")
        print_stats(s, "  ")

    # =====================================================================
    # BY YEAR
    # =====================================================================
    print("\n" + "=" * 80)
    print("BY YEAR (entry year)")
    print("=" * 80)

    by_year = defaultdict(list)
    for t in all_trades:
        by_year[t["year"]].append(t)

    print(f"\n  {'Year':<6} {'Trades':>7} {'W/L':>8} {'WR':>7} {'AvgRet':>8} {'MedRet':>8} {'PF':>7} {'BestTr':>8} {'WorstTr':>8}")
    print("  " + "-" * 75)
    for year in sorted(by_year.keys()):
        s = compute_stats(by_year[year], year)
        if s:
            print(f"  {year:<6} {s['trades']:>7} {s['winners']:>3}/{s['losers']:<4} {s['win_rate']:>6.1f}% "
                  f"{s['avg_return']:>+7.2f}% {s['median_return']:>+7.2f}% {s['profit_factor']:>6.1f} "
                  f"{s['best_trade']:>+7.1f}% {s['max_drawdown']:>+7.1f}%")

    # =====================================================================
    # TOP 15 / BOTTOM 5 INSTRUMENTS
    # =====================================================================
    print("\n" + "=" * 80)
    print("TOP 15 INSTRUMENTS (by avg return, min 10 trades)")
    print("=" * 80)

    by_ticker = defaultdict(list)
    for t in all_trades:
        by_ticker[t["ticker"]].append(t)

    ticker_stats = []
    for ticker, trades in by_ticker.items():
        s = compute_stats(trades, ticker)
        if s and s["trades"] >= 10:
            ticker_stats.append(s)

    ticker_stats.sort(key=lambda x: x["avg_return"], reverse=True)

    print(f"\n  {'Ticker':<12} {'Trades':>7} {'WR':>7} {'AvgRet':>8} {'MedRet':>8} {'AvgWin':>8} {'AvgLoss':>8} {'PF':>7} {'Sharpe':>7}")
    print("  " + "-" * 85)
    for s in ticker_stats[:15]:
        print(f"  {s['label']:<12} {s['trades']:>7} {s['win_rate']:>6.1f}% "
              f"{s['avg_return']:>+7.2f}% {s['median_return']:>+7.2f}% "
              f"{s['avg_win']:>+7.2f}% {s['avg_loss']:>+7.2f}% "
              f"{s['profit_factor']:>6.1f} {s['sharpe']:>6.3f}")

    print(f"\n  BOTTOM 5 INSTRUMENTS:")
    print("  " + "-" * 85)
    for s in ticker_stats[-5:]:
        print(f"  {s['label']:<12} {s['trades']:>7} {s['win_rate']:>6.1f}% "
              f"{s['avg_return']:>+7.2f}% {s['median_return']:>+7.2f}% "
              f"{s['avg_win']:>+7.2f}% {s['avg_loss']:>+7.2f}% "
              f"{s['profit_factor']:>6.1f} {s['sharpe']:>6.3f}")

    # =====================================================================
    # DURATION ANALYSIS
    # =====================================================================
    print("\n" + "=" * 80)
    print("TRADE DURATION ANALYSIS")
    print("=" * 80)

    durations = [t["duration_days"] for t in all_trades if t["duration_days"] > 0]
    win_durations = [t["duration_days"] for t in all_trades if t["pnl_pct"] > 0 and t["duration_days"] > 0]
    loss_durations = [t["duration_days"] for t in all_trades if t["pnl_pct"] <= 0 and t["duration_days"] > 0]

    print(f"\n  All trades:     avg {statistics.mean(durations):.0f} days,  median {statistics.median(durations):.0f} days")
    if win_durations:
        print(f"  Winners:        avg {statistics.mean(win_durations):.0f} days,  median {statistics.median(win_durations):.0f} days")
    if loss_durations:
        print(f"  Losers:         avg {statistics.mean(loss_durations):.0f} days,  median {statistics.median(loss_durations):.0f} days")

    # Bucket by duration
    buckets = [(0, 7, "< 1 week"), (7, 14, "1-2 weeks"), (14, 30, "2-4 weeks"),
               (30, 60, "1-2 months"), (60, 120, "2-4 months"), (120, 365, "4-12 months"),
               (365, 9999, "> 1 year")]

    print(f"\n  {'Duration':<15} {'Trades':>7} {'WR':>7} {'AvgRet':>8} {'MedRet':>8}")
    print("  " + "-" * 50)
    for lo, hi, label in buckets:
        bucket_trades = [t for t in all_trades if lo <= t["duration_days"] < hi]
        if bucket_trades:
            s = compute_stats(bucket_trades, label)
            print(f"  {label:<15} {s['trades']:>7} {s['win_rate']:>6.1f}% {s['avg_return']:>+7.2f}% {s['median_return']:>+7.2f}%")

    # =====================================================================
    # WIN/LOSS STREAK ANALYSIS
    # =====================================================================
    print("\n" + "=" * 80)
    print("WIN/LOSS STREAK ANALYSIS (all instruments combined, chronological)")
    print("=" * 80)

    sorted_trades = sorted(all_trades, key=lambda t: t["entry_date"])
    outcomes = [1 if t["pnl_pct"] > 0 else 0 for t in sorted_trades]

    # Find streaks
    max_win_streak = 0
    max_loss_streak = 0
    current_streak = 0
    current_type = None

    for o in outcomes:
        if o == current_type:
            current_streak += 1
        else:
            current_type = o
            current_streak = 1
        if current_type == 1:
            max_win_streak = max(max_win_streak, current_streak)
        else:
            max_loss_streak = max(max_loss_streak, current_streak)

    print(f"\n  Longest winning streak: {max_win_streak} trades")
    print(f"  Longest losing streak:  {max_loss_streak} trades")

    # Distribution of streak lengths
    streaks = {"win": [], "loss": []}
    current_streak = 1
    for i in range(1, len(outcomes)):
        if outcomes[i] == outcomes[i - 1]:
            current_streak += 1
        else:
            key = "win" if outcomes[i - 1] == 1 else "loss"
            streaks[key].append(current_streak)
            current_streak = 1
    key = "win" if outcomes[-1] == 1 else "loss"
    streaks[key].append(current_streak)

    for stype in ["win", "loss"]:
        s = streaks[stype]
        if s:
            print(f"  Avg {stype} streak length: {statistics.mean(s):.1f} trades  "
                  f"(median {statistics.median(s):.0f})")

    # =====================================================================
    # RETURN DISTRIBUTION
    # =====================================================================
    print("\n" + "=" * 80)
    print("RETURN DISTRIBUTION")
    print("=" * 80)

    pnls = sorted([t["pnl_pct"] for t in all_trades])
    percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    n = len(pnls)

    print(f"\n  Percentile distribution:")
    for p in percentiles:
        idx = min(int(n * p / 100), n - 1)
        print(f"    P{p:>2d}: {pnls[idx]:>+7.2f}%")

    # Buckets
    ranges = [(-200, -50), (-50, -20), (-20, -10), (-10, -5), (-5, 0),
              (0, 5), (5, 10), (10, 20), (20, 50), (50, 200)]
    print(f"\n  Return distribution:")
    for lo, hi in ranges:
        count = sum(1 for p in pnls if lo <= p < hi)
        bar = "#" * int(count / max(len(pnls), 1) * 100)
        print(f"    {lo:>+4d}% to {hi:>+4d}%:  {count:>5}  ({count / len(pnls) * 100:>5.1f}%)  {bar}")

    conn.close()


if __name__ == "__main__":
    main()
