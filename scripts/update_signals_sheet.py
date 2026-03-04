"""update_signals_sheet.py — Refresh NennerSignals sheet in Nenner_Positions.xlsm

Populates a lookup sheet with current Nenner signal data so the Puts/Calls
trade sheets can VLOOKUP signal direction, cancel levels, score, etc.

Usage:
    python update_signals_sheet.py

Requires Nenner_Positions.xlsm to be open in Excel (xlwings attaches to the
running instance).
"""

import sqlite3
import sys

import xlwings as xw

from nenner_engine.trade_stats import compute_instrument_stats

DB = "E:/Workspace/NennerEngine/nenner_signals.db"
WB = "E:/Workspace/DataCenter/Nenner_Positions.xlsm"
SHEET = "NennerSignals"

HEADERS = ["Ticker", "Signal", "Origin Price", "Cancel Level", "Signal Date", "Score"]


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # Active signals from current_state
    rows = conn.execute(
        """SELECT ticker, effective_signal, origin_price, cancel_level, last_signal_date
           FROM current_state
           WHERE effective_signal IN ('BUY', 'SELL')
           ORDER BY ticker"""
    ).fetchall()

    if not rows:
        print("No active signals found.")
        conn.close()
        return

    # Composite scores from trade_stats
    stats = compute_instrument_stats(conn, use_cache=False)
    conn.close()

    # Build data rows: [ticker, signal, origin_price, cancel_level, signal_date, score]
    data = []
    for r in rows:
        ticker = r["ticker"]
        score_raw = stats.get(ticker, {}).get("composite", 0.0)
        score = round(score_raw * 100, 1)
        data.append([
            ticker,
            r["effective_signal"],
            r["origin_price"],
            r["cancel_level"],
            r["last_signal_date"],
            score,
        ])

    # Open workbook (must already be open in Excel)
    try:
        wb = xw.Book(WB)
    except Exception as e:
        print(f"Could not attach to workbook: {e}")
        print("Make sure Nenner_Positions.xlsm is open in Excel.")
        sys.exit(1)

    # Create or clear the sheet
    if SHEET in [s.name for s in wb.sheets]:
        ws = wb.sheets[SHEET]
        ws.clear()
    else:
        ws = wb.sheets.add(SHEET, after=wb.sheets[-1])

    # Write headers + data
    ws.range("A1").value = HEADERS
    ws.range("A2").value = data

    # Bold headers
    ws.range("A1").expand("right").font.bold = True

    # Auto-fit columns
    ws.autofit("c")

    print(f"NennerSignals: wrote {len(data)} rows.")


if __name__ == "__main__":
    main()
