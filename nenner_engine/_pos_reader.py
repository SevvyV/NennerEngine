"""
Subprocess helper — reads open positions from Nenner_Positions.xlsm via COM.
==========================================================================
Called by fischer_chain.py via subprocess.run() with a hard timeout.

Usage:
    python _pos_reader.py <ticker> [workbook_name]

Scans all 5 trading sheets (Puts 1-3, Calls 1-2) for an open position
matching the given ticker. Returns entry price, direction, shares, etc.

Outputs JSON to stdout or temp file:
    {"ok": true, "found": true, "ticker": "TSLA", "entry_price": 405.0, ...}
    or
    {"ok": true, "found": false}
    or
    {"ok": false, "error": "..."}
"""

import json
import sys


TRADING_SHEETS = ["Puts 1", "Puts 2", "Puts 3", "Calls 1", "Calls 2"]


def _output(data: dict, out_path: str | None) -> None:
    """Write JSON result to file or stdout."""
    text = json.dumps(data)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        print(text)


def main():
    ticker = sys.argv[1].upper() if len(sys.argv) > 1 else ""
    workbook_name = sys.argv[2] if len(sys.argv) > 2 else "Nenner_Positions.xlsm"
    out_path = sys.argv[3] if len(sys.argv) > 3 else None

    if not ticker:
        _output({"ok": False, "error": "No ticker provided"}, out_path)
        return

    try:
        import pythoncom
        pythoncom.CoInitialize()

        import xlwings as xw

        wb = xw.Book(workbook_name)

        # Scan all trading sheets for an open position matching this ticker.
        # A position is only "found" if the trade log has an unclosed entry.
        # Trade log columns: A=trade#, B=open_date, C=close_date,
        #   D=sheet, E=ticker, F=side, G=qty, H=entry_price, I=exit_price
        for sheet_name in TRADING_SHEETS:
            try:
                ws = wb.sheets[sheet_name]
            except Exception:
                continue

            sheet_ticker = ws.range("B4").value
            if not sheet_ticker or str(sheet_ticker).upper() != ticker:
                continue

            # Scan local trade log for an open entry (no close date)
            open_trade = None
            for row in range(40, 201):
                trade_num = ws.range(f"A{row}").value
                if trade_num is None or str(trade_num).strip() == "":
                    break  # No more entries
                date_close = ws.range(f"C{row}").value
                if date_close is None or str(date_close).strip() == "":
                    # Read position data from the trade log entry
                    side = ws.range(f"F{row}").value
                    qty = ws.range(f"G{row}").value
                    entry = ws.range(f"H{row}").value
                    open_trade = {
                        "side": str(side).strip().upper() if side else None,
                        "shares": int(float(qty)) if qty else 0,
                        "entry_price": float(entry) if entry else 0.0,
                    }
                    break

            if open_trade and open_trade["entry_price"] > 0:
                is_put = sheet_name.startswith("Puts")
                default_dir = "SHORT" if is_put else "LONG"
                _output({
                    "ok": True,
                    "found": True,
                    "ticker": ticker,
                    "sheet": sheet_name,
                    "entry_price": open_trade["entry_price"],
                    "direction": open_trade["side"] or default_dir,
                    "shares": open_trade["shares"],
                    "intent": "covered_put" if is_put else "covered_call",
                }, out_path)
                return

        # No matching position found
        _output({"ok": True, "found": False, "ticker": ticker}, out_path)

    except Exception as e:
        _output({"ok": False, "error": str(e)}, out_path)


if __name__ == "__main__":
    main()
