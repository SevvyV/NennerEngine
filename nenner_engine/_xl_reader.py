"""
Subprocess helper — reads Options_RT from Nenner_DataCenter.xlsm via COM.
==========================================================================
Called by fischer_chain.py via subprocess.run() with a hard timeout.
This isolates the COM interaction so it can be killed if Excel hangs.

Usage:
    python _xl_reader.py <ticker> [workbook_name] [sheet_name]

Outputs JSON to stdout:
    {"ok": true, "ticker": "TSLA", "spot": 405.0, "rate": 0.045, ...}
    or
    {"ok": false, "error": "..."}
"""

import json
import sys
import time


def _pick_increment(ticker: str, spot: float) -> float:
    """Choose strike increment based on spot price."""
    if spot < 50:
        return 0.5
    if spot < 200:
        return 1
    if spot < 500:
        return 2.5
    return 5


def _output(data: dict, out_path: str | None) -> None:
    """Write JSON result to file or stdout."""
    text = json.dumps(data)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        print(text)


def _find_workbook(workbook_name: str):
    """Find a workbook by name, skipping Excel instances with broken COM handles.

    Phantom Excel processes (no open workbooks, stale RTD add-in remnants)
    can cause AccessibleObjectFromWindow to throw OSError -2147467259.
    This iterates through all instances defensively so one bad instance
    doesn't take down the entire chain read.
    """
    import xlwings as xw

    for app in xw.apps:
        try:
            for book in app.books:
                if book.name.lower() == workbook_name.lower():
                    return book
        except OSError:
            # Phantom Excel instance — broken COM handle, skip it
            continue

    return None


def main():
    ticker = sys.argv[1].upper() if len(sys.argv) > 1 else ""
    workbook_name = sys.argv[2] if len(sys.argv) > 2 else "Nenner_DataCenter.xlsm"
    sheet_name = sys.argv[3] if len(sys.argv) > 3 else "Options_RT"
    out_path = sys.argv[4] if len(sys.argv) > 4 else None  # temp file path

    if not ticker:
        _output({"ok": False, "error": "No ticker provided"}, out_path)
        return

    try:
        import pythoncom
        pythoncom.CoInitialize()

        wb = _find_workbook(workbook_name)
        if wb is None:
            _output({"ok": False, "error": f"Workbook '{workbook_name}' not found in any Excel instance"}, out_path)
            return
        ws = wb.sheets[sheet_name]

        # Check if we need to switch tickers
        sheet_ticker = ws.range("B1").value
        needs_switch = (
            not sheet_ticker
            or str(sheet_ticker).upper() != ticker
        )

        if needs_switch:
            ws.range("B1").value = ticker
            time.sleep(2)
            _safe_calc(wb)
            time.sleep(1)

            prelim_spot = ws.range("B3").value
            if prelim_spot and float(prelim_spot) > 0:
                new_inc = _pick_increment(ticker, float(prelim_spot))
            else:
                new_inc = 2.5
            ws.range("B2").value = new_inc

            _safe_calc(wb)
            time.sleep(3)
            _safe_calc(wb)
            time.sleep(1)

        # Read header
        spot = ws.range("B3").value
        rate = ws.range("B4").value
        div_yield = ws.range("B5").value or 0.0

        if not spot or float(spot) <= 0:
            _output({"ok": False, "error": "Spot price is missing or zero"}, out_path)
            return

        spot = float(spot)
        rate = float(rate) if rate else 0.045
        div_yield = float(div_yield)

        # Read expiry dates
        from datetime import datetime, date
        expiries = []
        for cell in ["B8", "B9", "B10", "B11"]:
            val = ws.range(cell).value
            if val:
                if isinstance(val, datetime):
                    expiries.append(val.strftime("%Y-%m-%d"))
                elif isinstance(val, date):
                    expiries.append(val.isoformat())

        if not expiries:
            _output({"ok": False, "error": "No expiry dates found"}, out_path)
            return

        # Read strike data — bulk read for speed
        rows = []
        blocks_per_section = 4
        rows_per_block = 42

        for opt_type, section_offset in [("P", 0), ("C", 1)]:
            if opt_type == "P":
                section_start = 12
            else:
                section_start = 12 + blocks_per_section * rows_per_block + 2

            for block_idx in range(min(blocks_per_section, len(expiries))):
                block_start = section_start + block_idx * rows_per_block
                data_start = block_start + 1

                # Bulk read: A(strike), C(bid), D(ask), E(last), F(oi), G(vol)
                # Read 40 rows at once for each column
                strikes = ws.range(f"A{data_start}:A{data_start + 39}").value
                bids = ws.range(f"C{data_start}:C{data_start + 39}").value
                asks = ws.range(f"D{data_start}:D{data_start + 39}").value
                lasts = ws.range(f"E{data_start}:E{data_start + 39}").value
                ois = ws.range(f"F{data_start}:F{data_start + 39}").value
                vols = ws.range(f"G{data_start}:G{data_start + 39}").value

                for i in range(40):
                    strike = strikes[i] if isinstance(strikes, list) else strikes
                    if strike is None or strike == "" or strike == 0:
                        continue
                    try:
                        strike = float(strike)
                    except (ValueError, TypeError):
                        continue

                    bid = _safe_float(bids[i] if isinstance(bids, list) else bids)
                    ask = _safe_float(asks[i] if isinstance(asks, list) else asks)
                    last = _safe_float(lasts[i] if isinstance(lasts, list) else lasts)
                    oi = _safe_int(ois[i] if isinstance(ois, list) else ois)
                    vol = _safe_int(vols[i] if isinstance(vols, list) else vols)

                    if bid == 0 and ask == 0 and last == 0:
                        continue

                    rows.append({
                        "expiry": expiries[block_idx],
                        "strike": strike,
                        "type": opt_type,
                        "bid": bid,
                        "ask": ask,
                        "last": last,
                        "oi": oi,
                        "volume": vol,
                    })

        _output({
            "ok": True,
            "ticker": ticker,
            "spot": spot,
            "rate": rate,
            "div_yield": div_yield,
            "expiries": expiries,
            "switched": needs_switch,
            "rows": rows,
        }, out_path)

    except Exception as e:
        _output({"ok": False, "error": str(e)}, out_path)


def _safe_calc(wb, retries=3, delay=1.0):
    """Calculate with retry for OLE busy errors."""
    for attempt in range(retries):
        try:
            wb.app.api.Calculate()
            return
        except Exception as e:
            if "800ac472" in str(e) or "rejected" in str(e).lower():
                time.sleep(delay)
            else:
                raise


def _safe_float(val) -> float:
    if val is None or val == "":
        return 0.0
    if isinstance(val, str) and val.startswith("#"):
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _safe_int(val) -> int:
    if val is None or val == "":
        return 0
    if isinstance(val, str) and val.startswith("#"):
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


if __name__ == "__main__":
    main()
