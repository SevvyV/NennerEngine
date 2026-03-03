"""
Subprocess helper — reads ALL 17 put+call chains from OptionChains.xlsm in one pass.
=====================================================================================
Called by fischer_chain.read_all_chains() via subprocess.run() with a hard timeout.
This isolates the COM interaction so it can be killed if Excel hangs.

Unlike _xl_reader.py (which reads one ticker at a time from Options_RT and must
switch tickers), this reads the pre-populated PutChains and CallChains sheets
where all 17 tickers already have live RTD bid/ask data.  No ticker switching,
no sleep, no recalc.

OptionChains.xlsm layout (built by build_option_chains.py):
    Sheets: "PutChains" (puts) and "CallChains" (calls) — identical layout
    Row 2:  4 expiry dates at cols B(2), G(7), L(12), Q(17) — 1-based
    Row 3:  Rate at B3
    Row 5+: 17 ticker blocks, 14 rows apart (13 data + 1 spacer)
    Per block: header row (ticker/spot/increment), col-header row, 11 data rows
    4 column sets: A-D, F-I, K-N, P-S  (Strike | RIC | Bid | Ask)

Usage:
    python _oc_reader.py [workbook_name] [out_path]

Outputs JSON:
    {"ok": true, "rate": 0.045, "expiries": [...],
     "puts":  {"TSLA": {"spot": 405.0, "rows": [...]}, ...},
     "calls": {"TSLA": {"spot": 405.0, "rows": [...]}, ...},
     "failed_puts": [], "failed_calls": []}
    or
    {"ok": false, "error": "..."}
"""

import json
import sys


# ---------------------------------------------------------------------------
# OptionChains layout constants (must match build_option_chains.py)
# ---------------------------------------------------------------------------
BLOCK_START_ROW = 5       # first ticker block
ROWS_PER_BLOCK = 13       # 1 header + 1 col header + 11 data
BLOCK_SPACING = 14        # 13 + 1 spacer row between blocks
STRIKES_PER_TICKER = 11

TICKERS = [
    "AAPL", "AMZN", "AVGO", "GOOGL", "IWM", "META", "MSFT", "NVDA",
    "QQQ", "TSLA", "GLD", "IBIT", "SLV", "SPY", "TLT", "UNG", "USO",
]

# 4 column sets — 1-based column indices (matching build_option_chains.py)
# Each set: (strike_col, bid_col, ask_col) — skip RIC column
SETS = [
    {"col_start": 1},   # A-D  → strike=A(1), bid=C(3), ask=D(4)
    {"col_start": 6},   # F-I  → strike=F(6), bid=H(8), ask=I(9)
    {"col_start": 11},  # K-N  → strike=K(11), bid=M(13), ask=N(14)
    {"col_start": 16},  # P-S  → strike=P(16), bid=R(18), ask=S(19)
]

# Expiry date cells (1-based): B2, G2, L2, Q2
EXPIRY_CELLS = [(2, 2), (2, 7), (2, 12), (2, 17)]

# Rate cell
RATE_CELL = (3, 2)


# ---------------------------------------------------------------------------
# Reusable helpers (same patterns as _xl_reader.py)
# ---------------------------------------------------------------------------
def _output(data: dict, out_path: str | None) -> None:
    text = json.dumps(data)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        print(text)


def _find_workbook(workbook_name: str):
    import xlwings as xw
    for app in xw.apps:
        try:
            for book in app.books:
                if book.name.lower() == workbook_name.lower():
                    return book
        except OSError:
            continue
    return None


def _safe_float(val) -> float:
    if val is None or val == "":
        return 0.0
    if isinstance(val, str) and val.startswith("#"):
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Sheet reader — reads all 17 ticker blocks from one sheet
# ---------------------------------------------------------------------------
def _read_sheet(wb, sheet_name, opt_type, expiries):
    """Read all 17 ticker blocks from a single sheet.

    Returns (tickers_data, failed) where tickers_data is
    {TICKER: {"spot": float, "rows": [...]}} and failed is [TICKER, ...].
    """
    try:
        ws = wb.sheets[sheet_name]
    except Exception:
        # Sheet doesn't exist — return all tickers as failed
        return {}, list(TICKERS)

    tickers_data = {}
    failed = []

    for t_idx, ticker in enumerate(TICKERS):
        block_row = BLOCK_START_ROW + t_idx * BLOCK_SPACING
        header_row = block_row
        data_start = block_row + 2  # skip header + col-header

        # Spot price is in col B of the header row (set 0's RIC col = col 2)
        spot_val = ws.range((header_row, 2)).value
        spot = _safe_float(spot_val)
        if spot <= 0:
            failed.append(ticker)
            continue

        # Read all 4 expiry sets for this ticker
        rows = []
        for s_idx, s in enumerate(SETS):
            if s_idx >= len(expiries):
                break

            cs = s["col_start"]
            strike_col = cs       # A, F, K, P
            bid_col = cs + 2      # C, H, M, R
            ask_col = cs + 3      # D, I, N, S

            # Bulk read 11 rows for strike, bid, ask
            strikes = ws.range(
                (data_start, strike_col),
                (data_start + STRIKES_PER_TICKER - 1, strike_col)
            ).value
            bids = ws.range(
                (data_start, bid_col),
                (data_start + STRIKES_PER_TICKER - 1, bid_col)
            ).value
            asks = ws.range(
                (data_start, ask_col),
                (data_start + STRIKES_PER_TICKER - 1, ask_col)
            ).value

            for i in range(STRIKES_PER_TICKER):
                # xlwings returns a list of lists for multi-row ranges
                s_val = strikes[i][0] if isinstance(strikes[i], (list, tuple)) else strikes[i]
                b_val = bids[i][0] if isinstance(bids[i], (list, tuple)) else bids[i]
                a_val = asks[i][0] if isinstance(asks[i], (list, tuple)) else asks[i]

                strike = _safe_float(s_val)
                if strike <= 0:
                    continue

                bid = _safe_float(b_val)
                ask = _safe_float(a_val)

                if bid == 0 and ask == 0:
                    continue

                rows.append({
                    "expiry": expiries[s_idx],
                    "strike": strike,
                    "type": opt_type,
                    "bid": bid,
                    "ask": ask,
                    "last": 0.0,
                    "oi": 0,
                    "volume": 0,
                })

        if rows:
            tickers_data[ticker] = {"spot": spot, "rows": rows}
        else:
            failed.append(ticker)

    return tickers_data, failed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    workbook_name = sys.argv[1] if len(sys.argv) > 1 else "OptionChains.xlsm"
    out_path = sys.argv[2] if len(sys.argv) > 2 else None

    try:
        import pythoncom
        pythoncom.CoInitialize()

        wb = _find_workbook(workbook_name)
        if wb is None:
            _output({"ok": False, "error": f"Workbook '{workbook_name}' not found"}, out_path)
            return

        # --- Read global header from PutChains (expiries + rate) ---
        from datetime import datetime, date as dt_date
        try:
            ws_header = wb.sheets["PutChains"]
        except Exception:
            _output({"ok": False, "error": "PutChains sheet not found"}, out_path)
            return

        expiries = []
        for row, col in EXPIRY_CELLS:
            val = ws_header.range((row, col)).value
            if val:
                if isinstance(val, datetime):
                    expiries.append(val.strftime("%Y-%m-%d"))
                elif isinstance(val, dt_date):
                    expiries.append(val.isoformat())

        if not expiries:
            _output({"ok": False, "error": "No expiry dates found in PutChains sheet"}, out_path)
            return

        rate_val = ws_header.range((RATE_CELL[0], RATE_CELL[1])).value
        rate = float(rate_val) if rate_val else 0.045

        # --- Read both sheets ---
        puts, failed_puts = _read_sheet(wb, "PutChains", "P", expiries)
        calls, failed_calls = _read_sheet(wb, "CallChains", "C", expiries)

        _output({
            "ok": True,
            "rate": rate,
            "expiries": expiries,
            "puts": puts,
            "calls": calls,
            "failed_puts": failed_puts,
            "failed_calls": failed_calls,
        }, out_path)

    except Exception as e:
        _output({"ok": False, "error": str(e)}, out_path)


if __name__ == "__main__":
    main()
