"""
Create / refresh the "Puts 1" worksheet in Nenner_DataCenter.xlsm.
=================================================================

Adds a probability analysis table alongside put option prices from Options_RT.
All columns use Excel formulas referencing Options_RT live RTD data,
so everything auto-updates as market data streams in.

Columns:
  Strike | Expiry | DTE | Bid | Ask | Mid | Vol(%) | P(OTM) | P(Win) | OPT($)

P(OTM) = N(d2)            — probability put expires out-of-the-money
P(Win) = N(-d2_breakeven)  — probability covered put position is profitable
OPT($) = Bid × Shares     — total option income for the position

Usage:
    python -m nenner_engine.xl_puts1_setup          # create/refresh sheet
    python -m nenner_engine.xl_puts1_setup --vol 20 # set default vol to 20%
"""

from __future__ import annotations

import argparse
import sys


# ---------------------------------------------------------------------------
# Sheet parameters
# ---------------------------------------------------------------------------

WORKBOOK = "Nenner_DataCenter.xlsm"
SOURCE_SHEET = "Options_RT"
TARGET_SHEET = "Puts 1"

# Options_RT layout
BLOCKS_PER_SECTION = 3       # 3 expiry blocks
ROWS_PER_BLOCK = 42          # rows reserved per block in Options_RT
PUT_SECTION_START = 12       # row 12 = first expiry header in Options_RT
STRIKES_PER_BLOCK = 40       # data rows per block

# Share allocations (same as fischer_daily_report.py)
SHARE_ALLOC: dict[str, int] = {
    "AAPL": 1_800,
    "BAC":  10_000,
    "GOOG": 1_600,
    "MSFT": 1_200,
    "NVDA": 2_800,
    "TSLA": 1_200,
    "QQQ":  800,
    "SPY":  700,
}
DEFAULT_SHARES = 2_000

DEFAULT_VOL = 20.0  # default implied vol (%)

# Formatting
CLR_HEADER_BG = "#1e293b"
CLR_HEADER_FG = "#ffffff"
CLR_BORDER = "#e2e8f0"
CLR_GREEN = "#16a34a"
CLR_BLUE = "#2563eb"
CLR_LIGHT_BLUE = "#eff6ff"
CLR_LIGHT_GREEN = "#f0fdf4"
CLR_INPUT_BG = "#fffde7"   # light yellow for editable cells


def setup_puts1(vol_default: float = DEFAULT_VOL):
    """Create or refresh the Puts 1 worksheet."""
    import xlwings as xw

    wb = xw.Book(WORKBOOK)

    # Create sheet if it doesn't exist
    sheet_names = [s.name for s in wb.sheets]
    if TARGET_SHEET in sheet_names:
        ws = wb.sheets[TARGET_SHEET]
        ws.clear()
    else:
        ws = wb.sheets.add(TARGET_SHEET, after=wb.sheets[SOURCE_SHEET])

    src = f"'{SOURCE_SHEET}'"  # quoted sheet name for formulas

    # -----------------------------------------------------------------------
    # Header area (rows 1-5)
    # -----------------------------------------------------------------------
    ws.range("A1").value = "Puts 1 — Probability Analysis"
    ws.range("A1").font.size = 14
    ws.range("A1").font.bold = True
    ws.range("A1:J1").merge()

    # Link to Options_RT header
    ws.range("A2").value = "Ticker:"
    ws.range("B2").formula = f"={src}!B1"
    ws.range("B2").font.bold = True

    ws.range("C2").value = "Spot:"
    ws.range("D2").formula = f"={src}!B3"
    ws.range("D2").number_format = "$#,##0.00"
    ws.range("D2").font.bold = True

    ws.range("E2").value = "Rate:"
    ws.range("F2").formula = f"={src}!B4"
    ws.range("F2").number_format = "0.00%"

    ws.range("G2").value = "Div Yld:"
    ws.range("H2").formula = f"={src}!B5"
    ws.range("H2").number_format = "0.00%"

    # Vol input cell — user can edit this for hypotheticals
    ws.range("A3").value = "Vol (%):"
    ws.range("A3").font.bold = True
    vol_cell = ws.range("B3")
    vol_cell.value = vol_default
    vol_cell.number_format = "0.0"
    vol_cell.font.bold = True
    vol_cell.font.size = 13
    vol_cell.color = CLR_INPUT_BG

    ws.range("C3").value = "Shares:"
    shares_cell = ws.range("D3")
    shares_cell.formula = (
        f'=IFERROR(VLOOKUP({src}!B1,'
        '{"AAPL",1800;"BAC",10000;"GOOG",1600;"MSFT",1200;'
        '"NVDA",2800;"TSLA",1200;"QQQ",800;"SPY",700},2,FALSE),2000)'
    )
    shares_cell.number_format = "#,##0"
    shares_cell.font.bold = True

    # Column widths
    col_widths = {"A": 10, "B": 12, "C": 6, "D": 8, "E": 8, "F": 8,
                  "G": 8, "H": 9, "I": 9, "J": 11}
    for col, w in col_widths.items():
        ws.range(f"{col}1").column_width = w

    # -----------------------------------------------------------------------
    # Build 3 expiry blocks
    # -----------------------------------------------------------------------
    current_row = 5  # start writing from row 5

    for block_idx in range(BLOCKS_PER_SECTION):
        # Expiry date cell reference in Options_RT
        expiry_cell = f"B{8 + block_idx}"      # B8, B9, B10
        dte_cell = f"D{8 + block_idx}"          # D8, D9, D10

        # Data start row in Options_RT for this block
        src_data_start = PUT_SECTION_START + block_idx * ROWS_PER_BLOCK + 1

        # --- Expiry header row ---
        current_row += 1
        ws.range(f"A{current_row}").formula = (
            f'="Expiry: "&TEXT({src}!{expiry_cell},"MM/DD/YYYY")'
            f'&"  (DTE "&{src}!{dte_cell}&")"'
        )
        ws.range(f"A{current_row}").font.bold = True
        ws.range(f"A{current_row}").font.size = 11
        ws.range(f"A{current_row}:J{current_row}").color = CLR_LIGHT_BLUE

        # --- Column headers ---
        current_row += 1
        headers = ["Strike", "Expiry", "DTE", "Bid", "Ask", "Mid",
                    "Vol(%)", "P(OTM)", "P(Win)", "OPT($)"]
        for i, h in enumerate(headers):
            cell = ws.range((current_row, i + 1))
            cell.value = h
            cell.font.bold = True
            cell.font.color = CLR_HEADER_FG
            cell.color = CLR_HEADER_BG

        # --- Data rows (40 per block) ---
        for row_offset in range(STRIKES_PER_BLOCK):
            current_row += 1
            src_row = src_data_start + row_offset

            # A: Strike (from Options_RT)
            ws.range(f"A{current_row}").formula = f"={src}!A{src_row}"
            ws.range(f"A{current_row}").number_format = "$#,##0.00"

            # B: Expiry date
            ws.range(f"B{current_row}").formula = f"={src}!{expiry_cell}"
            ws.range(f"B{current_row}").number_format = "MM/DD"

            # C: DTE
            ws.range(f"C{current_row}").formula = f"={src}!{dte_cell}"
            ws.range(f"C{current_row}").number_format = "0"

            # D: Bid
            ws.range(f"D{current_row}").formula = f"={src}!C{src_row}"
            ws.range(f"D{current_row}").number_format = "$#,##0.00"

            # E: Ask
            ws.range(f"E{current_row}").formula = f"={src}!D{src_row}"
            ws.range(f"E{current_row}").number_format = "$#,##0.00"

            # F: Mid = (Bid+Ask)/2
            ws.range(f"F{current_row}").formula = (
                f"=IF(AND(D{current_row}>0,E{current_row}>0),"
                f"(D{current_row}+E{current_row})/2,\"\")"
            )
            ws.range(f"F{current_row}").number_format = "$#,##0.00"

            # G: Vol (%) — defaults to header cell, user can override per row
            ws.range(f"G{current_row}").formula = f"=$B$3"
            ws.range(f"G{current_row}").number_format = "0.0"
            ws.range(f"G{current_row}").color = CLR_INPUT_BG

            # H: P(OTM) = N(d2)
            # d2 = (LN(S/K) + (r - q - sigma^2/2) * T) / (sigma * SQRT(T))
            # where sigma = G{row}/100, T = C{row}/365.25
            # P(OTM) for put = N(d2) = probability stock stays ABOVE strike
            ws.range(f"H{current_row}").formula = (
                f'=IF(OR(A{current_row}="",A{current_row}=0,C{current_row}<=0),"",'
                f"NORM.S.DIST("
                f"(LN($D$2/A{current_row})"
                f"+($F$2-$H$2-(G{current_row}/100)^2/2)"
                f"*(C{current_row}/365.25))"
                f"/((G{current_row}/100)"
                f"*SQRT(C{current_row}/365.25))"
                f",TRUE))"
            )
            ws.range(f"H{current_row}").number_format = "0.0%"

            # I: P(Win) = P(S_T < Spot + Bid) = N(-d2_breakeven)
            # Breakeven for covered put = Spot + Bid
            # d2_be = (LN(S/BE) + (r - q - sigma^2/2)*T) / (sigma*SQRT(T))
            # P(Win) = 1 - N(d2_be) = N(-d2_be)
            ws.range(f"I{current_row}").formula = (
                f'=IF(OR(A{current_row}="",A{current_row}=0,C{current_row}<=0,D{current_row}<=0),"",'
                f"NORM.S.DIST("
                f"-1*(LN($D$2/($D$2+D{current_row}))"
                f"+($F$2-$H$2-(G{current_row}/100)^2/2)"
                f"*(C{current_row}/365.25))"
                f"/((G{current_row}/100)"
                f"*SQRT(C{current_row}/365.25))"
                f",TRUE))"
            )
            ws.range(f"I{current_row}").number_format = "0.0%"
            ws.range(f"I{current_row}").font.bold = True

            # J: OPT($) = Bid × Shares
            ws.range(f"J{current_row}").formula = (
                f'=IF(D{current_row}>0,D{current_row}*$D$3,"")'
            )
            ws.range(f"J{current_row}").number_format = "$#,##0"
            ws.range(f"J{current_row}").font.color = CLR_GREEN

        # Spacer row between blocks
        current_row += 1

    # -----------------------------------------------------------------------
    # Notes at bottom
    # -----------------------------------------------------------------------
    current_row += 2
    notes = [
        "P(OTM) = probability the put expires out-of-the-money (stock stays above strike)",
        "P(Win) = probability the covered put position is profitable at expiry",
        "OPT($) = total option income collected (bid x shares)",
        "Change Vol(%) in B3 to run hypotheticals across all strikes at once",
        "Override Vol(%) in column G for per-strike hypotheticals",
    ]
    ws.range(f"A{current_row}").value = "Notes:"
    ws.range(f"A{current_row}").font.bold = True
    for note in notes:
        current_row += 1
        ws.range(f"A{current_row}").value = f"  {note}"
        ws.range(f"A{current_row}").font.size = 9
        ws.range(f"A{current_row}").font.color = "#64748b"

    # Activate the new sheet and freeze panes
    try:
        ws.activate()
        ws.range("A5").select()
        wb.app.api.ActiveWindow.FreezePanes = False
        wb.app.api.ActiveWindow.FreezePanes = True
    except Exception:
        pass  # freeze panes may fail if Excel window isn't focused
    print(f"Puts 1 sheet created with {BLOCKS_PER_SECTION} expiry blocks, "
          f"{STRIKES_PER_BLOCK} strikes each, default vol={vol_default}%")


def main():
    parser = argparse.ArgumentParser(description="Create Puts 1 probability sheet")
    parser.add_argument("--vol", type=float, default=DEFAULT_VOL,
                        help="Default implied volatility in %% (e.g., 20)")
    args = parser.parse_args()
    setup_puts1(vol_default=args.vol)


if __name__ == "__main__":
    main()
