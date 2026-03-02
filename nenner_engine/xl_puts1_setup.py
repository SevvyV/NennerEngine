"""
Create / refresh the "Options Prob" worksheet in Nenner_DataCenter.xlsm.
====================================================================

Adds a probability analysis table alongside option prices from Options_RT.
Column G computes implied volatility per-strike via a VBA Newton-Raphson
solver (ImpliedVol UDF), so IV updates live as market data streams in.

Columns:
  Strike | Expiry | DTE | Bid | Ask | Mid | Vol(%) | P(OTM) | P(Win) | OPT($)

P(OTM) = N(d2)            — probability option expires out-of-the-money
P(Win) = N(-d2_breakeven)  — probability covered put position is profitable
OPT($) = Bid x Shares     — total option income for the position

Usage:
    python -m nenner_engine.xl_puts1_setup          # create/refresh sheet
"""

from __future__ import annotations

import argparse
import sys
import time


# ---------------------------------------------------------------------------
# Sheet parameters
# ---------------------------------------------------------------------------

WORKBOOK = "Nenner_DataCenter.xlsm"
SOURCE_SHEET = "Options_RT"
TARGET_SHEET = "Options Prob"
OLD_SHEET_NAME = "Puts 1"  # for migration from old name

VBA_MODULE_NAME = "FischerIV"

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

# Formatting
CLR_HEADER_BG = "#1e293b"
CLR_HEADER_FG = "#ffffff"
CLR_BORDER = "#e2e8f0"
CLR_GREEN = "#16a34a"
CLR_BLUE = "#2563eb"
CLR_LIGHT_BLUE = "#eff6ff"
CLR_LIGHT_GREEN = "#f0fdf4"
CLR_INPUT_BG = "#fffde7"   # light yellow for editable cells


# ---------------------------------------------------------------------------
# VBA source code for the ImpliedVol UDF
# ---------------------------------------------------------------------------

VBA_SOURCE = r"""
Option Explicit

Private Const PI As Double = 3.14159265358979

Private Function NormPDF(x As Double) As Double
    NormPDF = Exp(-0.5 * x * x) / Sqr(2 * PI)
End Function

Private Function NormCDF(x As Double) As Double
    Dim t As Double, absX As Double
    Dim a1 As Double, a2 As Double, a3 As Double
    If x > 8 Then
        NormCDF = 1
        Exit Function
    ElseIf x < -8 Then
        NormCDF = 0
        Exit Function
    End If
    absX = Abs(x)
    t = 1 / (1 + 0.2316419 * absX)
    a1 = t * 1.330274429
    a2 = t * (-1.821255978 + a1)
    a3 = t * (1.781477937 + a2)
    a3 = t * (-0.356563782 + a3)
    a3 = t * (0.31938153 + a3)
    NormCDF = 1 - NormPDF(absX) * a3
    If x < 0 Then NormCDF = 1 - NormCDF
End Function

Private Sub CalcD1D2(S As Double, K As Double, T As Double, _
    r As Double, sigma As Double, q As Double, _
    ByRef d1 As Double, ByRef d2 As Double)
    Dim sqrtT As Double
    sqrtT = Sqr(T)
    d1 = (Log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
End Sub

Private Function MyMax(a As Double, b As Double) As Double
    If a > b Then MyMax = a Else MyMax = b
End Function

Public Function BSMPrice(S As Double, K As Double, T As Double, _
    r As Double, sigma As Double, q As Double, _
    ByVal OptType As String) As Double
    Dim d1 As Double, d2 As Double
    Dim disc As Double, fwdDisc As Double
    If T <= 0 Then
        If UCase(OptType) = "C" Then
            BSMPrice = MyMax(S - K, 0)
        Else
            BSMPrice = MyMax(K - S, 0)
        End If
        Exit Function
    End If
    If sigma <= 0 Then
        BSMPrice = 0
        Exit Function
    End If
    Call CalcD1D2(S, K, T, r, sigma, q, d1, d2)
    disc = Exp(-r * T)
    fwdDisc = Exp(-q * T)
    If UCase(OptType) = "C" Then
        BSMPrice = S * fwdDisc * NormCDF(d1) - K * disc * NormCDF(d2)
    Else
        BSMPrice = K * disc * NormCDF(-d2) - S * fwdDisc * NormCDF(-d1)
    End If
End Function

Private Function BSMVega(S As Double, K As Double, T As Double, _
    r As Double, sigma As Double, q As Double) As Double
    Dim d1 As Double, d2 As Double
    Call CalcD1D2(S, K, T, r, sigma, q, d1, d2)
    BSMVega = S * Exp(-q * T) * NormPDF(d1) * Sqr(T)
End Function

Private Function IVBisection(MktPrice As Double, S As Double, _
    K As Double, T As Double, r As Double, q As Double, _
    ByVal OptType As String, tol As Double) As Double
    Dim lo As Double, hi As Double, mid As Double
    Dim price As Double, finalPrice As Double
    Dim i As Long
    lo = 0.001
    hi = 10
    For i = 1 To 200
        mid = (lo + hi) / 2
        price = BSMPrice(S, K, T, r, mid, q, OptType)
        If Abs(price - MktPrice) < tol Then
            IVBisection = mid
            Exit Function
        End If
        If price > MktPrice Then
            hi = mid
        Else
            lo = mid
        End If
        If hi - lo < 0.00000001 Then Exit For
    Next i
    mid = (lo + hi) / 2
    finalPrice = BSMPrice(S, K, T, r, mid, q, OptType)
    If Abs(finalPrice - MktPrice) < tol * 10 Then
        IVBisection = mid
    Else
        IVBisection = -1
    End If
End Function

Public Function ImpliedVol(ByVal vS As Variant, ByVal vK As Variant, _
    ByVal vT As Variant, ByVal vR As Variant, _
    ByVal vQ As Variant, ByVal vMkt As Variant, _
    ByVal OptType As Variant) As Variant

    Dim S As Double, K As Double, T As Double
    Dim r As Double, q As Double, MktPrice As Double
    Dim sigma As Double, price As Double, diff As Double
    Dim vegaRaw As Double, intrinsic As Double
    Dim i As Long
    Const tol As Double = 0.001
    Const maxIter As Long = 100

    If Not IsNumeric(vS) Then GoTo BadInput
    If Not IsNumeric(vK) Then GoTo BadInput
    If Not IsNumeric(vT) Then GoTo BadInput
    If Not IsNumeric(vR) Then GoTo BadInput
    If Not IsNumeric(vQ) Then GoTo BadInput
    If Not IsNumeric(vMkt) Then GoTo BadInput
    If IsEmpty(vS) Or IsEmpty(vK) Then GoTo BadInput
    If IsEmpty(vT) Or IsEmpty(vMkt) Then GoTo BadInput

    S = CDbl(vS)
    K = CDbl(vK)
    T = CDbl(vT)
    r = CDbl(vR)
    q = CDbl(vQ)
    MktPrice = CDbl(vMkt)

    If T <= 0 Or MktPrice <= 0 Then GoTo BadInput
    If S <= 0 Or K <= 0 Then GoTo BadInput

    If UCase(OptType) = "C" Then
        intrinsic = MyMax(S - K, 0)
    Else
        intrinsic = MyMax(K - S, 0)
    End If
    If MktPrice < intrinsic - tol Then GoTo BadInput

    sigma = Sqr(2 * PI / T) * (MktPrice / S)
    If sigma < 0.01 Then sigma = 0.01
    If sigma > 5 Then sigma = 5

    For i = 1 To maxIter
        price = BSMPrice(S, K, T, r, sigma, q, CStr(OptType))
        diff = price - MktPrice
        If Abs(diff) < tol Then
            ImpliedVol = sigma
            Exit Function
        End If
        vegaRaw = BSMVega(S, K, T, r, sigma, q)
        If vegaRaw < 0.000000000001 Then
            sigma = IVBisection(MktPrice, S, K, T, r, q, CStr(OptType), tol)
            If sigma < 0 Then GoTo BadInput
            ImpliedVol = sigma
            Exit Function
        End If
        sigma = sigma - diff / vegaRaw
        If sigma < 0.001 Then sigma = 0.001
        If sigma > 10 Then sigma = 10
    Next i

    sigma = IVBisection(MktPrice, S, K, T, r, q, CStr(OptType), tol)
    If sigma < 0 Then GoTo BadInput
    ImpliedVol = sigma
    Exit Function

BadInput:
    ImpliedVol = ""
End Function
"""


# ---------------------------------------------------------------------------
# OLE retry helper (Excel busy during RTD recalc)
# ---------------------------------------------------------------------------

def _retry(fn, retries=5, delay=1.0):
    """Retry a callable that may hit OLE busy errors (0x800ac472)."""
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            if "800ac472" in str(e) and attempt < retries - 1:
                time.sleep(delay)
            else:
                raise


class _SafeCell:
    """Wrapper around xlwings range that retries OLE busy errors."""

    def __init__(self, rng):
        self._rng = rng

    @property
    def formula(self):
        return _retry(lambda: self._rng.formula)

    @formula.setter
    def formula(self, value):
        _retry(lambda: setattr(self._rng, 'formula', value))

    @property
    def value(self):
        return _retry(lambda: self._rng.value)

    @value.setter
    def value(self, v):
        _retry(lambda: setattr(self._rng, 'value', v))

    @property
    def number_format(self):
        return _retry(lambda: self._rng.number_format)

    @number_format.setter
    def number_format(self, v):
        _retry(lambda: setattr(self._rng, 'number_format', v))

    @property
    def font(self):
        return _SafeFont(self._rng.font)

    @property
    def color(self):
        return _retry(lambda: self._rng.color)

    @color.setter
    def color(self, v):
        _retry(lambda: setattr(self._rng, 'color', v))

    @property
    def column_width(self):
        return _retry(lambda: self._rng.column_width)

    @column_width.setter
    def column_width(self, v):
        _retry(lambda: setattr(self._rng, 'column_width', v))

    @property
    def api(self):
        return self._rng.api

    def merge(self):
        _retry(lambda: self._rng.merge())

    def select(self):
        _retry(lambda: self._rng.select())


class _SafeFont:
    """Wrapper around xlwings font that retries OLE busy errors."""

    def __init__(self, font):
        self._font = font

    def __setattr__(self, name, value):
        if name == '_font':
            super().__setattr__(name, value)
        else:
            _retry(lambda: setattr(self._font, name, value))


def _cell(ws, addr):
    """Get a safe-wrapped cell reference."""
    return _SafeCell(ws.range(addr))


def _cell_rc(ws, row, col):
    """Get a safe-wrapped cell by row/col tuple."""
    return _SafeCell(ws.range((row, col)))


# ---------------------------------------------------------------------------
# VBA module injection
# ---------------------------------------------------------------------------

def _inject_vba_module(wb) -> None:
    """Add or replace the FischerIV VBA module in the workbook.

    Writes a .bas file and uses VBComponents.Import() instead of
    AddFromString() to avoid encoding/line-ending issues.
    """
    import pathlib
    vb_project = wb.api.VBProject

    # Remove existing module if present
    for comp in vb_project.VBComponents:
        if comp.Name == VBA_MODULE_NAME:
            vb_project.VBComponents.Remove(comp)
            break

    # Write .bas file (ANSI/cp1252, CRLF line endings)
    bas_path = pathlib.Path(__file__).with_name("FischerIV.bas")
    safe_source = VBA_SOURCE.strip().encode('ascii', errors='replace').decode('ascii')
    with open(bas_path, 'w', encoding='cp1252', newline='\r\n') as f:
        f.write(f'Attribute VB_Name = "{VBA_MODULE_NAME}"\n')
        f.write(safe_source)
        f.write('\n')

    # Import .bas file (more reliable than AddFromString)
    vb_project.VBComponents.Import(str(bas_path))
    print(f"  VBA module '{VBA_MODULE_NAME}' injected")


# ---------------------------------------------------------------------------
# Main setup
# ---------------------------------------------------------------------------

def setup_options_prob():
    """Create or refresh the Options Prob worksheet."""
    import xlwings as xw

    wb = xw.Book(WORKBOOK)

    # Inject VBA IV solver
    _inject_vba_module(wb)

    # Handle rename from old "Puts 1" name
    sheet_names = [s.name for s in wb.sheets]
    if TARGET_SHEET in sheet_names:
        ws = wb.sheets[TARGET_SHEET]
        ws.clear()
    elif OLD_SHEET_NAME in sheet_names:
        ws = wb.sheets[OLD_SHEET_NAME]
        ws.name = TARGET_SHEET
        ws.clear()
        print(f"  Renamed '{OLD_SHEET_NAME}' -> '{TARGET_SHEET}'")
    else:
        ws = wb.sheets.add(TARGET_SHEET, after=wb.sheets[SOURCE_SHEET])

    src = f"'{SOURCE_SHEET}'"  # quoted sheet name for formulas

    # Pause automatic calculation while we build (avoids OLE busy errors)
    app = wb.app
    old_calc_mode = app.api.Calculation
    app.api.Calculation = -4135  # xlCalculationManual

    try:
        _build_sheet(ws, src)
    finally:
        # Restore calculation mode and recalc
        app.api.Calculation = old_calc_mode
        app.api.Calculate()

    print(f"Options Prob sheet created with {BLOCKS_PER_SECTION} expiry blocks, "
          f"{STRIKES_PER_BLOCK} strikes each, live IV via VBA UDF")


def _build_sheet(ws, src: str):
    """Build all sheet content (called with calculation paused)."""
    c = lambda addr: _cell(ws, addr)
    cr = lambda row, col: _cell_rc(ws, row, col)

    # -----------------------------------------------------------------------
    # Header area (rows 1-5)
    # -----------------------------------------------------------------------
    c("A1").value = "Options Prob \u2014 Probability Analysis"
    c("A1").font.size = 14
    c("A1").font.bold = True
    _SafeCell(ws.range("A1:J1")).merge()

    c("A2").value = "Ticker:"
    c("B2").formula = f"={src}!B1"
    c("B2").font.bold = True

    c("C2").value = "Spot:"
    c("D2").formula = f"={src}!B3"
    c("D2").number_format = "$#,##0.00"
    c("D2").font.bold = True

    c("E2").value = "Rate:"
    c("F2").formula = f"={src}!B4"
    c("F2").number_format = "0.00%"

    c("G2").value = "Div Yld:"
    c("H2").formula = f"={src}!B5"
    c("H2").number_format = "0.00%"

    # ATM Vol (%) — auto-computed from nearest-to-ATM strike in first expiry block
    # Data rows for block 0 start at row 8 and run 40 rows (8..47)
    c("A3").value = "ATM Vol(%):"
    c("A3").font.bold = True
    atm = c("B3")
    # CSE array formula: find strike nearest to spot, return its IV
    _retry(lambda: setattr(
        ws.range("B3").api, 'FormulaArray',
        '=IFERROR(INDEX(G8:G47,MATCH(MIN(ABS(A8:A47-$D$2)),ABS(A8:A47-$D$2),0)),"")'
    ))
    atm.number_format = "0.0"
    atm.font.bold = True
    atm.font.size = 13

    c("C3").value = "Shares:"
    d3 = c("D3")
    d3.formula = (
        f'=IFERROR(VLOOKUP({src}!B1,'
        '{"AAPL",1800;"BAC",10000;"GOOG",1600;"MSFT",1200;'
        '"NVDA",2800;"TSLA",1200;"QQQ",800;"SPY",700},2,FALSE),2000)'
    )
    d3.number_format = "#,##0"
    d3.font.bold = True

    # Column widths
    for col, w in {"A": 12, "B": 12, "C": 6, "D": 8, "E": 8, "F": 8,
                   "G": 8, "H": 9, "I": 9, "J": 11}.items():
        c(f"{col}1").column_width = w

    # -----------------------------------------------------------------------
    # Build 3 expiry blocks
    # -----------------------------------------------------------------------
    current_row = 5

    for block_idx in range(BLOCKS_PER_SECTION):
        expiry_cell = f"B{8 + block_idx}"
        dte_cell = f"D{8 + block_idx}"
        src_data_start = PUT_SECTION_START + block_idx * ROWS_PER_BLOCK + 1

        # --- Expiry header row ---
        current_row += 1
        c(f"A{current_row}").formula = (
            f'="Expiry: "&TEXT({src}!{expiry_cell},"MM/DD/YYYY")'
            f'&"  (DTE "&{src}!{dte_cell}&")"'
        )
        c(f"A{current_row}").font.bold = True
        c(f"A{current_row}").font.size = 11
        _SafeCell(ws.range(f"A{current_row}:J{current_row}")).color = CLR_LIGHT_BLUE

        # --- Column headers ---
        current_row += 1
        for i, h in enumerate(["Strike", "Expiry", "DTE", "Bid", "Ask", "Mid",
                                "Vol(%)", "P(OTM)", "P(Win)", "OPT($)"]):
            cell = cr(current_row, i + 1)
            cell.value = h
            cell.font.bold = True
            cell.font.color = CLR_HEADER_FG
            cell.color = CLR_HEADER_BG

        # --- Data rows (40 per block) ---
        for row_offset in range(STRIKES_PER_BLOCK):
            current_row += 1
            r = current_row
            sr = src_data_start + row_offset

            c(f"A{r}").formula = f"=IFERROR({src}!A{sr}*1,\"\")"
            c(f"A{r}").number_format = "$#,##0.00"

            c(f"B{r}").formula = f"=IFERROR({src}!{expiry_cell},\"\")"
            c(f"B{r}").number_format = "MM/DD"

            c(f"C{r}").formula = f"=IFERROR({src}!{dte_cell}*1,0)"
            c(f"C{r}").number_format = "0"

            c(f"D{r}").formula = f"=IFERROR({src}!C{sr}*1,0)"
            c(f"D{r}").number_format = "$#,##0.00"

            c(f"E{r}").formula = f"=IFERROR({src}!D{sr}*1,0)"
            c(f"E{r}").number_format = "$#,##0.00"

            # Mid
            c(f"F{r}").formula = (
                f'=IF(AND(D{r}>0,E{r}>0),(D{r}+E{r})/2,"")'
            )
            c(f"F{r}").number_format = "$#,##0.00"

            # Vol(%) — live IV from VBA UDF
            c(f"G{r}").formula = (
                f'=IFERROR(IF(OR(A{r}="",A{r}=0,C{r}<=0,F{r}=""),"",'
                f'ImpliedVol($D$2,A{r},C{r}/365.25,$F$2,$H$2,F{r},"P")*100),"")'
            )
            c(f"G{r}").number_format = "0.0"

            # P(OTM) = N(d2)
            c(f"H{r}").formula = (
                f'=IFERROR(IF(OR(A{r}="",A{r}=0,C{r}<=0,G{r}=""),"",NORM.S.DIST('
                f'(LN($D$2/A{r})+($F$2-$H$2-(G{r}/100)^2/2)*(C{r}/365.25))'
                f'/((G{r}/100)*SQRT(C{r}/365.25)),TRUE)),"")'
            )
            c(f"H{r}").number_format = "0.0%"

            # P(Win) = N(-d2_breakeven)
            c(f"I{r}").formula = (
                f'=IFERROR(IF(OR(A{r}="",A{r}=0,C{r}<=0,D{r}<=0,G{r}=""),"",NORM.S.DIST('
                f'-1*(LN($D$2/($D$2+D{r}))+($F$2-$H$2-(G{r}/100)^2/2)*(C{r}/365.25))'
                f'/((G{r}/100)*SQRT(C{r}/365.25)),TRUE)),"")'
            )
            c(f"I{r}").number_format = "0.0%"
            c(f"I{r}").font.bold = True

            # OPT($)
            c(f"J{r}").formula = f'=IFERROR(IF(D{r}>0,D{r}*$D$3,""),"")'
            c(f"J{r}").number_format = "$#,##0"
            c(f"J{r}").font.color = CLR_GREEN

        current_row += 1  # spacer

    # -----------------------------------------------------------------------
    # Notes at bottom
    # -----------------------------------------------------------------------
    current_row += 2
    c(f"A{current_row}").value = "Notes:"
    c(f"A{current_row}").font.bold = True
    for note in [
        "Vol(%) = implied volatility solved per-strike from mid price (Newton-Raphson)",
        "ATM Vol in B3 = IV of the strike nearest to spot in the first expiry",
        "P(OTM) = probability the put expires out-of-the-money (stock stays above strike)",
        "P(Win) = probability the covered put position is profitable at expiry",
        "OPT($) = total option income collected (bid x shares)",
    ]:
        current_row += 1
        c(f"A{current_row}").value = f"  {note}"
        c(f"A{current_row}").font.size = 9
        c(f"A{current_row}").font.color = "#64748b"

    # Freeze panes
    try:
        _retry(lambda: ws.activate())
        _retry(lambda: ws.range("A5").select())
        _retry(lambda: setattr(ws.book.app.api.ActiveWindow, 'FreezePanes', False))
        _retry(lambda: setattr(ws.book.app.api.ActiveWindow, 'FreezePanes', True))
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="Create Options Prob probability sheet")
    args = parser.parse_args()
    setup_options_prob()


if __name__ == "__main__":
    main()
