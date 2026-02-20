"""
Position Tracker
=================
Reads live trade positions from TSLA_Options.xlsm via xlwings,
parses option codes, computes dollar P/L, and links to Nenner signals.

The Excel workbook is read-only — no cells are written or modified.
"""

import logging
import re
import sqlite3
from typing import Optional

log = logging.getLogger("nenner")


# ---------------------------------------------------------------------------
# Option Code Parser
# ---------------------------------------------------------------------------
# Format: [TICKER][YYMM][TYPE_CODE][STRIKE]
# Examples: TSLA2620N410, BAC2620N51, SIL2715M85
# Type codes: N=Put near-term, M=Put far-term, B=Call near-term, A=Call far-term

_OPTION_RE = re.compile(
    r"^([A-Z]+)"        # underlying ticker
    r"(\d{2})(\d{2})"   # YY DD (year + day-of-month; month from type code context)
    r"([NMBA])"          # type code
    r"([\d.]+)$"         # strike price
)

OPTION_TYPE_MAP = {
    "N": "PUT",
    "M": "PUT",
    "B": "CALL",
    "A": "CALL",
}


def parse_option_code(code: str) -> Optional[dict]:
    """Parse an option code like TSLA2620N410 into components.

    Returns dict with keys: underlying, option_type (PUT/CALL), strike,
    type_code, year, day.
    Returns None if code is a plain stock ticker (no match).
    """
    if not code:
        return None
    m = _OPTION_RE.match(code.strip())
    if not m:
        return None
    underlying, yy, dd, type_code, strike_str = m.groups()
    return {
        "underlying": underlying,
        "option_type": OPTION_TYPE_MAP[type_code],
        "strike": float(strike_str),
        "type_code": type_code,
        "year": int(yy) + 2000,
        "day": int(dd),
    }


# ---------------------------------------------------------------------------
# Trade Sheet Configuration
# ---------------------------------------------------------------------------
# Standard sheets share a common layout; the collar sheet is different.

STANDARD_SHEETS = {
    "TradeSheet PUTS": "covered_put",
    "TradeSheet #2 Puts": "covered_put",
    "TradeSheet Calls": "covered_call",
}

COLLAR_SHEET = "Put_Call Trade"

# Standard layout constants
_UNDERLYING_TICKER_CELL = "B4"
_UNDERLYING_BID_CELL = "C4"
_UNDERLYING_ASK_CELL = "D4"
_UNDERLYING_LAST_CELL = "E4"
_NEAR_EXPIRY_CELL = "B5"
_NEXT_EXPIRY_CELL = "B6"

_TRADE_START_ROW = 12
_TRADE_END_ROW = 22
_COL_SIDE = "B"
_COL_TICKER = "C"
_COL_SHARES = "D"
_COL_PRICE = "E"
_COL_PROCEEDS = "F"


# ---------------------------------------------------------------------------
# Sheet Readers
# ---------------------------------------------------------------------------

def _read_underlying_info(ws) -> dict:
    """Read header section of a standard trade sheet."""
    ticker = ws.range(_UNDERLYING_TICKER_CELL).value
    bid = ws.range(_UNDERLYING_BID_CELL).value
    ask = ws.range(_UNDERLYING_ASK_CELL).value
    last = ws.range(_UNDERLYING_LAST_CELL).value
    near_expiry = ws.range(_NEAR_EXPIRY_CELL).value
    next_expiry = ws.range(_NEXT_EXPIRY_CELL).value

    return {
        "ticker": str(ticker).strip() if ticker else None,
        "bid": float(bid) if bid else None,
        "ask": float(ask) if ask else None,
        "last": float(last) if last else None,
        "near_expiry": str(near_expiry)[:10] if near_expiry else None,
        "next_expiry": str(next_expiry)[:10] if next_expiry else None,
    }


def _read_standard_trade_sheet(ws) -> list[dict]:
    """Read trade legs from a standard layout sheet (rows 12-22).

    Returns list of leg dicts. Skips rows with 0 or None shares/price.
    """
    legs = []
    for row in range(_TRADE_START_ROW, _TRADE_END_ROW + 1):
        side = ws.range(f"{_COL_SIDE}{row}").value
        ticker = ws.range(f"{_COL_TICKER}{row}").value
        shares = ws.range(f"{_COL_SHARES}{row}").value
        price = ws.range(f"{_COL_PRICE}{row}").value
        proceeds = ws.range(f"{_COL_PROCEEDS}{row}").value

        if not side or not shares:
            continue

        side_str = str(side).strip().upper()
        if side_str not in ("SHORT", "LONG"):
            continue

        shares_val = float(shares) if shares else 0
        price_val = float(price) if price else 0
        proceeds_val = float(proceeds) if proceeds else 0

        # Skip empty trade slots (shares > 0 but price = 0 and no ticker)
        if price_val == 0 and not ticker:
            continue

        ticker_str = str(ticker).strip() if ticker else None
        option_info = parse_option_code(ticker_str) if ticker_str else None

        legs.append({
            "side": side_str,
            "ticker": ticker_str,
            "shares": shares_val,
            "entry_price": abs(price_val),
            "proceeds": proceeds_val,
            "is_option": option_info is not None,
            "option_type": option_info["option_type"] if option_info else None,
            "strike": option_info["strike"] if option_info else None,
        })

    return legs


def _read_collar_sheet(ws) -> dict:
    """Read the Put_Call Trade sheet (collar layout).

    Layout:
      Row 4: A:Ticker, B:SIL, C:103.22, E:Put Strike, F:130.0
      Row 5: A:Expiry, B:2027-01-15, E:Call Strike, F:85.0
      Row 7: B:Put to sell, C:130.0, D:36.9 (put premium)
      Row 8: B:Call to buy, C:100.0, D:25.8 (call premium)
      Row 11: D:Put Proceeds, E:10 (contracts), F:36900
      Row 12: D:Call Cost, E:-25 (contracts), F:-64500

    Returns position dict with legs, or None if sheet is empty.
    """
    ticker = ws.range("B4").value
    bid = ws.range("C4").value
    put_strike = ws.range("F4").value
    call_strike = ws.range("F5").value
    expiry = ws.range("B5").value

    if not ticker:
        return None

    put_contracts = ws.range("E11").value
    put_proceeds = ws.range("F11").value
    call_contracts = ws.range("E12").value
    call_cost = ws.range("F12").value

    legs = []

    # Put leg (sold)
    if put_contracts and put_proceeds:
        put_premium = ws.range("D7").value
        legs.append({
            "side": "SHORT",
            "ticker": None,  # collar puts don't use option codes in this sheet
            "shares": abs(float(put_contracts)) * 100,  # contracts to shares
            "entry_price": float(put_premium) if put_premium else 0,
            "proceeds": float(put_proceeds),
            "is_option": True,
            "option_type": "PUT",
            "strike": float(put_strike) if put_strike else None,
        })

    # Call leg (bought)
    if call_contracts and call_cost:
        call_premium = ws.range("D8").value
        legs.append({
            "side": "LONG",
            "ticker": None,
            "shares": abs(float(call_contracts)) * 100,
            "entry_price": float(call_premium) if call_premium else 0,
            "proceeds": float(call_cost),
            "is_option": True,
            "option_type": "CALL",
            "strike": float(call_strike) if call_strike else None,
        })

    return {
        "sheet_name": COLLAR_SHEET,
        "strategy": "collar",
        "underlying": str(ticker).strip(),
        "underlying_bid": float(bid) if bid else None,
        "expiry": str(expiry)[:10] if expiry else None,
        "legs": legs,
    }


# ---------------------------------------------------------------------------
# Main Position Reader
# ---------------------------------------------------------------------------

def read_positions() -> list[dict]:
    """Read all trade positions from TSLA_Options.xlsm via xlwings.

    Returns one dict per trade sheet (position group):
      {sheet_name, strategy, underlying, underlying_bid, legs: [...]}

    Returns empty list if workbook is unavailable.
    """
    try:
        import xlwings as xw
    except ImportError:
        log.debug("xlwings not installed — position tracking unavailable")
        return []

    from .prices import T1_WORKBOOK

    try:
        wb = xw.Book(T1_WORKBOOK)
    except Exception as e:
        log.debug(f"Cannot open workbook for positions: {e}")
        return []

    positions = []

    # Standard trade sheets
    for sheet_name, strategy in STANDARD_SHEETS.items():
        try:
            ws = wb.sheets[sheet_name]
        except Exception:
            log.debug(f"Sheet '{sheet_name}' not found, skipping")
            continue

        info = _read_underlying_info(ws)
        legs = _read_standard_trade_sheet(ws)

        if info["ticker"] and legs:
            positions.append({
                "sheet_name": sheet_name,
                "strategy": strategy,
                "underlying": info["ticker"],
                "underlying_bid": info["bid"],
                "underlying_ask": info["ask"],
                "underlying_last": info["last"],
                "near_expiry": info["near_expiry"],
                "legs": legs,
            })

    # Collar sheet
    try:
        ws = wb.sheets[COLLAR_SHEET]
        collar = _read_collar_sheet(ws)
        if collar and collar["legs"]:
            positions.append(collar)
    except Exception as e:
        log.debug(f"Error reading collar sheet: {e}")

    log.info(f"Position tracker: read {len(positions)} position groups")
    return positions


# ---------------------------------------------------------------------------
# P/L Calculator
# ---------------------------------------------------------------------------

def compute_position_pnl(position: dict, current_price: float) -> dict:
    """Compute dollar P/L for a position group given the current underlying price.

    Stock legs: (current - entry) * shares for LONG, (entry - current) * shares for SHORT.
    Option legs: intrinsic value minus entry premium, scaled by shares.
      PUT intrinsic = max(0, strike - current_price)
      CALL intrinsic = max(0, current_price - strike)
    """
    stock_pnl = 0.0
    option_pnl = 0.0
    total_proceeds = 0.0

    for leg in position.get("legs", []):
        shares = leg["shares"]
        entry = leg["entry_price"]
        proceeds = leg.get("proceeds", 0)
        total_proceeds += proceeds

        if not leg["is_option"]:
            # Stock leg
            if leg["side"] == "LONG":
                stock_pnl += (current_price - entry) * shares
            else:  # SHORT
                stock_pnl += (entry - current_price) * shares
        else:
            # Option leg — intrinsic value approximation
            strike = leg.get("strike", 0)
            if leg["option_type"] == "PUT":
                intrinsic = max(0.0, strike - current_price)
            else:  # CALL
                intrinsic = max(0.0, current_price - strike)

            if leg["side"] == "SHORT":
                # Sold option: profit = premium received - current intrinsic
                option_pnl += (entry - intrinsic) * shares
            else:  # LONG
                # Bought option: profit = current intrinsic - premium paid
                option_pnl += (intrinsic - entry) * shares

    return {
        "stock_pnl_dollar": round(stock_pnl, 2),
        "option_pnl_dollar": round(option_pnl, 2),
        "total_pnl_dollar": round(stock_pnl + option_pnl, 2),
        "total_proceeds": round(total_proceeds, 2),
    }


# ---------------------------------------------------------------------------
# Signal Integration
# ---------------------------------------------------------------------------

def get_positions_with_signal_context(
    conn: sqlite3.Connection,
    positions: Optional[list[dict]] = None,
    try_t1: bool = True,
) -> list[dict]:
    """Join position data with Nenner signal state and current prices.

    For each position group, looks up the underlying in current_state and
    enriches with signal direction, cancel level, and dollar P/L.
    """
    if positions is None:
        positions = read_positions()
    if not positions:
        return []

    # Get current prices — returns {ticker: {"price": float, ...}}
    from .prices import get_current_prices
    raw_prices = get_current_prices(conn, try_t1=try_t1)
    prices = {tk: v["price"] for tk, v in raw_prices.items() if v.get("price")}

    enriched = []
    for pos in positions:
        underlying = pos["underlying"]

        # Look up Nenner signal
        row = conn.execute(
            "SELECT effective_signal, origin_price, cancel_level, "
            "cancel_direction, trigger_level, implied_reversal, "
            "last_signal_date FROM current_state WHERE ticker = ?",
            (underlying,),
        ).fetchone()

        # Get current price (from T1 or cache, fall back to workbook bid)
        current_price = prices.get(underlying) or pos.get("underlying_bid")

        pnl = {"stock_pnl_dollar": 0, "option_pnl_dollar": 0,
               "total_pnl_dollar": 0, "total_proceeds": 0}
        if current_price:
            pnl = compute_position_pnl(pos, current_price)

        cancel_dist_pct = None
        if row and row[2] and current_price:
            cancel_dist_pct = (row[2] - current_price) / abs(current_price) * 100

        entry = {
            "sheet_name": pos["sheet_name"],
            "strategy": pos["strategy"],
            "underlying": underlying,
            "current_price": current_price,
            "underlying_bid": pos.get("underlying_bid"),
            "near_expiry": pos.get("near_expiry"),
            "legs": pos["legs"],
            # P/L
            "stock_pnl_dollar": pnl["stock_pnl_dollar"],
            "option_pnl_dollar": pnl["option_pnl_dollar"],
            "total_pnl_dollar": pnl["total_pnl_dollar"],
            "total_proceeds": pnl["total_proceeds"],
            # Nenner signal context
            "nenner_signal": row[0] if row else None,
            "origin_price": row[1] if row else None,
            "cancel_level": row[2] if row else None,
            "cancel_direction": row[3] if row else None,
            "trigger_level": row[4] if row else None,
            "implied_reversal": bool(row[5]) if row else None,
            "last_signal_date": row[6] if row else None,
            "cancel_dist_pct": round(cancel_dist_pct, 2) if cancel_dist_pct is not None else None,
        }
        enriched.append(entry)

    return enriched


def get_held_tickers(positions: Optional[list[dict]] = None) -> set[str]:
    """Return set of underlying tickers that have open positions."""
    if positions is None:
        positions = read_positions()
    return {p["underlying"] for p in positions if p.get("legs")}
