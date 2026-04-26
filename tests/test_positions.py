"""Tests for positions module — option-code parser and P/L math.

Skips Excel/xlwings-dependent paths (workbook reading), since those
require Windows COM and a live workbook. The pure-logic surface is
where money math happens, and that's what the audit flagged as
critical and untested.
"""

import math
import unittest

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from nenner_engine.positions import (
    parse_option_code,
    compute_position_pnl,
    get_held_tickers,
)


class TestParseOptionCode(unittest.TestCase):
    """[TICKER][YY][DD][TYPE_CODE][STRIKE] format.

    Examples from the codebase comments: TSLA2620N410, BAC2620N51,
    SIL2715M85. N/M = PUT, B/A = CALL.
    """

    def test_tsla_put_near_term(self):
        out = parse_option_code("TSLA2620N410")
        self.assertEqual(out["underlying"], "TSLA")
        self.assertEqual(out["option_type"], "PUT")
        self.assertEqual(out["strike"], 410.0)
        self.assertEqual(out["type_code"], "N")
        self.assertEqual(out["year"], 2026)
        self.assertEqual(out["day"], 20)

    def test_bac_put(self):
        out = parse_option_code("BAC2620N51")
        self.assertEqual(out["underlying"], "BAC")
        self.assertEqual(out["option_type"], "PUT")
        self.assertEqual(out["strike"], 51.0)

    def test_far_term_put(self):
        out = parse_option_code("SIL2715M85")
        self.assertEqual(out["option_type"], "PUT")
        self.assertEqual(out["type_code"], "M")
        self.assertEqual(out["strike"], 85.0)

    def test_call_codes_resolve(self):
        for code, expected_type in [("AAPL2620B270", "CALL"),
                                     ("MSFT2715A420", "CALL")]:
            out = parse_option_code(code)
            self.assertIsNotNone(out, f"failed to parse {code}")
            self.assertEqual(out["option_type"], expected_type)

    def test_decimal_strike(self):
        out = parse_option_code("BAC2620N52.5")
        self.assertEqual(out["strike"], 52.5)

    def test_plain_stock_ticker_returns_none(self):
        """A plain symbol like 'AAPL' should NOT parse as an option."""
        self.assertIsNone(parse_option_code("AAPL"))
        self.assertIsNone(parse_option_code("TSLA"))

    def test_empty_input(self):
        self.assertIsNone(parse_option_code(""))
        self.assertIsNone(parse_option_code(None))

    def test_whitespace_is_stripped(self):
        out = parse_option_code("  TSLA2620N410  ")
        self.assertIsNotNone(out)
        self.assertEqual(out["underlying"], "TSLA")

    def test_lowercase_is_rejected(self):
        """Option codes are uppercase by convention; mixed case is bad data."""
        self.assertIsNone(parse_option_code("tsla2620n410"))


class TestComputePositionPnl(unittest.TestCase):
    """Stock and option leg dollar P/L math.

    Stock LONG: (cur - entry) * shares
    Stock SHORT: (entry - cur) * shares
    Option leg uses intrinsic-value approximation (no time premium).
    """

    def _stock_leg(self, side, entry, shares, proceeds=0.0):
        return {
            "is_option": False, "side": side,
            "entry_price": entry, "shares": shares, "proceeds": proceeds,
        }

    def _option_leg(self, option_type, side, entry, strike, shares,
                    proceeds=0.0):
        return {
            "is_option": True, "option_type": option_type, "side": side,
            "entry_price": entry, "strike": strike,
            "shares": shares, "proceeds": proceeds,
        }

    def test_long_stock_in_profit(self):
        pos = {"legs": [self._stock_leg("LONG", entry=100, shares=10)]}
        out = compute_position_pnl(pos, current_price=110)
        self.assertEqual(out["stock_pnl_dollar"], 100.0)
        self.assertEqual(out["option_pnl_dollar"], 0.0)
        self.assertEqual(out["total_pnl_dollar"], 100.0)

    def test_long_stock_in_loss(self):
        pos = {"legs": [self._stock_leg("LONG", entry=100, shares=10)]}
        out = compute_position_pnl(pos, current_price=90)
        self.assertEqual(out["stock_pnl_dollar"], -100.0)

    def test_short_stock_in_profit(self):
        pos = {"legs": [self._stock_leg("SHORT", entry=100, shares=10)]}
        out = compute_position_pnl(pos, current_price=90)
        self.assertEqual(out["stock_pnl_dollar"], 100.0)

    def test_short_put_otm_keeps_full_premium(self):
        """Sold PUT, current price above strike → intrinsic 0 → P/L = full
        premium received per share × shares. The classic covered-put win."""
        pos = {"legs": [self._option_leg(
            "PUT", "SHORT", entry=2.50, strike=95.0, shares=100,
        )]}
        out = compute_position_pnl(pos, current_price=100.0)
        self.assertEqual(out["option_pnl_dollar"], 250.0)

    def test_short_put_itm_loses_intrinsic(self):
        """Sold PUT at $2.50, strike 95, underlying drops to 90 → intrinsic
        is $5. P/L = (2.50 - 5.00) × 100 = -$250."""
        pos = {"legs": [self._option_leg(
            "PUT", "SHORT", entry=2.50, strike=95.0, shares=100,
        )]}
        out = compute_position_pnl(pos, current_price=90.0)
        self.assertEqual(out["option_pnl_dollar"], -250.0)

    def test_short_call_otm_keeps_full_premium(self):
        pos = {"legs": [self._option_leg(
            "CALL", "SHORT", entry=3.00, strike=110.0, shares=100,
        )]}
        out = compute_position_pnl(pos, current_price=105.0)
        self.assertEqual(out["option_pnl_dollar"], 300.0)

    def test_short_call_itm_loses_intrinsic(self):
        pos = {"legs": [self._option_leg(
            "CALL", "SHORT", entry=3.00, strike=110.0, shares=100,
        )]}
        out = compute_position_pnl(pos, current_price=120.0)
        # (3 - 10) * 100 = -700
        self.assertEqual(out["option_pnl_dollar"], -700.0)

    def test_long_put_at_strike_no_intrinsic(self):
        """Bought PUT, underlying at strike → intrinsic 0 → loss = full premium."""
        pos = {"legs": [self._option_leg(
            "PUT", "LONG", entry=2.50, strike=95.0, shares=100,
        )]}
        out = compute_position_pnl(pos, current_price=95.0)
        self.assertEqual(out["option_pnl_dollar"], -250.0)

    def test_collar_combines_stock_and_options(self):
        """Long 100 stock + short call + long put — values net correctly."""
        pos = {"legs": [
            self._stock_leg("LONG", entry=100.0, shares=100),
            self._option_leg("CALL", "SHORT", entry=3.0, strike=110.0,
                             shares=100, proceeds=300.0),
            self._option_leg("PUT", "LONG", entry=2.0, strike=90.0,
                             shares=100, proceeds=-200.0),
        ]}
        out = compute_position_pnl(pos, current_price=105.0)
        # Stock: (105-100)*100 = 500
        # Short call: (3 - max(0, 105-110))*100 = (3-0)*100 = 300
        # Long put: (max(0, 90-105) - 2)*100 = (0 - 2)*100 = -200
        # Total: 500 + 300 - 200 = 600
        self.assertEqual(out["stock_pnl_dollar"], 500.0)
        self.assertEqual(out["option_pnl_dollar"], 100.0)
        self.assertEqual(out["total_pnl_dollar"], 600.0)
        self.assertEqual(out["total_proceeds"], 100.0)

    def test_empty_position(self):
        out = compute_position_pnl({"legs": []}, current_price=100.0)
        self.assertEqual(out["total_pnl_dollar"], 0.0)
        self.assertEqual(out["total_proceeds"], 0.0)


class TestGetHeldTickers(unittest.TestCase):

    def test_returns_underlying_set(self):
        # Positions with no legs are filtered out (no actual exposure).
        leg = {"is_option": False, "side": "LONG", "entry_price": 100,
               "shares": 100}
        positions = [
            {"underlying": "TSLA", "legs": [leg]},
            {"underlying": "BAC", "legs": [leg]},
            {"underlying": "TSLA", "legs": [leg]},  # duplicate
        ]
        self.assertEqual(get_held_tickers(positions), {"TSLA", "BAC"})

    def test_positions_without_legs_are_excluded(self):
        leg = {"is_option": False, "side": "LONG", "entry_price": 100,
               "shares": 100}
        positions = [
            {"underlying": "TSLA", "legs": [leg]},   # held
            {"underlying": "BAC", "legs": []},       # closed / no exposure
        ]
        self.assertEqual(get_held_tickers(positions), {"TSLA"})

    def test_empty_or_none(self):
        self.assertEqual(get_held_tickers([]), set())


if __name__ == "__main__":
    unittest.main()
