"""Tests for Fischer settlement P&L logic."""

import unittest
from datetime import date, timedelta
from unittest.mock import patch

from tests.conftest import DBTestMixin, seed_fischer_recommendation, seed_price_history
from nenner_engine.fischer_daily_report import settle_expired_trades


class TestCoveredPutSettlement(DBTestMixin, unittest.TestCase):
    """Covered put: short stock + short put."""

    def test_itm_assigned(self):
        """Close < strike → put assigned, cover at strike."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="AAPL", strike=220.0,
            entry_price=225.0, premium_per_share=2.50,
            option_type="P", intent="covered_put",
        )
        seed_price_history(self.conn, "AAPL", close=215.0,
                           price_date=(date.today() - timedelta(days=1)).isoformat())

        results = settle_expired_trades(self.conn)
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertTrue(r["itm"])
        # pnl_ps = (entry - strike) + premium = (225 - 220) + 2.50 = 7.50
        self.assertAlmostEqual(r["pnl_per_share"], 7.50, places=2)
        self.assertAlmostEqual(r["pnl_total"], 750.0, places=2)
        self.assertIn("ITM", r["notes"])

    def test_otm_cover_at_close(self):
        """Close >= strike → put expires worthless, cover short at close."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="AAPL", strike=220.0,
            entry_price=225.0, premium_per_share=2.50,
            option_type="P",
        )
        seed_price_history(self.conn, "AAPL", close=228.0,
                           price_date=(date.today() - timedelta(days=1)).isoformat())

        results = settle_expired_trades(self.conn)
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertFalse(r["itm"])
        # pnl_ps = (entry - close) + premium = (225 - 228) + 2.50 = -0.50
        self.assertAlmostEqual(r["pnl_per_share"], -0.50, places=2)
        self.assertAlmostEqual(r["pnl_total"], -50.0, places=2)
        self.assertIn("OTM", r["notes"])

    def test_at_strike_is_otm(self):
        """Close == strike → not strictly < strike, so OTM."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="AAPL", strike=220.0,
            entry_price=225.0, premium_per_share=2.50,
            option_type="P",
        )
        seed_price_history(self.conn, "AAPL", close=220.0,
                           price_date=(date.today() - timedelta(days=1)).isoformat())

        results = settle_expired_trades(self.conn)
        r = results[0]
        self.assertFalse(r["itm"])
        # pnl_ps = (225 - 220) + 2.50 = 7.50
        self.assertAlmostEqual(r["pnl_per_share"], 7.50, places=2)

    def test_max_profit_itm(self):
        """Deep ITM: profit capped by strike distance + premium."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="TSLA", strike=300.0,
            entry_price=310.0, premium_per_share=5.0,
            option_type="P",
        )
        seed_price_history(self.conn, "TSLA", close=250.0,
                           price_date=(date.today() - timedelta(days=1)).isoformat())

        results = settle_expired_trades(self.conn)
        r = results[0]
        self.assertTrue(r["itm"])
        # pnl_ps = (310 - 300) + 5 = 15.0
        self.assertAlmostEqual(r["pnl_per_share"], 15.0, places=2)


class TestCoveredCallSettlement(DBTestMixin, unittest.TestCase):
    """Covered call: long stock + short call."""

    def test_itm_called_away(self):
        """Close > strike → call assigned, stock called away at strike."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="MSFT", strike=400.0,
            entry_price=390.0, premium_per_share=3.0,
            option_type="C", intent="covered_call",
        )
        seed_price_history(self.conn, "MSFT", close=410.0,
                           price_date=(date.today() - timedelta(days=1)).isoformat())

        results = settle_expired_trades(self.conn)
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertTrue(r["itm"])
        # pnl_ps = (strike - entry) + premium = (400 - 390) + 3 = 13.0
        self.assertAlmostEqual(r["pnl_per_share"], 13.0, places=2)
        self.assertAlmostEqual(r["pnl_total"], 1300.0, places=2)

    def test_otm_keep_stock(self):
        """Close <= strike → call expires, keep stock at close."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="MSFT", strike=400.0,
            entry_price=390.0, premium_per_share=3.0,
            option_type="C", intent="covered_call",
        )
        seed_price_history(self.conn, "MSFT", close=385.0,
                           price_date=(date.today() - timedelta(days=1)).isoformat())

        results = settle_expired_trades(self.conn)
        r = results[0]
        self.assertFalse(r["itm"])
        # pnl_ps = (close - entry) + premium = (385 - 390) + 3 = -2.0
        self.assertAlmostEqual(r["pnl_per_share"], -2.0, places=2)

    def test_at_strike_is_otm(self):
        """Close == strike → not > strike, so OTM for calls."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="MSFT", strike=400.0,
            entry_price=390.0, premium_per_share=3.0,
            option_type="C",
        )
        seed_price_history(self.conn, "MSFT", close=400.0,
                           price_date=(date.today() - timedelta(days=1)).isoformat())

        results = settle_expired_trades(self.conn)
        r = results[0]
        self.assertFalse(r["itm"])
        # pnl_ps = (400 - 390) + 3 = 13.0
        self.assertAlmostEqual(r["pnl_per_share"], 13.0, places=2)


class TestSettlementDBFlow(DBTestMixin, unittest.TestCase):
    """End-to-end: seed → settle → verify DB state."""

    def test_settled_flag_set(self):
        """After settlement, settled=1 and P&L columns populated."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="NVDA", strike=130.0,
            entry_price=135.0, premium_per_share=2.0,
            option_type="P",
        )
        seed_price_history(self.conn, "NVDA", close=125.0,
                           price_date=(date.today() - timedelta(days=1)).isoformat())

        settle_expired_trades(self.conn)

        row = self.conn.execute(
            "SELECT * FROM fischer_recommendations WHERE id = ?", (rec_id,)
        ).fetchone()
        self.assertEqual(row["settled"], 1)
        self.assertIsNotNone(row["pnl_per_share"])
        self.assertIsNotNone(row["pnl_total"])
        self.assertIsNotNone(row["settlement_notes"])
        self.assertEqual(row["close_price_at_expiry"], 125.0)

    def test_already_settled_skipped(self):
        """Trades with settled=1 are not re-settled."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="AAPL", strike=220.0,
            entry_price=225.0, premium_per_share=2.0,
            option_type="P", settled=1,
        )
        seed_price_history(self.conn, "AAPL", close=210.0,
                           price_date=(date.today() - timedelta(days=1)).isoformat())

        results = settle_expired_trades(self.conn)
        self.assertEqual(len(results), 0)

    def test_future_expiry_not_settled(self):
        """Trades with expiry in the future are not settled."""
        future = (date.today() + timedelta(days=3)).isoformat()
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="AAPL", strike=220.0,
            entry_price=225.0, premium_per_share=2.0,
            option_type="P", expiry=future,
        )
        seed_price_history(self.conn, "AAPL", close=210.0,
                           price_date=future)

        results = settle_expired_trades(self.conn)
        self.assertEqual(len(results), 0)

    def test_no_close_price_skipped(self):
        """If no closing price available, trade is skipped (not settled)."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="AAPL", strike=220.0,
            entry_price=225.0, premium_per_share=2.0,
            option_type="P",
        )
        # Don't seed price_history — no close price available
        # Mock _get_closing_price to return None (avoid yfinance call)
        with patch("nenner_engine.fischer_daily_report._get_closing_price", return_value=None):
            results = settle_expired_trades(self.conn)
        self.assertEqual(len(results), 0)

        row = self.conn.execute(
            "SELECT settled FROM fischer_recommendations WHERE id = ?", (rec_id,)
        ).fetchone()
        self.assertEqual(row["settled"], 0)

    def test_pnl_total_is_100x_per_share(self):
        """pnl_total = pnl_per_share × 100."""
        rec_id = seed_fischer_recommendation(
            self.conn, ticker="GOOGL", strike=170.0,
            entry_price=175.0, premium_per_share=1.50,
            option_type="P",
        )
        seed_price_history(self.conn, "GOOGL", close=165.0,
                           price_date=(date.today() - timedelta(days=1)).isoformat())

        results = settle_expired_trades(self.conn)
        r = results[0]
        self.assertAlmostEqual(r["pnl_total"], r["pnl_per_share"] * 100, places=2)


if __name__ == "__main__":
    unittest.main()
