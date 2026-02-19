"""
Test Suite for Nenner Signal Engine
====================================
Tests regex parsing, instrument attribution, signal state machine,
and data integrity.

Run: python -m pytest test_nenner_engine.py -v
  or: python test_nenner_engine.py  (standalone)
"""

import sqlite3
import os
import sys
import unittest

# Import from the engine
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nenner_engine import (
    RE_ACTIVE, RE_CANCELLED, RE_TRIGGER, RE_TARGET, RE_CYCLE, RE_NOTE_CHANGE,
    parse_price, get_section_instrument, identify_instrument,
    parse_email_signals, init_db, migrate_db, compute_current_state,
    store_parsed_results, classify_email,
)


class TestParsePrice(unittest.TestCase):
    """Test price string parsing."""

    def test_simple_integer(self):
        self.assertEqual(parse_price("5000"), 5000.0)

    def test_comma_separated(self):
        self.assertEqual(parse_price("6,950"), 6950.0)

    def test_decimal(self):
        self.assertEqual(parse_price("1.1880"), 1.188)

    def test_large_comma(self):
        self.assertEqual(parse_price("68,280"), 68280.0)

    def test_none(self):
        self.assertIsNone(parse_price(None))

    def test_empty(self):
        self.assertIsNone(parse_price(""))

    def test_trailing_dot(self):
        self.assertEqual(parse_price("54."), 54.0)


class TestRegexActiveSignal(unittest.TestCase):
    """Test Pattern 1 – Active Signal regex."""

    def test_gold_buy(self):
        text = "Continues on a buy signal from 4,380 as long as there is no close below 4,590"
        m = RE_ACTIVE.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "BUY")
        self.assertEqual(parse_price(m.group(2)), 4380.0)
        self.assertEqual(m.group(4).upper(), "BELOW")
        self.assertEqual(parse_price(m.group(5)), 4590.0)

    def test_silver_sell(self):
        text = "Continues on a sell signal from 78 as long as there is no close above 77 (note the change)"
        m = RE_ACTIVE.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "SELL")
        self.assertEqual(parse_price(m.group(2)), 78.0)
        self.assertEqual(m.group(4).upper(), "ABOVE")
        self.assertEqual(parse_price(m.group(5)), 77.0)
        # note the change captured
        self.assertIsNotNone(m.group(6))

    def test_trend_line_syntax(self):
        text = "Continues the buy signal from 52.85 as long as there is no close below the trend line, around 54."
        m = RE_ACTIVE.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "BUY")
        self.assertEqual(parse_price(m.group(2)), 52.85)
        self.assertEqual(parse_price(m.group(5)), 54.0)

    def test_hourly_close(self):
        text = "Continues on a sell signal from 68,280 as long as there is no hourly close above 68,800"
        m = RE_ACTIVE.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "SELL")
        self.assertIsNotNone(m.group(3))  # hourly captured
        self.assertEqual(m.group(4).upper(), "ABOVE")
        self.assertEqual(parse_price(m.group(5)), 68800.0)

    def test_ntc_after_match(self):
        text = "Continues on a sell signal from 425 as long as there is no close above the trend line, around 418 (note the change)"
        m = RE_ACTIVE.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "SELL")

    def test_continues_the_variant(self):
        text = "Continues the sell signal from 54 as long as there is no close above 53.60."
        m = RE_ACTIVE.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "SELL")
        self.assertEqual(parse_price(m.group(2)), 54.0)
        self.assertEqual(parse_price(m.group(5)), 53.6)


class TestRegexCancelledSignal(unittest.TestCase):
    """Test Pattern 2 – Signal Cancelled regex."""

    def test_gold_buy_cancelled(self):
        text = "Cancelled the buy signal from 5,025 again with the close below 5,000."
        m = RE_CANCELLED.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "BUY")
        self.assertEqual(parse_price(m.group(2)), 5025.0)
        self.assertEqual(m.group(4).upper(), "BELOW")
        self.assertEqual(parse_price(m.group(5)), 5000.0)

    def test_gold_sell_cancelled(self):
        text = "Cancelled the sell signal from 5,000 with the close above 5,025."
        m = RE_CANCELLED.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "SELL")
        self.assertEqual(parse_price(m.group(2)), 5000.0)
        self.assertEqual(m.group(4).upper(), "ABOVE")
        self.assertEqual(parse_price(m.group(5)), 5025.0)

    def test_bac_sell_cancelled(self):
        text = "Cancelled the sell signal from 55.65 with the close above 52.85."
        m = RE_CANCELLED.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "SELL")
        self.assertEqual(parse_price(m.group(2)), 55.65)
        self.assertEqual(parse_price(m.group(5)), 52.85)


class TestRegexTrigger(unittest.TestCase):
    """Test the trigger regex (follows cancellation text)."""

    def test_new_buy_trigger(self):
        text = "A close above 5,000 will give a new buy"
        m = RE_TRIGGER.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "ABOVE")
        self.assertEqual(parse_price(m.group(2)), 5000.0)
        self.assertEqual(m.group(3).upper(), "BUY")

    def test_new_sell_trigger(self):
        text = "A close below 5,000 will give a new sell"
        m = RE_TRIGGER.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "BELOW")
        self.assertEqual(parse_price(m.group(2)), 5000.0)
        self.assertEqual(m.group(3).upper(), "SELL")

    def test_resume_variant(self):
        text = "A close above 188 will resume a buy"
        m = RE_TRIGGER.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(3).upper(), "BUY")


class TestRegexPriceTarget(unittest.TestCase):
    """Test Pattern 3 – Price Target regex."""

    def test_downside_target(self):
        text = "There is a downside price target of 4,750"
        m = RE_TARGET.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "DOWNSIDE")
        self.assertEqual(parse_price(m.group(2)), 4750.0)

    def test_upside_with_still(self):
        text = "There is still an upside price target at 5,100"
        m = RE_TARGET.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "UPSIDE")
        self.assertEqual(parse_price(m.group(2)), 5100.0)

    def test_new_downside(self):
        text = "There is a new downside price target of 68"
        m = RE_TARGET.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).upper(), "DOWNSIDE")
        self.assertEqual(parse_price(m.group(2)), 68.0)


class TestRegexCycle(unittest.TestCase):
    """Test Pattern 4 – Cycle Direction regex."""

    def test_daily_up(self):
        text = "The daily cycle is up until end of the week"
        m = RE_CYCLE.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).lower(), "daily")
        self.assertIn("up", m.group(2).lower())

    def test_weekly_down(self):
        text = "The weekly cycle continues down into April"
        m = RE_CYCLE.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).lower(), "weekly")
        self.assertIn("down", m.group(2).lower())

    def test_hourly_bottom(self):
        text = "The hourly cycle projects a bottom for tomorrow midday"
        m = RE_CYCLE.search(text)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1).lower(), "hourly")
        # "a bottom" -> should normalize to UP

    def test_dominant_daily(self):
        text = "The dominant daily cycle is down until next week"
        m = RE_CYCLE.search(text)
        self.assertIsNotNone(m)
        self.assertIn("dominant", m.group(1).lower())


class TestNoteTheChange(unittest.TestCase):
    """Test the (note the change) detection."""

    def test_match(self):
        self.assertIsNotNone(RE_NOTE_CHANGE.search("(note the change)"))

    def test_case_insensitive(self):
        self.assertIsNotNone(RE_NOTE_CHANGE.search("(Note the Change)"))

    def test_no_match(self):
        self.assertIsNone(RE_NOTE_CHANGE.search("some other text"))


class TestInstrumentAttribution(unittest.TestCase):
    """Test section-header-based instrument attribution."""

    def test_gold_section(self):
        text = "Gold (April Futures): Continues on a buy"
        inst, ticker, ac = get_section_instrument(text)
        self.assertEqual(ticker, "GC")
        self.assertEqual(inst, "Gold")

    def test_silver_section(self):
        text = "Silver (March Futures): something here"
        inst, ticker, ac = get_section_instrument(text)
        self.assertEqual(ticker, "SI")

    def test_bac_section(self):
        text = "Bank of America (BAC) Daily chart\nContinues the sell"
        inst, ticker, ac = get_section_instrument(text)
        self.assertEqual(ticker, "BAC")

    def test_vix_section(self):
        text = "CBOE Market Volatility Index (VIX) here"
        inst, ticker, ac = get_section_instrument(text)
        self.assertEqual(ticker, "VIX")

    def test_sp500_section(self):
        text = "S&P (March Futures): continues"
        inst, ticker, ac = get_section_instrument(text)
        self.assertEqual(ticker, "ES")

    def test_nearest_instrument_wins(self):
        """When multiple instruments appear, the nearest (last) one wins."""
        text = "Gold (April Futures): buy signal\n\nSilver (March Futures): something"
        inst, ticker, ac = get_section_instrument(text)
        self.assertEqual(ticker, "SI")  # Silver is nearest to end

    def test_bitcoin_vs_gbtc(self):
        """Bitcoin & GBTC combined header: GBTC matches last (nearest to signal).
        The parser handles this correctly via price-magnitude post-processing:
        prices > 10,000 get reassigned from GBTC to BTC."""
        text = "Bitcoin & GBTC section header"
        inst, ticker, ac = get_section_instrument(text)
        # GBTC appears after Bitcoin in the text, so it wins by position.
        # The price-magnitude fix in parse_email_signals corrects this for signals.
        self.assertEqual(ticker, "GBTC")

    def test_unknown_fallback(self):
        text = "Some random text with no instrument"
        inst, ticker, ac = get_section_instrument(text)
        self.assertEqual(ticker, "UNK")


class TestEmailClassification(unittest.TestCase):
    """Test email type classification."""

    def test_morning_update(self):
        self.assertEqual(classify_email("Morning Update - February 18 #2026-028"),
                        "morning_update")

    def test_intraday(self):
        self.assertEqual(classify_email("Intraday Update - February 18"),
                        "intraday_update")

    def test_stocks_update(self):
        self.assertEqual(classify_email("Stocks Update - February 18 #2026-014"),
                        "stocks_update")

    def test_sunday_cycles(self):
        self.assertEqual(classify_email("Sunday Cycle Charts - February 16"),
                        "sunday_cycles")

    def test_special_report(self):
        self.assertEqual(classify_email("Special Report - Market Analysis"),
                        "special_report")

    def test_other(self):
        self.assertEqual(classify_email("Some Random Subject Line"),
                        "other")


class TestParseEmailSignals(unittest.TestCase):
    """Test full email body parsing with realistic content."""

    def _make_body(self, sections: list[str]) -> str:
        return "\n\n".join(sections)

    def test_gold_active_buy(self):
        body = (
            "Gold (April Futures):\n"
            "Continues on a buy signal from 4,380 as long as there is no close below 4,590 (note the change)\n"
            "There is still an upside price target at 5,100\n"
            "The daily cycle is up until end of the week"
        )
        results = parse_email_signals(body, "2026-01-20", 1)
        self.assertEqual(len(results["signals"]), 1)
        sig = results["signals"][0]
        self.assertEqual(sig["signal_type"], "BUY")
        self.assertEqual(sig["signal_status"], "ACTIVE")
        self.assertEqual(sig["origin_price"], 4380.0)
        self.assertEqual(sig["cancel_level"], 4590.0)
        self.assertEqual(sig["note_the_change"], 1)
        self.assertEqual(sig["ticker"], "GC")

        self.assertEqual(len(results["price_targets"]), 1)
        self.assertEqual(results["price_targets"][0]["target_price"], 5100.0)
        self.assertEqual(results["price_targets"][0]["direction"], "UPSIDE")

        self.assertEqual(len(results["cycles"]), 1)
        self.assertEqual(results["cycles"][0]["direction"], "UP")

    def test_gold_cancelled_with_trigger(self):
        body = (
            "Gold (April Futures):\n"
            "Cancelled the buy signal from 5,025 again with the close below 5,000. "
            "A close above 5,000 will give a new buy signal.\n"
            "There is a downside price target of 4,750"
        )
        results = parse_email_signals(body, "2026-02-18", 1)
        self.assertEqual(len(results["signals"]), 1)
        sig = results["signals"][0]
        self.assertEqual(sig["signal_type"], "BUY")
        self.assertEqual(sig["signal_status"], "CANCELLED")
        self.assertEqual(sig["origin_price"], 5025.0)
        self.assertEqual(sig["cancel_level"], 5000.0)
        self.assertEqual(sig["trigger_direction"], "ABOVE")
        self.assertEqual(sig["trigger_level"], 5000.0)

        self.assertEqual(len(results["price_targets"]), 1)
        self.assertEqual(results["price_targets"][0]["target_price"], 4750.0)

    def test_multiple_instruments(self):
        body = (
            "S&P (March Futures):\n"
            "Continues on a sell signal from 6,950 as long as there is no close above 6,900\n"
            "There is a downside price target of 6,680\n\n"
            "Nasdaq (March Futures):\n"
            "Continues on a sell signal from 25,170 as long as there is no close above 24,960\n"
        )
        results = parse_email_signals(body, "2026-02-18", 1)
        self.assertEqual(len(results["signals"]), 2)
        tickers = {s["ticker"] for s in results["signals"]}
        self.assertEqual(tickers, {"ES", "NQ"})

    def test_bac_sell_with_trendline(self):
        body = (
            "Bank of America (BAC) Daily:\n"
            "Continues the sell signal from 54 as long as there is no close above the trend line, around 53.60.\n"
            "There is a downside price target of 51\n"
        )
        results = parse_email_signals(body, "2026-02-18", 1)
        self.assertEqual(len(results["signals"]), 1)
        sig = results["signals"][0]
        self.assertEqual(sig["ticker"], "BAC")
        self.assertEqual(sig["signal_type"], "SELL")
        self.assertEqual(sig["origin_price"], 54.0)
        self.assertEqual(sig["cancel_level"], 53.6)

    def test_crypto_price_correction(self):
        """Bitcoin signals with prices > 10,000 should be attributed to BTC, not GBTC."""
        body = (
            "Bitcoin & GBTC:\n"
            "Continues on a sell signal from 68,280 as long as there is no hourly close above 68,800\n\n"
            "GBTC - Continues on a sell signal from 73.50 as long as there is no close above 54"
        )
        results = parse_email_signals(body, "2026-02-18", 1)
        # The first signal should be BTC (price > 10,000)
        btc_signals = [s for s in results["signals"] if s["ticker"] == "BTC"]
        gbtc_signals = [s for s in results["signals"] if s["ticker"] == "GBTC"]
        self.assertTrue(len(btc_signals) >= 1)
        self.assertEqual(btc_signals[0]["origin_price"], 68280.0)


class TestSignalStateMachine(unittest.TestCase):
    """Test the current_state computation with cancellation = reversal logic."""

    def setUp(self):
        """Create in-memory database for testing."""
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        # Reuse init_db schema creation by running the SQL directly
        self.conn = init_db(":memory:")
        migrate_db(self.conn)

    def tearDown(self):
        self.conn.close()

    def _insert_email(self, date: str) -> int:
        cur = self.conn.execute(
            "INSERT INTO emails (message_id, subject, date_sent, date_parsed, email_type, raw_text) "
            "VALUES (?, ?, ?, datetime('now'), 'morning_update', 'test')",
            (f"test-{date}", f"Test {date}", date)
        )
        self.conn.commit()
        return cur.lastrowid

    def _insert_signal(self, email_id, date, ticker, instrument, signal_type,
                       signal_status, origin_price, cancel_dir, cancel_level,
                       trigger_dir=None, trigger_level=None):
        self.conn.execute(
            "INSERT INTO signals (email_id, date, instrument, ticker, asset_class, "
            "signal_type, signal_status, origin_price, cancel_direction, cancel_level, "
            "trigger_direction, trigger_level, note_the_change, uses_hourly_close, raw_text) "
            "VALUES (?, ?, ?, ?, 'Test', ?, ?, ?, ?, ?, ?, ?, 0, 0, 'test')",
            (email_id, date, instrument, ticker, signal_type, signal_status,
             origin_price, cancel_dir, cancel_level, trigger_dir, trigger_level)
        )
        self.conn.commit()

    def test_active_buy_stays_buy(self):
        """An ACTIVE BUY signal should result in effective BUY state."""
        eid = self._insert_email("2026-02-18")
        self._insert_signal(eid, "2026-02-18", "GC", "Gold",
                           "BUY", "ACTIVE", 4380.0, "BELOW", 4590.0)
        compute_current_state(self.conn)
        row = self.conn.execute("SELECT * FROM current_state WHERE ticker='GC'").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "BUY")
        self.assertEqual(row["implied_reversal"], 0)
        self.assertEqual(row["origin_price"], 4380.0)
        self.assertEqual(row["cancel_level"], 4590.0)

    def test_cancelled_buy_becomes_sell(self):
        """A CANCELLED BUY should result in effective SELL (implied reversal)."""
        eid = self._insert_email("2026-02-18")
        self._insert_signal(eid, "2026-02-18", "GC", "Gold",
                           "BUY", "CANCELLED", 5025.0, "BELOW", 5000.0,
                           "ABOVE", 5000.0)
        compute_current_state(self.conn)
        row = self.conn.execute("SELECT * FROM current_state WHERE ticker='GC'").fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "SELL")
        self.assertEqual(row["implied_reversal"], 1)
        self.assertEqual(row["origin_price"], 5000.0)  # cancel level becomes origin
        self.assertEqual(row["cancel_level"], 5000.0)   # trigger level becomes cancel

    def test_cancelled_sell_becomes_buy(self):
        """A CANCELLED SELL should result in effective BUY."""
        eid = self._insert_email("2026-02-17")
        self._insert_signal(eid, "2026-02-17", "GC", "Gold",
                           "SELL", "CANCELLED", 5000.0, "ABOVE", 5025.0,
                           "BELOW", 5000.0)
        compute_current_state(self.conn)
        row = self.conn.execute("SELECT * FROM current_state WHERE ticker='GC'").fetchone()
        self.assertEqual(row["effective_signal"], "BUY")
        self.assertEqual(row["implied_reversal"], 1)
        self.assertEqual(row["origin_price"], 5025.0)

    def test_latest_signal_wins(self):
        """When multiple signals exist, the most recent (by date+id) wins."""
        eid1 = self._insert_email("2026-02-17")
        self._insert_signal(eid1, "2026-02-17", "GC", "Gold",
                           "BUY", "ACTIVE", 4900.0, "BELOW", 4850.0)
        eid2 = self._insert_email("2026-02-18")
        self._insert_signal(eid2, "2026-02-18", "GC", "Gold",
                           "BUY", "CANCELLED", 5025.0, "BELOW", 5000.0,
                           "ABOVE", 5000.0)
        compute_current_state(self.conn)
        row = self.conn.execute("SELECT * FROM current_state WHERE ticker='GC'").fetchone()
        # Feb 18 cancelled buy -> implied SELL
        self.assertEqual(row["effective_signal"], "SELL")
        self.assertEqual(row["implied_reversal"], 1)

    def test_bac_full_cycle(self):
        """BAC: SELL cancelled -> BUY active -> BUY cancelled -> SELL active.
        Mimics actual Feb 2026 BAC signal history."""
        # Feb 1: SELL cancelled
        eid1 = self._insert_email("2026-02-01")
        self._insert_signal(eid1, "2026-02-01", "BAC", "Bank of America",
                           "SELL", "CANCELLED", 55.65, "ABOVE", 52.85,
                           "BELOW", 53.60)
        # Feb 3: BUY active (this is what followed in reality)
        eid2 = self._insert_email("2026-02-03")
        self._insert_signal(eid2, "2026-02-03", "BAC", "Bank of America",
                           "BUY", "ACTIVE", 52.85, "BELOW", 53.60)
        # Feb 15: BUY cancelled
        eid3 = self._insert_email("2026-02-15")
        self._insert_signal(eid3, "2026-02-15", "BAC", "Bank of America",
                           "BUY", "CANCELLED", 52.85, "BELOW", 54.0,
                           "ABOVE", 53.60)
        # Feb 18: SELL active (this is what followed in reality)
        eid4 = self._insert_email("2026-02-18")
        self._insert_signal(eid4, "2026-02-18", "BAC", "Bank of America",
                           "SELL", "ACTIVE", 54.0, "ABOVE", 53.60)

        compute_current_state(self.conn)
        row = self.conn.execute("SELECT * FROM current_state WHERE ticker='BAC'").fetchone()
        self.assertEqual(row["effective_signal"], "SELL")
        self.assertEqual(row["implied_reversal"], 0)  # direct, not implied
        self.assertEqual(row["origin_price"], 54.0)
        self.assertEqual(row["cancel_level"], 53.60)

    def test_cancelled_without_trigger(self):
        """Cancellation without a trigger level should still imply reversal."""
        eid = self._insert_email("2026-02-17")
        self._insert_signal(eid, "2026-02-17", "USD/BRL", "Brazil Real",
                           "DIRECTIONAL", "CANCELLED", 5.24, "ABOVE", 5.22,
                           None, None)
        compute_current_state(self.conn)
        row = self.conn.execute("SELECT * FROM current_state WHERE ticker='USD/BRL'").fetchone()
        self.assertIsNotNone(row)
        # Should still show a reversal, even without trigger
        self.assertEqual(row["implied_reversal"], 1)
        self.assertEqual(row["origin_price"], 5.22)
        self.assertIsNone(row["cancel_level"])  # no trigger = no cancel level for implied

    def test_multiple_instruments_independent(self):
        """State machine handles multiple instruments independently."""
        eid = self._insert_email("2026-02-18")
        self._insert_signal(eid, "2026-02-18", "GC", "Gold",
                           "BUY", "ACTIVE", 4900.0, "BELOW", 4850.0)
        self._insert_signal(eid, "2026-02-18", "SI", "Silver",
                           "SELL", "ACTIVE", 78.0, "ABOVE", 77.0)
        compute_current_state(self.conn)

        gold = self.conn.execute("SELECT * FROM current_state WHERE ticker='GC'").fetchone()
        silver = self.conn.execute("SELECT * FROM current_state WHERE ticker='SI'").fetchone()
        self.assertEqual(gold["effective_signal"], "BUY")
        self.assertEqual(silver["effective_signal"], "SELL")

    def test_empty_database(self):
        """compute_current_state handles empty signal table gracefully."""
        compute_current_state(self.conn)
        rows = self.conn.execute("SELECT COUNT(*) FROM current_state").fetchone()
        self.assertEqual(rows[0], 0)


class TestLiveDatabaseValidation(unittest.TestCase):
    """Validate state machine against the live production database.

    These tests run against the real nenner_signals.db and verify that
    the computed state matches the Feb 18, 2026 signals from the
    strategy document. Skip if database doesn't exist.
    """

    @classmethod
    def setUpClass(cls):
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nenner_signals.db")
        if not os.path.exists(db_path):
            raise unittest.SkipTest("Live database not found")
        cls.conn = sqlite3.connect(db_path)
        cls.conn.row_factory = sqlite3.Row
        # Rebuild state
        compute_current_state(cls.conn)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'conn'):
            cls.conn.close()

    def _get_state(self, ticker: str):
        return self.conn.execute(
            "SELECT * FROM current_state WHERE ticker = ?", (ticker,)
        ).fetchone()

    def test_gold_sell_implied(self):
        row = self._get_state("GC")
        self.assertIsNotNone(row, "Gold not found in current_state")
        self.assertEqual(row["effective_signal"], "SELL")
        self.assertEqual(row["implied_reversal"], 1)
        self.assertEqual(row["origin_price"], 5000.0)

    def test_silver_sell_direct(self):
        row = self._get_state("SI")
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "SELL")
        self.assertEqual(row["cancel_level"], 77.0)

    def test_tsla_sell(self):
        row = self._get_state("TSLA")
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "SELL")
        self.assertEqual(row["origin_price"], 425.0)
        self.assertEqual(row["cancel_level"], 418.0)

    def test_msft_sell(self):
        row = self._get_state("MSFT")
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "SELL")
        self.assertEqual(row["cancel_level"], 409.0)

    def test_bac_sell(self):
        row = self._get_state("BAC")
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "SELL")
        self.assertEqual(row["origin_price"], 54.0)
        self.assertEqual(row["cancel_level"], 53.6)

    def test_sp500_sell(self):
        row = self._get_state("ES")
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "SELL")
        self.assertEqual(row["cancel_level"], 6900.0)

    def test_vix_buy(self):
        row = self._get_state("VIX")
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "BUY")
        self.assertEqual(row["cancel_level"], 19.3)

    def test_bonds_buy(self):
        row = self._get_state("ZB")
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "BUY")
        self.assertEqual(row["cancel_level"], 117.2)

    def test_dollar_buy(self):
        row = self._get_state("DXY")
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "BUY")

    def test_bitcoin_sell(self):
        row = self._get_state("BTC")
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "SELL")
        self.assertEqual(row["cancel_level"], 68800.0)

    def test_nem_outlier_buy(self):
        """NEM is the outlier — still on buy while rest of gold complex is sell."""
        row = self._get_state("NEM")
        self.assertIsNotNone(row)
        self.assertEqual(row["effective_signal"], "BUY")
        self.assertEqual(row["cancel_level"], 121.0)

    def test_database_stats(self):
        """Verify database has expected volume of data."""
        email_count = self.conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
        signal_count = self.conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        self.assertGreater(email_count, 2000)
        self.assertGreater(signal_count, 10000)


if __name__ == "__main__":
    unittest.main(verbosity=2)
