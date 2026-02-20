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
from nenner_engine.alerts import (
    evaluate_price_alerts,
    detect_signal_changes,
    is_cooled_down,
    log_alert,
    show_alert_history,
    PROXIMITY_DANGER_PCT,
    PROXIMITY_WARNING_PCT,
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


class TestAlertEngine(unittest.TestCase):
    """Test alert condition evaluation, cooldown, and persistence."""

    def setUp(self):
        self.conn = init_db(":memory:")
        migrate_db(self.conn)

    def tearDown(self):
        self.conn.close()

    def _make_row(self, ticker="GC", instrument="Gold", price=5000.0,
                  cancel_dist_pct=None, trigger_dist_pct=None,
                  effective_signal="BUY", cancel_level=None,
                  trigger_level=None, origin_price=None):
        return {
            "ticker": ticker, "instrument": instrument,
            "price": price, "effective_signal": effective_signal,
            "cancel_dist_pct": cancel_dist_pct,
            "trigger_dist_pct": trigger_dist_pct,
            "cancel_level": cancel_level,
            "trigger_level": trigger_level,
            "origin_price": origin_price,
        }

    def test_cancel_danger_alert(self):
        """Cancel distance below DANGER threshold fires CANCEL_DANGER."""
        rows = [self._make_row(cancel_dist_pct=0.3, cancel_level=5015)]
        alerts = evaluate_price_alerts(rows)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["alert_type"], "CANCEL_DANGER")
        self.assertEqual(alerts[0]["severity"], "DANGER")
        self.assertEqual(alerts[0]["ticker"], "GC")

    def test_cancel_watch_alert(self):
        """Cancel distance between DANGER and WARNING fires CANCEL_WATCH."""
        rows = [self._make_row(cancel_dist_pct=0.7, cancel_level=5035)]
        alerts = evaluate_price_alerts(rows)
        cancel_alerts = [a for a in alerts if a["alert_type"] == "CANCEL_WATCH"]
        self.assertEqual(len(cancel_alerts), 1)
        self.assertEqual(cancel_alerts[0]["severity"], "WARNING")

    def test_cancel_no_alert_far(self):
        """Cancel distance above WARNING threshold produces no cancel alert."""
        rows = [self._make_row(cancel_dist_pct=3.0, cancel_level=5150)]
        alerts = evaluate_price_alerts(rows)
        cancel_alerts = [a for a in alerts if "CANCEL" in a["alert_type"]]
        self.assertEqual(len(cancel_alerts), 0)

    def test_trigger_danger_alert(self):
        """Trigger distance below DANGER threshold fires TRIGGER_DANGER."""
        rows = [self._make_row(trigger_dist_pct=0.2, trigger_level=5010)]
        alerts = evaluate_price_alerts(rows)
        trigger_alerts = [a for a in alerts if a["alert_type"] == "TRIGGER_DANGER"]
        self.assertEqual(len(trigger_alerts), 1)
        self.assertEqual(trigger_alerts[0]["severity"], "DANGER")

    def test_trigger_watch_alert(self):
        """Trigger distance between DANGER and WARNING fires TRIGGER_WATCH."""
        rows = [self._make_row(trigger_dist_pct=0.8, trigger_level=5040)]
        alerts = evaluate_price_alerts(rows)
        trigger_alerts = [a for a in alerts if a["alert_type"] == "TRIGGER_WATCH"]
        self.assertEqual(len(trigger_alerts), 1)

    def test_no_price_skipped(self):
        """Rows with price=None produce no alerts."""
        rows = [self._make_row(price=None, cancel_dist_pct=0.1)]
        alerts = evaluate_price_alerts(rows)
        self.assertEqual(len(alerts), 0)

    def test_multiple_alerts_same_row(self):
        """Row near both cancel and trigger produces two alerts."""
        rows = [self._make_row(
            cancel_dist_pct=0.3, cancel_level=5015,
            trigger_dist_pct=0.4, trigger_level=5020,
        )]
        alerts = evaluate_price_alerts(rows)
        types = {a["alert_type"] for a in alerts}
        self.assertIn("CANCEL_DANGER", types)
        self.assertIn("TRIGGER_DANGER", types)
        self.assertEqual(len(alerts), 2)

    def test_signal_change_detection(self):
        """New signals after baseline id should be detected."""
        self.conn.execute(
            "INSERT INTO emails (message_id, subject, date_sent, date_parsed, "
            "email_type, raw_text) "
            "VALUES ('test-1', 'Test', '2026-02-18', datetime('now'), "
            "'morning_update', 'test')"
        )
        self.conn.commit()
        baseline_id = 0

        self.conn.execute(
            "INSERT INTO signals (email_id, date, instrument, ticker, "
            "asset_class, signal_type, signal_status, origin_price, "
            "cancel_direction, cancel_level, note_the_change, "
            "uses_hourly_close, raw_text) "
            "VALUES (1, '2026-02-18', 'Gold', 'GC', 'Precious Metals', "
            "'BUY', 'ACTIVE', 5000.0, 'BELOW', 4950.0, 0, 0, 'test')"
        )
        self.conn.commit()

        alerts, new_max_id = detect_signal_changes(self.conn, baseline_id)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["ticker"], "GC")
        self.assertEqual(alerts[0]["alert_type"], "SIGNAL_CHANGE")
        self.assertGreater(new_max_id, baseline_id)

        # Second call with new max_id should find nothing
        alerts2, same_id = detect_signal_changes(self.conn, new_max_id)
        self.assertEqual(len(alerts2), 0)
        self.assertEqual(same_id, new_max_id)

    def test_cooldown_blocks_repeat(self):
        """Alert within cooldown period should be suppressed."""
        from datetime import datetime
        tracker = {("GC", "CANCEL_DANGER"): datetime.now()}
        self.assertFalse(is_cooled_down(tracker, "GC", "CANCEL_DANGER"))

    def test_cooldown_expires(self):
        """Alert after cooldown period should fire."""
        from datetime import datetime, timedelta
        tracker = {("GC", "CANCEL_DANGER"): datetime.now() - timedelta(minutes=61)}
        self.assertTrue(is_cooled_down(tracker, "GC", "CANCEL_DANGER"))

    def test_cooldown_first_time(self):
        """First alert for a ticker/type should always fire."""
        self.assertTrue(is_cooled_down({}, "GC", "CANCEL_DANGER"))

    def test_alert_log_persistence(self):
        """log_alert writes to DB and can be retrieved."""
        alert = {
            "ticker": "GC", "instrument": "Gold",
            "alert_type": "CANCEL_DANGER", "severity": "DANGER",
            "message": "Test alert", "current_price": 5000.0,
            "cancel_dist_pct": 0.3, "trigger_dist_pct": None,
            "effective_signal": "BUY",
        }
        log_alert(self.conn, alert, ["toast", "telegram"])

        rows = self.conn.execute(
            "SELECT * FROM alert_log WHERE ticker = 'GC'"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["alert_type"], "CANCEL_DANGER")
        self.assertEqual(rows[0]["channels_sent"], "toast,telegram")

    def test_show_alert_history_empty(self):
        """show_alert_history on empty DB prints 'No alerts' message."""
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            show_alert_history(self.conn)
        self.assertIn("No alerts recorded yet", buf.getvalue())


# ========================================================================
# Position Tracker Tests
# ========================================================================

from nenner_engine.positions import (
    parse_option_code,
    compute_position_pnl,
    get_held_tickers,
)


class TestOptionCodeParser(unittest.TestCase):
    """Tests for the option code regex parser."""

    def test_tsla_put_near(self):
        result = parse_option_code("TSLA2620N410")
        self.assertEqual(result["underlying"], "TSLA")
        self.assertEqual(result["option_type"], "PUT")
        self.assertEqual(result["strike"], 410.0)
        self.assertEqual(result["type_code"], "N")
        self.assertEqual(result["year"], 2026)
        self.assertEqual(result["day"], 20)

    def test_tsla_put_decimal_strike(self):
        result = parse_option_code("TSLA2620N407.5")
        self.assertEqual(result["underlying"], "TSLA")
        self.assertEqual(result["strike"], 407.5)
        self.assertEqual(result["option_type"], "PUT")

    def test_bac_put_near(self):
        result = parse_option_code("BAC2620N51")
        self.assertEqual(result["underlying"], "BAC")
        self.assertEqual(result["strike"], 51.0)
        self.assertEqual(result["option_type"], "PUT")

    def test_bac_put_half_strike(self):
        result = parse_option_code("BAC2620N50.5")
        self.assertEqual(result["strike"], 50.5)

    def test_call_near(self):
        result = parse_option_code("QQQ2620B610")
        self.assertEqual(result["underlying"], "QQQ")
        self.assertEqual(result["option_type"], "CALL")
        self.assertEqual(result["strike"], 610.0)
        self.assertEqual(result["type_code"], "B")

    def test_put_far(self):
        result = parse_option_code("SIL2715M85")
        self.assertEqual(result["underlying"], "SIL")
        self.assertEqual(result["option_type"], "PUT")
        self.assertEqual(result["strike"], 85.0)
        self.assertEqual(result["type_code"], "M")
        self.assertEqual(result["year"], 2027)

    def test_call_far(self):
        result = parse_option_code("SIL2715A100")
        self.assertEqual(result["option_type"], "CALL")
        self.assertEqual(result["type_code"], "A")

    def test_plain_ticker_returns_none(self):
        self.assertIsNone(parse_option_code("TSLA"))
        self.assertIsNone(parse_option_code("BAC"))
        self.assertIsNone(parse_option_code("QQQ"))
        self.assertIsNone(parse_option_code("SIL"))

    def test_empty_string_returns_none(self):
        self.assertIsNone(parse_option_code(""))
        self.assertIsNone(parse_option_code(None))


class TestPositionPnL(unittest.TestCase):
    """Tests for dollar P/L calculation."""

    def test_covered_put_stock_drops_profit(self):
        """Short stock at 410.75, stock drops to 400 -> $21,500 stock profit."""
        position = {
            "legs": [
                {"side": "SHORT", "ticker": "TSLA", "shares": 2000,
                 "entry_price": 410.75, "proceeds": 821500.0,
                 "is_option": False, "option_type": None, "strike": None},
            ],
        }
        result = compute_position_pnl(position, current_price=400.0)
        self.assertAlmostEqual(result["stock_pnl_dollar"], 21500.0)
        self.assertEqual(result["option_pnl_dollar"], 0.0)
        self.assertAlmostEqual(result["total_pnl_dollar"], 21500.0)

    def test_covered_put_stock_rises_loss(self):
        """Short stock at 410.75, stock rises to 420 -> $-18,500 stock loss."""
        position = {
            "legs": [
                {"side": "SHORT", "ticker": "TSLA", "shares": 2000,
                 "entry_price": 410.75, "proceeds": 821500.0,
                 "is_option": False, "option_type": None, "strike": None},
            ],
        }
        result = compute_position_pnl(position, current_price=420.0)
        self.assertAlmostEqual(result["stock_pnl_dollar"], -18500.0)

    def test_covered_call_long_stock_profit(self):
        """Long stock at 604.20, stock rises to 610 -> $14,500 profit."""
        position = {
            "legs": [
                {"side": "LONG", "ticker": "QQQ", "shares": 2500,
                 "entry_price": 604.20, "proceeds": -1510500.0,
                 "is_option": False, "option_type": None, "strike": None},
            ],
        }
        result = compute_position_pnl(position, current_price=610.0)
        self.assertAlmostEqual(result["stock_pnl_dollar"], 14500.0)

    def test_short_put_otm_full_profit(self):
        """Short put at 410 strike, sold for 4.26, stock at 415 (OTM) -> full premium profit."""
        position = {
            "legs": [
                {"side": "SHORT", "ticker": "TSLA2620N410", "shares": 2000,
                 "entry_price": 4.26, "proceeds": 8520.0,
                 "is_option": True, "option_type": "PUT", "strike": 410.0},
            ],
        }
        result = compute_position_pnl(position, current_price=415.0)
        # OTM: intrinsic=0, profit = (4.26 - 0) * 2000 = 8520
        self.assertAlmostEqual(result["option_pnl_dollar"], 8520.0)

    def test_short_put_itm_partial_loss(self):
        """Short put at 410, sold for 4.26, stock at 400 -> losing money."""
        position = {
            "legs": [
                {"side": "SHORT", "ticker": "TSLA2620N410", "shares": 2000,
                 "entry_price": 4.26, "proceeds": 8520.0,
                 "is_option": True, "option_type": "PUT", "strike": 410.0},
            ],
        }
        result = compute_position_pnl(position, current_price=400.0)
        # ITM: intrinsic = 410-400 = 10, P/L = (4.26 - 10) * 2000 = -11480
        self.assertAlmostEqual(result["option_pnl_dollar"], -11480.0)

    def test_long_call_itm(self):
        """Long call at 85 strike, paid 25.8, stock at 110 -> profit."""
        position = {
            "legs": [
                {"side": "LONG", "ticker": None, "shares": 2500,
                 "entry_price": 25.8, "proceeds": -64500.0,
                 "is_option": True, "option_type": "CALL", "strike": 85.0},
            ],
        }
        result = compute_position_pnl(position, current_price=110.0)
        # intrinsic = 110-85 = 25, P/L = (25 - 25.8) * 2500 = -2000
        self.assertAlmostEqual(result["option_pnl_dollar"], -2000.0)

    def test_combined_covered_put(self):
        """Short stock + short put, stock drops -> net profit."""
        position = {
            "legs": [
                {"side": "SHORT", "ticker": "TSLA", "shares": 2000,
                 "entry_price": 410.75, "proceeds": 821500.0,
                 "is_option": False, "option_type": None, "strike": None},
                {"side": "SHORT", "ticker": "TSLA2620N410", "shares": 2000,
                 "entry_price": 4.26, "proceeds": 8520.0,
                 "is_option": True, "option_type": "PUT", "strike": 410.0},
            ],
        }
        result = compute_position_pnl(position, current_price=400.0)
        # Stock: (410.75-400)*2000 = 21500
        # Put ITM: (4.26 - 10)*2000 = -11480
        # Total: 21500 - 11480 = 10020
        self.assertAlmostEqual(result["total_pnl_dollar"], 10020.0)

    def test_empty_legs(self):
        position = {"legs": []}
        result = compute_position_pnl(position, current_price=400.0)
        self.assertEqual(result["total_pnl_dollar"], 0.0)
        self.assertEqual(result["stock_pnl_dollar"], 0.0)
        self.assertEqual(result["option_pnl_dollar"], 0.0)


class TestGetHeldTickers(unittest.TestCase):
    """Tests for get_held_tickers."""

    def test_returns_underlying_tickers(self):
        positions = [
            {"underlying": "TSLA", "legs": [{"side": "SHORT"}]},
            {"underlying": "BAC", "legs": [{"side": "SHORT"}]},
        ]
        result = get_held_tickers(positions)
        self.assertEqual(result, {"TSLA", "BAC"})

    def test_empty_positions(self):
        self.assertEqual(get_held_tickers([]), set())

    def test_skips_empty_legs(self):
        positions = [
            {"underlying": "TSLA", "legs": [{"side": "SHORT"}]},
            {"underlying": "QQQ", "legs": []},
        ]
        result = get_held_tickers(positions)
        self.assertEqual(result, {"TSLA"})


class TestPositionSignalContext(unittest.TestCase):
    """Tests for position-signal linking."""

    def setUp(self):
        self.conn = init_db(":memory:")
        migrate_db(self.conn)
        # Insert a TSLA SELL signal via current_state directly
        self.conn.execute("""
            INSERT INTO current_state
            (ticker, instrument, asset_class, effective_signal, origin_price,
             cancel_direction, cancel_level, trigger_level, implied_reversal,
             last_signal_date)
            VALUES ('TSLA', 'Tesla', 'Single Stock', 'SELL', 425.0,
                    'above', 450.0, 400.0, 0, '2026-02-15')
        """)
        self.conn.commit()

    def test_held_ticker_gets_signal(self):
        from nenner_engine.positions import get_positions_with_signal_context
        positions = [{
            "sheet_name": "TradeSheet PUTS",
            "strategy": "covered_put",
            "underlying": "TSLA",
            "underlying_bid": 411.0,
            "near_expiry": "2026-02-20",
            "legs": [
                {"side": "SHORT", "ticker": "TSLA", "shares": 2000,
                 "entry_price": 410.75, "proceeds": 821500.0,
                 "is_option": False, "option_type": None, "strike": None},
            ],
        }]
        enriched = get_positions_with_signal_context(
            self.conn, positions, try_t1=False,
        )
        self.assertEqual(len(enriched), 1)
        self.assertEqual(enriched[0]["nenner_signal"], "SELL")
        self.assertEqual(enriched[0]["cancel_level"], 450.0)
        self.assertIsNotNone(enriched[0]["total_pnl_dollar"])

    def test_unknown_ticker_no_signal(self):
        from nenner_engine.positions import get_positions_with_signal_context
        positions = [{
            "sheet_name": "Put_Call Trade",
            "strategy": "collar",
            "underlying": "XYZ",
            "underlying_bid": 100.0,
            "legs": [
                {"side": "SHORT", "ticker": "XYZ", "shares": 1000,
                 "entry_price": 95.0, "proceeds": 95000.0,
                 "is_option": False, "option_type": None, "strike": None},
            ],
        }]
        enriched = get_positions_with_signal_context(
            self.conn, positions, try_t1=False,
        )
        self.assertEqual(len(enriched), 1)
        self.assertIsNone(enriched[0]["nenner_signal"])

    def tearDown(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# LLM Parser Tests (mocked API)
# ---------------------------------------------------------------------------

from unittest.mock import patch, MagicMock
from nenner_engine.llm_parser import (
    parse_email_signals_llm,
    _validate_signal,
    _validate_cycle,
    _validate_target,
    _apply_crypto_fix,
    _validate_ticker,
)


class TestLLMParserValidation(unittest.TestCase):
    """Test LLM parser validation and post-processing (no API calls)."""

    def test_validate_signal_complete(self):
        sig = {
            "instrument": "Gold",
            "ticker": "GC",
            "asset_class": "Precious Metals",
            "signal_type": "BUY",
            "signal_status": "ACTIVE",
            "origin_price": 2900.0,
            "cancel_direction": "BELOW",
            "cancel_level": 2850.0,
            "trigger_direction": None,
            "trigger_level": None,
            "price_target": None,
            "target_direction": None,
            "note_the_change": 0,
            "uses_hourly_close": 0,
            "raw_text": "Continues on a buy signal from 2900",
        }
        result = _validate_signal(sig)
        self.assertEqual(result["signal_type"], "BUY")
        self.assertEqual(result["origin_price"], 2900.0)
        self.assertEqual(result["cancel_level"], 2850.0)
        self.assertEqual(result["note_the_change"], 0)

    def test_validate_signal_missing_fields(self):
        """Missing fields get defaults."""
        sig = {"instrument": "Gold", "ticker": "GC", "signal_type": "sell"}
        result = _validate_signal(sig)
        self.assertEqual(result["signal_type"], "SELL")
        self.assertEqual(result["signal_status"], "ACTIVE")
        self.assertIsNone(result["origin_price"])
        self.assertEqual(result["note_the_change"], 0)

    def test_validate_signal_type_normalization(self):
        sig = {"signal_type": "buy", "signal_status": "active"}
        result = _validate_signal(sig)
        self.assertEqual(result["signal_type"], "BUY")
        self.assertEqual(result["signal_status"], "ACTIVE")

    def test_validate_cycle(self):
        cyc = {
            "instrument": "Gold",
            "ticker": "GC",
            "timeframe": "weekly",
            "direction": "up",
            "until_description": "next week",
            "raw_text": "The weekly cycle is up until next week",
        }
        result = _validate_cycle(cyc)
        self.assertEqual(result["direction"], "UP")
        self.assertEqual(result["timeframe"], "weekly")

    def test_validate_target(self):
        tgt = {
            "instrument": "Tesla",
            "ticker": "TSLA",
            "target_price": 375.0,
            "direction": "downside",
            "condition": "stays on sell signal",
            "raw_text": "There is still a downside price target at 375",
        }
        result = _validate_target(tgt)
        self.assertEqual(result["direction"], "DOWNSIDE")
        self.assertEqual(result["target_price"], 375.0)

    def test_crypto_fix_gbtc_to_btc(self):
        signals = [{"ticker": "GBTC", "instrument": "GBTC",
                     "asset_class": "Crypto ETF", "origin_price": 95000.0}]
        _apply_crypto_fix(signals)
        self.assertEqual(signals[0]["ticker"], "BTC")
        self.assertEqual(signals[0]["instrument"], "Bitcoin")

    def test_crypto_fix_ethe_to_eth(self):
        signals = [{"ticker": "ETHE", "instrument": "ETHE",
                     "asset_class": "Crypto ETF", "origin_price": 3500.0}]
        _apply_crypto_fix(signals)
        self.assertEqual(signals[0]["ticker"], "ETH")

    def test_crypto_fix_no_change_low_price(self):
        signals = [{"ticker": "GBTC", "instrument": "GBTC",
                     "asset_class": "Crypto ETF", "origin_price": 50.0}]
        _apply_crypto_fix(signals)
        self.assertEqual(signals[0]["ticker"], "GBTC")

    def test_validate_ticker_known(self):
        self.assertTrue(_validate_ticker({"ticker": "GC"}))
        self.assertTrue(_validate_ticker({"ticker": "TSLA"}))
        self.assertTrue(_validate_ticker({"ticker": "BTC"}))

    def test_validate_ticker_unknown(self):
        self.assertFalse(_validate_ticker({"ticker": "FAKE"}))
        self.assertFalse(_validate_ticker({"ticker": "UNK"}))


class TestLLMParserMocked(unittest.TestCase):
    """Test LLM parser with mocked _call_llm (bypasses Anthropic API entirely)."""

    @patch("nenner_engine.llm_parser._call_llm")
    def test_gold_active_buy(self, mock_call):
        mock_call.return_value = {
            "signals": [{
                "instrument": "Gold",
                "ticker": "GC",
                "asset_class": "Precious Metals",
                "signal_type": "BUY",
                "signal_status": "ACTIVE",
                "origin_price": 2900.0,
                "cancel_direction": "BELOW",
                "cancel_level": 2850.0,
                "trigger_direction": None,
                "trigger_level": None,
                "price_target": None,
                "target_direction": None,
                "note_the_change": 0,
                "uses_hourly_close": 0,
                "raw_text": "Continues on a buy signal from 2900"
            }],
            "cycles": [],
            "price_targets": [],
        }

        result = parse_email_signals_llm("Continues on a buy signal from 2900 as long as there is no close below 2850. Test email body.",
                                          "2026-02-18", 1, api_key="test-key")
        self.assertEqual(len(result["signals"]), 1)
        sig = result["signals"][0]
        self.assertEqual(sig["signal_type"], "BUY")
        self.assertEqual(sig["ticker"], "GC")
        self.assertEqual(sig["email_id"], 1)
        self.assertEqual(sig["date"], "2026-02-18")

    @patch("nenner_engine.llm_parser._call_llm")
    def test_cancelled_with_trigger(self, mock_call):
        mock_call.return_value = {
            "signals": [{
                "instrument": "S&P",
                "ticker": "ES",
                "asset_class": "Equity Index",
                "signal_type": "BUY",
                "signal_status": "CANCELLED",
                "origin_price": 6000.0,
                "cancel_direction": "BELOW",
                "cancel_level": 5950.0,
                "trigger_direction": "ABOVE",
                "trigger_level": 6050.0,
                "price_target": None,
                "target_direction": None,
                "note_the_change": 0,
                "uses_hourly_close": 0,
                "raw_text": "Cancelled the buy signal"
            }],
            "cycles": [],
            "price_targets": [],
        }

        result = parse_email_signals_llm("Continues on a buy signal from 2900 as long as there is no close below 2850. Test email body.",
                                          "2026-02-18", 2, api_key="test-key")
        sig = result["signals"][0]
        self.assertEqual(sig["signal_status"], "CANCELLED")
        self.assertEqual(sig["trigger_direction"], "ABOVE")
        self.assertEqual(sig["trigger_level"], 6050.0)

    @patch("nenner_engine.llm_parser._call_llm")
    def test_multiple_instruments(self, mock_call):
        mock_call.return_value = {
            "signals": [
                {"instrument": "Gold", "ticker": "GC",
                 "asset_class": "Precious Metals",
                 "signal_type": "BUY", "signal_status": "ACTIVE",
                 "origin_price": 2900.0, "cancel_direction": "BELOW",
                 "cancel_level": 2850.0, "raw_text": "gold buy"},
                {"instrument": "Silver", "ticker": "SI",
                 "asset_class": "Precious Metals",
                 "signal_type": "SELL", "signal_status": "ACTIVE",
                 "origin_price": 33.0, "cancel_direction": "ABOVE",
                 "cancel_level": 34.0, "raw_text": "silver sell"},
            ],
            "cycles": [],
            "price_targets": [],
        }

        result = parse_email_signals_llm("Continues on a buy signal from 2900 as long as there is no close below 2850. Test email body.",
                                          "2026-02-18", 3, api_key="test-key")
        self.assertEqual(len(result["signals"]), 2)
        tickers = {s["ticker"] for s in result["signals"]}
        self.assertEqual(tickers, {"GC", "SI"})

    @patch("nenner_engine.llm_parser._call_llm")
    def test_unknown_ticker_filtered(self, mock_call):
        mock_call.return_value = {
            "signals": [
                {"instrument": "FakeInstrument", "ticker": "FAKE",
                 "asset_class": "Unknown",
                 "signal_type": "BUY", "signal_status": "ACTIVE",
                 "origin_price": 100.0, "cancel_direction": "BELOW",
                 "cancel_level": 95.0, "raw_text": "fake"},
            ],
            "cycles": [],
            "price_targets": [],
        }

        result = parse_email_signals_llm("Continues on a buy signal from 2900 as long as there is no close below 2850. Test email body.",
                                          "2026-02-18", 4, api_key="test-key")
        self.assertEqual(len(result["signals"]), 0)

    def test_empty_body(self):
        result = parse_email_signals_llm("", "2026-02-18", 5, api_key="test-key")
        self.assertEqual(result, {"signals": [], "cycles": [], "price_targets": []})

    def test_short_body(self):
        result = parse_email_signals_llm("hi", "2026-02-18", 6, api_key="test-key")
        self.assertEqual(result, {"signals": [], "cycles": [], "price_targets": []})

    @patch("nenner_engine.llm_parser._call_llm")
    def test_api_error_returns_empty(self, mock_call):
        mock_call.return_value = {"signals": [], "cycles": [], "price_targets": []}

        result = parse_email_signals_llm(
            "A real email body with enough text to pass the length check here.",
            "2026-02-18", 7, api_key="test-key"
        )
        self.assertEqual(result["signals"], [])
        self.assertEqual(result["cycles"], [])

    @patch("nenner_engine.llm_parser._call_llm")
    def test_malformed_response_returns_empty(self, mock_call):
        # _call_llm returns empty on JSON parse failure
        mock_call.return_value = {"signals": [], "cycles": [], "price_targets": []}

        result = parse_email_signals_llm(
            "A real email body with enough text to pass the length check here.",
            "2026-02-18", 8, api_key="test-key"
        )
        self.assertEqual(result["signals"], [])


# ---------------------------------------------------------------------------
# Auto-Cancel Tests (in-memory database)
# ---------------------------------------------------------------------------

import json
from nenner_engine.auto_cancel import check_auto_cancellations
from nenner_engine.db import store_email, store_parsed_results


class TestAutoCancel(unittest.TestCase):
    """Test automatic cancellation detection using in-memory SQLite."""

    def setUp(self):
        self.conn = init_db(":memory:")
        migrate_db(self.conn)

    def _insert_signal(self, ticker, instrument, signal_type, signal_status,
                       origin_price, cancel_direction, cancel_level,
                       uses_hourly_close=0):
        """Helper to insert a signal and rebuild state."""
        email_id = store_email(
            self.conn, f"test-{ticker}-{signal_type}",
            f"Test {ticker}", "2026-02-17", "morning_update", "test"
        )
        results = {
            "signals": [{
                "email_id": email_id,
                "date": "2026-02-17",
                "instrument": instrument,
                "ticker": ticker,
                "asset_class": "Test",
                "signal_type": signal_type,
                "signal_status": signal_status,
                "origin_price": origin_price,
                "cancel_direction": cancel_direction,
                "cancel_level": cancel_level,
                "trigger_direction": None,
                "trigger_level": None,
                "price_target": None,
                "target_direction": None,
                "note_the_change": 0,
                "uses_hourly_close": uses_hourly_close,
                "raw_text": "test signal",
            }],
            "cycles": [],
            "price_targets": [],
        }
        store_parsed_results(self.conn, results, email_id)

    def _insert_price(self, ticker, date, close_price):
        """Helper to insert a price record."""
        self.conn.execute(
            "INSERT OR REPLACE INTO price_history (ticker, date, close, source) "
            "VALUES (?, ?, ?, 'test')",
            (ticker, date, close_price)
        )
        self.conn.commit()

    @patch("nenner_engine.prices.fetch_yfinance_daily")
    def test_cancel_above_breached(self, mock_fetch):
        """SELL signal cancelled when close > cancel level (ABOVE)."""
        self._insert_signal("TSLA", "Tesla", "SELL", "ACTIVE",
                            425.0, "ABOVE", 418.0)
        self._insert_price("TSLA", "2026-02-18", 420.0)

        cancellations = check_auto_cancellations(self.conn, "2026-02-18")
        self.assertEqual(len(cancellations), 1)
        self.assertEqual(cancellations[0]["ticker"], "TSLA")
        self.assertEqual(cancellations[0]["old_signal"], "SELL")
        self.assertEqual(cancellations[0]["new_signal"], "BUY")

        # Verify state was rebuilt
        row = self.conn.execute(
            "SELECT effective_signal, implied_reversal FROM current_state WHERE ticker = 'TSLA'"
        ).fetchone()
        self.assertEqual(row["effective_signal"], "BUY")
        self.assertEqual(row["implied_reversal"], 1)

    @patch("nenner_engine.prices.fetch_yfinance_daily")
    def test_cancel_below_breached(self, mock_fetch):
        """BUY signal cancelled when close < cancel level (BELOW)."""
        self._insert_signal("GC", "Gold", "BUY", "ACTIVE",
                            2900.0, "BELOW", 2850.0)
        self._insert_price("GC", "2026-02-18", 2840.0)

        cancellations = check_auto_cancellations(self.conn, "2026-02-18")
        self.assertEqual(len(cancellations), 1)
        self.assertEqual(cancellations[0]["old_signal"], "BUY")
        self.assertEqual(cancellations[0]["new_signal"], "SELL")

    @patch("nenner_engine.prices.fetch_yfinance_daily")
    def test_cancel_not_breached(self, mock_fetch):
        """Signal NOT cancelled when close doesn't breach cancel level."""
        self._insert_signal("TSLA", "Tesla", "SELL", "ACTIVE",
                            425.0, "ABOVE", 418.0)
        self._insert_price("TSLA", "2026-02-18", 410.0)

        cancellations = check_auto_cancellations(self.conn, "2026-02-18")
        self.assertEqual(len(cancellations), 0)

    @patch("nenner_engine.prices.fetch_yfinance_daily")
    def test_cancel_exact_level_not_breached(self, mock_fetch):
        """Close exactly at cancel level should NOT trigger (strict inequality)."""
        self._insert_signal("TSLA", "Tesla", "SELL", "ACTIVE",
                            425.0, "ABOVE", 418.0)
        self._insert_price("TSLA", "2026-02-18", 418.0)

        cancellations = check_auto_cancellations(self.conn, "2026-02-18")
        self.assertEqual(len(cancellations), 0)

    @patch("nenner_engine.prices.fetch_yfinance_daily")
    def test_hourly_close_skipped(self, mock_fetch):
        """Instruments with uses_hourly_close=1 are skipped."""
        self._insert_signal("ES", "S&P", "BUY", "ACTIVE",
                            6000.0, "BELOW", 5950.0, uses_hourly_close=1)
        self._insert_price("ES", "2026-02-18", 5900.0)

        cancellations = check_auto_cancellations(self.conn, "2026-02-18")
        self.assertEqual(len(cancellations), 0)

    @patch("nenner_engine.prices.fetch_yfinance_daily")
    def test_multiple_instruments(self, mock_fetch):
        """Two instruments, one breached and one not."""
        self._insert_signal("TSLA", "Tesla", "SELL", "ACTIVE",
                            425.0, "ABOVE", 418.0)
        self._insert_signal("MSFT", "Microsoft", "SELL", "ACTIVE",
                            469.0, "ABOVE", 409.0)
        self._insert_price("TSLA", "2026-02-18", 420.0)
        self._insert_price("MSFT", "2026-02-18", 400.0)

        cancellations = check_auto_cancellations(self.conn, "2026-02-18")
        self.assertEqual(len(cancellations), 1)
        self.assertEqual(cancellations[0]["ticker"], "TSLA")

    @patch("nenner_engine.prices.fetch_yfinance_daily")
    def test_no_price_data(self, mock_fetch):
        """No crash when there's no price data for an instrument."""
        self._insert_signal("TSLA", "Tesla", "SELL", "ACTIVE",
                            425.0, "ABOVE", 418.0)
        # No price inserted

        cancellations = check_auto_cancellations(self.conn, "2026-02-18")
        self.assertEqual(len(cancellations), 0)

    @patch("nenner_engine.prices.fetch_yfinance_daily")
    def test_duplicate_auto_cancel_ignored(self, mock_fetch):
        """Running auto-cancel twice for the same date produces no duplicates."""
        self._insert_signal("TSLA", "Tesla", "SELL", "ACTIVE",
                            425.0, "ABOVE", 418.0)
        self._insert_price("TSLA", "2026-02-18", 420.0)

        c1 = check_auto_cancellations(self.conn, "2026-02-18")
        self.assertEqual(len(c1), 1)

        # Second run — state already flipped, and message_id is duplicate
        c2 = check_auto_cancellations(self.conn, "2026-02-18")
        self.assertEqual(len(c2), 0)

    def tearDown(self):
        self.conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
