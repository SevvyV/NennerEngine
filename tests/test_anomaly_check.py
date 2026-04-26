"""Tests for anomaly_check — signal-deviation detection and alert dispatch."""

import unittest
from unittest.mock import patch

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from nenner_engine.anomaly_check import (
    check_signal_anomalies,
    alert_anomalies,
)
from conftest import make_test_db


def _seed_signal(conn, ticker, date_str, *,
                 origin_price=None, cancel_level=None, trigger_level=None,
                 signal_type="active"):
    conn.execute(
        "INSERT INTO signals (ticker, date, signal_type, origin_price, "
        "cancel_level, trigger_level, raw_text) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ticker, date_str, signal_type, origin_price, cancel_level,
         trigger_level, f"test signal {ticker}"),
    )
    conn.commit()


class TestCheckSignalAnomalies(unittest.TestCase):

    def setUp(self):
        self.conn = make_test_db()

    def tearDown(self):
        self.conn.close()

    def test_no_history_returns_no_anomalies(self):
        # Fresh ticker — no prior signals — nothing to compare.
        signals = [{"ticker": "GC", "cancel_level": 2580.0, "raw_text": "x"}]
        self.assertEqual(check_signal_anomalies(self.conn, signals), [])

    def test_value_within_threshold_not_flagged(self):
        # Median of [2580, 2585] = 2582.5; new 2590 deviates ~0.3% < 30%.
        _seed_signal(self.conn, "GC", "2026-04-20", cancel_level=2580.0)
        _seed_signal(self.conn, "GC", "2026-04-21", cancel_level=2585.0)
        signals = [{"ticker": "GC", "cancel_level": 2590.0, "raw_text": "x"}]
        self.assertEqual(check_signal_anomalies(self.conn, signals), [])

    def test_value_exceeding_threshold_flagged(self):
        # Median of [2580, 2585] = 2582.5; new 4000 = 55% deviation > 30%.
        _seed_signal(self.conn, "GC", "2026-04-20", cancel_level=2580.0)
        _seed_signal(self.conn, "GC", "2026-04-21", cancel_level=2585.0)
        signals = [{"ticker": "GC", "cancel_level": 4000.0,
                    "raw_text": "fat-fingered"}]
        anomalies = check_signal_anomalies(self.conn, signals)
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0]["ticker"], "GC")
        self.assertEqual(anomalies[0]["field"], "cancel_level")
        self.assertGreater(anomalies[0]["pct_diff"], 30)

    def test_zero_incoming_skipped(self):
        # Incoming = 0 should be skipped (legitimate "no value").
        _seed_signal(self.conn, "GC", "2026-04-20", cancel_level=2580.0)
        _seed_signal(self.conn, "GC", "2026-04-21", cancel_level=2585.0)
        signals = [{"ticker": "GC", "cancel_level": 0, "raw_text": "x"}]
        self.assertEqual(check_signal_anomalies(self.conn, signals), [])

    def test_multi_ticker_batch(self):
        """Multiple tickers in one call should be evaluated in a single
        round-trip — verify per-ticker boundaries are respected (history
        for GC must not pollute SI's anomaly detection)."""
        # GC: stable history at ~2580
        _seed_signal(self.conn, "GC", "2026-04-20", cancel_level=2580.0)
        _seed_signal(self.conn, "GC", "2026-04-21", cancel_level=2585.0)
        # SI: stable history at ~78
        _seed_signal(self.conn, "SI", "2026-04-20", cancel_level=78.0)
        _seed_signal(self.conn, "SI", "2026-04-21", cancel_level=78.5)

        signals = [
            {"ticker": "GC", "cancel_level": 2590.0, "raw_text": "ok"},
            {"ticker": "SI", "cancel_level": 200.0, "raw_text": "fat-finger"},
        ]
        anomalies = check_signal_anomalies(self.conn, signals)
        # Only SI's 200 is anomalous; GC's 2590 is normal.
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0]["ticker"], "SI")

    def test_threshold_is_configurable(self):
        # 10% threshold: 2580 → 2870 = 11% deviation, should flag.
        _seed_signal(self.conn, "GC", "2026-04-20", cancel_level=2580.0)
        _seed_signal(self.conn, "GC", "2026-04-21", cancel_level=2585.0)
        signals = [{"ticker": "GC", "cancel_level": 2870.0, "raw_text": "x"}]
        # Default 30% — not flagged
        self.assertEqual(check_signal_anomalies(self.conn, signals), [])
        # Tighter 5% — flagged
        anomalies = check_signal_anomalies(self.conn, signals, threshold=0.05)
        self.assertEqual(len(anomalies), 1)


class TestAlertAnomalies(unittest.TestCase):
    """alert_anomalies() must NEVER reach a real Telegram bot during tests."""

    def test_empty_list_short_circuits(self):
        # Should not call Telegram at all for an empty list.
        with patch("nenner_engine.anomaly_check.send_telegram") as mock_send:
            alert_anomalies([])
            mock_send.assert_not_called()

    def test_no_telegram_config_logged_only(self):
        with patch(
            "nenner_engine.anomaly_check.get_telegram_config",
            return_value=(None, None),
        ), patch("nenner_engine.anomaly_check.send_telegram") as mock_send:
            alert_anomalies([{
                "ticker": "GC", "field": "cancel_level",
                "incoming": 4000.0, "recent_median": 2582.5,
                "recent_values": [2580.0, 2585.0],
                "pct_diff": 55.0, "raw_text": "x",
            }])
            mock_send.assert_not_called()

    def test_telegram_called_with_summary(self):
        with patch(
            "nenner_engine.anomaly_check.get_telegram_config",
            return_value=("bot_token", "chat_id"),
        ), patch(
            "nenner_engine.anomaly_check.send_telegram", return_value=True,
        ) as mock_send:
            alert_anomalies([{
                "ticker": "GC", "field": "cancel_level",
                "incoming": 4000.0, "recent_median": 2582.5,
                "recent_values": [2580.0, 2585.0],
                "pct_diff": 55.0, "raw_text": "fat-fingered",
            }])
            self.assertEqual(mock_send.call_count, 1)
            args, _ = mock_send.call_args
            message = args[0]
            self.assertIn("GC", message)
            self.assertIn("cancel_level", message)
            self.assertIn("4000", message)

    def test_telegram_failure_swallowed(self):
        # Even if send_telegram raises, alert_anomalies must not propagate.
        with patch(
            "nenner_engine.anomaly_check.get_telegram_config",
            return_value=("bot_token", "chat_id"),
        ), patch(
            "nenner_engine.anomaly_check.send_telegram",
            side_effect=RuntimeError("boom"),
        ):
            try:
                alert_anomalies([{
                    "ticker": "GC", "field": "cancel_level",
                    "incoming": 4000.0, "recent_median": 2582.5,
                    "recent_values": [2580.0, 2585.0],
                    "pct_diff": 55.0, "raw_text": "x",
                }])
            except Exception as e:
                self.fail(f"alert_anomalies should swallow Telegram errors: {e}")


if __name__ == "__main__":
    unittest.main()
