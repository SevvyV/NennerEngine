"""Tests for the FastAPI signals server consumed by FischerDaily.

The API is read-only and small, but it's the contract surface between
NennerEngine and FischerDaily — a regression here breaks the downstream
options engine. Cover every route at least once.
"""

import os
import sqlite3
import tempfile
import unittest
from datetime import date

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi.testclient import TestClient

from nenner_engine.db import init_db, migrate_db, compute_current_state


def _seed_demo_db(path: str) -> None:
    """Build a tiny but realistic DB at *path* with one BUY signal and a
    cycle/target so every route returns non-empty data."""
    conn = init_db(path)
    migrate_db(conn)

    today = date.today().isoformat()
    cur = conn.execute(
        "INSERT INTO emails (message_id, subject, date_sent, date_parsed, "
        "email_type, raw_text) "
        "VALUES (?, ?, ?, datetime('now'), 'morning_update', 'test')",
        ("api-test-1", "Test", today),
    )
    email_id = cur.lastrowid

    conn.execute(
        "INSERT INTO signals (email_id, date, instrument, ticker, asset_class, "
        "signal_type, signal_status, origin_price, cancel_direction, cancel_level, "
        "trigger_direction, trigger_level, note_the_change, uses_hourly_close, "
        "raw_text) "
        "VALUES (?, ?, 'Gold', 'GC', 'Precious Metals', 'BUY', 'ACTIVE', "
        "4400.0, 'BELOW', 4350.0, 'ABOVE', 4350.0, 1, 0, 'test')",
        (email_id, today),
    )
    conn.execute(
        "INSERT INTO cycles (email_id, date, instrument, ticker, timeframe, "
        "direction, raw_text) "
        "VALUES (?, ?, 'Gold', 'GC', 'daily', 'UP', 'test')",
        (email_id, today),
    )
    # Direction must be UPSIDE (not 'UP') to match a BUY signal — see
    # the join in get_latest_targets.
    conn.execute(
        "INSERT INTO price_targets (email_id, date, instrument, ticker, "
        "target_price, direction, condition, reached, raw_text) "
        "VALUES (?, ?, 'Gold', 'GC', 4500.0, 'UPSIDE', 'upside', 0, 'test')",
        (email_id, today),
    )
    conn.commit()
    compute_current_state(conn)
    conn.close()


class TestSignalsAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Use a real temp file so check_same_thread=False/WAL pragmas work
        # exactly as in production. :memory: would not survive the
        # per-request reconnect inside _get_conn().
        fd, cls.db_path = tempfile.mkstemp(suffix=".db", prefix="api-test-")
        os.close(fd)
        _seed_demo_db(cls.db_path)

        # Import lazily so create_app sees the seeded DB path.
        from nenner_engine.api import create_app
        cls.app = create_app(cls.db_path)
        cls.client = TestClient(cls.app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        try:
            os.remove(cls.db_path)
        except OSError:
            pass

    # ---- Health ---------------------------------------------------------

    def test_health_returns_ok_and_count(self):
        r = self.client.get("/health")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["status"], "ok")
        self.assertGreaterEqual(body["instruments"], 1)
        self.assertEqual(body["db"], self.db_path)

    # ---- Current state --------------------------------------------------

    def test_current_state_returns_seeded_signal(self):
        r = self.client.get("/signals/current-state")
        self.assertEqual(r.status_code, 200)
        rows = r.json()["data"]
        self.assertTrue(any(row["ticker"] == "GC" for row in rows))

    def test_current_state_filters_by_ticker(self):
        r = self.client.get("/signals/current-state?tickers=GC")
        rows = r.json()["data"]
        self.assertTrue(rows)
        for row in rows:
            self.assertEqual(row["ticker"], "GC")

    def test_current_state_unknown_ticker_returns_empty_list(self):
        r = self.client.get("/signals/current-state?tickers=ZZZZ")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["data"], [])

    # ---- History --------------------------------------------------------

    def test_history_returns_signal_rows(self):
        r = self.client.get("/signals/history/GC")
        self.assertEqual(r.status_code, 200)
        rows = r.json()["data"]
        self.assertTrue(rows)
        self.assertEqual(rows[0]["ticker"], "GC")

    def test_history_limit_is_validated(self):
        # ge=1, le=500 — 0 must be rejected.
        r = self.client.get("/signals/history/GC?limit=0")
        self.assertEqual(r.status_code, 422)
        r = self.client.get("/signals/history/GC?limit=10000")
        self.assertEqual(r.status_code, 422)

    # ---- Cycles ---------------------------------------------------------

    def test_cycles_returns_seeded_row(self):
        r = self.client.get("/signals/cycles/GC")
        self.assertEqual(r.status_code, 200)
        rows = r.json()["data"]
        self.assertTrue(any(row["timeframe"] == "daily" for row in rows))

    # ---- Targets --------------------------------------------------------

    def test_targets_returns_unreached(self):
        r = self.client.get("/signals/targets/GC")
        self.assertEqual(r.status_code, 200)
        rows = r.json()["data"]
        self.assertTrue(rows)
        self.assertEqual(rows[0]["target_price"], 4500.0)

    # ---- NTC count ------------------------------------------------------

    def test_ntc_count_returns_int(self):
        r = self.client.get("/signals/ntc-count/GC")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertIn("count", body)
        self.assertIsInstance(body["count"], int)

    def test_ntc_count_days_validated(self):
        r = self.client.get("/signals/ntc-count/GC?days=0")
        self.assertEqual(r.status_code, 422)

    # ---- Snapshot -------------------------------------------------------

    def test_snapshot_returns_dict_keyed_by_ticker(self):
        r = self.client.get("/signals/snapshot")
        self.assertEqual(r.status_code, 200)
        data = r.json()["data"]
        self.assertIsInstance(data, dict)
        self.assertIn("GC", data)

    # ---- Instruments ----------------------------------------------------

    def test_instruments_lists_seeded_ticker(self):
        # get_instruments_with_signals returns list[str], not list[dict].
        r = self.client.get("/signals/instruments")
        self.assertEqual(r.status_code, 200)
        tickers = r.json()["data"]
        self.assertIn("GC", tickers)

    # ---- Search ---------------------------------------------------------

    def test_search_requires_pattern(self):
        r = self.client.get("/signals/search")
        self.assertEqual(r.status_code, 422)

    def test_search_pattern_minimum_length_enforced(self):
        # min_length=1 — empty string rejected.
        r = self.client.get("/signals/search?pattern=")
        self.assertEqual(r.status_code, 422)

    def test_search_returns_matching_rows(self):
        r = self.client.get("/signals/search?pattern=Gold")
        self.assertEqual(r.status_code, 200)
        rows = r.json()["data"]
        self.assertTrue(rows)


if __name__ == "__main__":
    unittest.main()
