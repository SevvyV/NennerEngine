"""Tests for the atomicity guarantees added in Phase 3.

Three pipelines are now wrapped in explicit transactions:

1. process_email (imap_client) — store email + parse + store signals +
   rebuild current_state. A crash anywhere after the email INSERT must
   roll the email row out so message_id dedup does not block a retry.
2. auto_cancel — INSERT cancellation rows + rebuild current_state.
   A crash between the inserts and the rebuild must leave neither.
3. compute_current_state — DELETE + per-ticker INSERTs (already covered
   by test_compute_current_state.py::test_mid_loop_exception_rolls_back_delete).

These tests use fault injection: they monkey-patch a downstream call to
raise mid-flow and assert that the DB ends up in a clean pre-flow state.
"""

from __future__ import annotations

import unittest
from email.message import EmailMessage
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from conftest import make_test_db, seed_current_state, seed_price_history


# ---------------------------------------------------------------------------
# process_email atomicity
# ---------------------------------------------------------------------------

def _build_msg(subject: str, body: str, message_id: str) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = "newsletter@charlesnenner.com"
    msg["Date"] = "Mon, 27 Apr 2026 06:30:00 -0400"
    msg["Message-Id"] = message_id
    msg.set_content(body)
    return msg


class TestProcessEmailAtomicity(unittest.TestCase):

    def setUp(self):
        self.conn = make_test_db()

    def tearDown(self):
        self.conn.close()

    def _email_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]

    def _signal_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]

    def test_llm_failure_rolls_back_email_row(self):
        """A signal-bearing email type that returns 0 signals on retry
        must result in NO row left in the emails table — otherwise
        message_id dedup blocks the next IMAP poll's retry."""
        from nenner_engine import imap_client

        body = "Stocks Cycle Charts " + "x" * 100  # > 50 chars passes the empty check
        msg = _build_msg("Stocks Cycle Charts", body, "msg-llm-fail")

        # Always returns empty signals — simulates persistent LLM failure
        empty = {"signals": [], "cycles": [], "price_targets": []}
        with patch.object(imap_client, "parse_email_signals_llm", return_value=empty), \
             patch.object(imap_client, "classify_email", return_value="stocks_update"):
            ok = imap_client.process_email(self.conn, msg, source_id="test-1")

        self.assertFalse(ok)
        self.assertEqual(self._email_count(), 0,
                         "email row should have been rolled back")
        self.assertEqual(self._signal_count(), 0)

    def test_exception_during_store_rolls_back_email_row(self):
        """If store_parsed_results raises mid-flow, the email row must
        roll back too — half-stored state would leave us with an email
        row but no signals, blocking future retries."""
        from nenner_engine import imap_client

        body = "Gold continues on a buy signal from 2900 " + "x" * 100
        msg = _build_msg("Stocks Cycle Charts", body, "msg-store-fail")

        results = {
            "signals": [{
                "email_id": None, "date": "2026-04-27",
                "instrument": "Gold", "ticker": "GC",
                "asset_class": "Precious Metals",
                "signal_type": "BUY", "signal_status": "ACTIVE",
                "origin_price": 2900.0,
                "cancel_direction": "BELOW", "cancel_level": 2850.0,
                "trigger_direction": None, "trigger_level": None,
                "price_target": None, "target_direction": None,
                "note_the_change": 0, "uses_hourly_close": 0,
                "raw_text": "test",
            }],
            "cycles": [{
                "email_id": None, "date": "2026-04-27",
                "instrument": "Gold", "ticker": "GC",
                "timeframe": "daily", "direction": "UP",
                "until_description": "", "raw_text": "test",
            }],
            "price_targets": [],
        }

        # Inject failure at store_parsed_results — must roll back the
        # already-INSERTed email row plus any partial work.
        with patch.object(imap_client, "parse_email_signals_llm", return_value=results), \
             patch.object(imap_client, "classify_email", return_value="stocks_update"), \
             patch.object(
                 imap_client, "store_parsed_results",
                 side_effect=RuntimeError("simulated DB write failure"),
             ):
            with self.assertRaises(RuntimeError):
                imap_client.process_email(self.conn, msg, source_id="test-2")

        self.assertEqual(self._email_count(), 0,
                         "email row should have been rolled back")
        self.assertEqual(self._signal_count(), 0)

    def test_duplicate_message_id_does_not_open_lasting_transaction(self):
        """A duplicate-message_id short-circuit must commit (no-op) so
        the connection isn't left in a wedged-transaction state for the
        next caller."""
        from nenner_engine import imap_client

        body = "First " + "x" * 100
        msg = _build_msg("Test", body, "msg-dupe")

        results = {"signals": [], "cycles": [], "price_targets": []}
        with patch.object(imap_client, "parse_email_signals_llm", return_value=results), \
             patch.object(imap_client, "classify_email", return_value="other"):
            self.assertTrue(
                imap_client.process_email(self.conn, msg, source_id="dupe-1")
            )
            # Same message_id — should return False without raising.
            self.assertFalse(
                imap_client.process_email(self.conn, msg, source_id="dupe-2")
            )
        self.assertFalse(self.conn.in_transaction,
                         "connection must not be left in a half-open transaction")


# ---------------------------------------------------------------------------
# auto_cancel atomicity
# ---------------------------------------------------------------------------

class TestAutoCancelAtomicity(unittest.TestCase):

    def setUp(self):
        self.conn = make_test_db()

    def tearDown(self):
        self.conn.close()

    def test_compute_state_failure_rolls_back_cancellation_inserts(self):
        """If compute_current_state raises after the auto-cancel row has
        been INSERTed, the row must roll back — otherwise we'd have a
        synthetic CANCELLED signal with no matching state-table update,
        which means the watchlist still shows the dead BUY/SELL."""
        from nenner_engine import auto_cancel

        # Active BUY in current_state with cancel BELOW 4350
        seed_current_state(
            self.conn, ticker="GC", signal="BUY",
            origin_price=4400.0, cancel_level=4350.0,
            cancel_direction="BELOW",
        )
        # Daily close at 4300 — through the cancel
        seed_price_history(
            self.conn, ticker="GC", close=4300.0,
            price_date="2026-04-27", source="yfinance",
        )

        # Inject fault: compute_current_state raises after the INSERT.
        # auto_cancel imports it lazily inside the function body, so
        # patch the source module rather than auto_cancel's namespace.
        with patch(
            "nenner_engine.db.compute_current_state",
            side_effect=RuntimeError("simulated state-rebuild fault"),
        ):
            with self.assertRaises(RuntimeError):
                auto_cancel.check_auto_cancellations(
                    self.conn, price_date="2026-04-27",
                )

        # The cancellation INSERT must have rolled back along with the
        # failed state rebuild.
        n = self.conn.execute(
            "SELECT COUNT(*) FROM signals "
            "WHERE ticker='GC' AND date='2026-04-27' AND source='auto_cancel'"
        ).fetchone()[0]
        self.assertEqual(n, 0, "auto_cancel row must be rolled back on failure")
        self.assertFalse(self.conn.in_transaction)


if __name__ == "__main__":
    unittest.main()
