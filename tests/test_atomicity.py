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

    def test_llm_runs_outside_write_transaction(self):
        """Phase 3.1 invariant: parse_email_signals_llm must NOT be called
        while a write transaction is open. The Anthropic round-trip is
        5-30s; holding the SQLite write lock that long blocks the equity
        stream and other concurrent writers. Any future refactor that
        accidentally moves the LLM back inside BEGIN is caught here."""
        from nenner_engine import imap_client

        body = "Gold cycle update " + "x" * 100
        msg = _build_msg("Stocks Cycle Charts", body, "msg-llm-tx-check")

        observed = {"in_tx_during_llm": None}

        def fake_llm(body_arg, email_date, email_id=None):
            observed["in_tx_during_llm"] = self.conn.in_transaction
            return {
                "signals": [{
                    "email_id": email_id, "date": "2026-04-27",
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
                    "email_id": email_id, "date": "2026-04-27",
                    "instrument": "Gold", "ticker": "GC",
                    "timeframe": "daily", "direction": "UP",
                    "until_description": "", "raw_text": "test",
                }],
                "price_targets": [],
            }

        with patch.object(imap_client, "parse_email_signals_llm", side_effect=fake_llm), \
             patch.object(imap_client, "classify_email", return_value="stocks_update"):
            ok = imap_client.process_email(self.conn, msg, source_id="tx-check")

        self.assertTrue(ok)
        self.assertIs(observed["in_tx_during_llm"], False,
                      "LLM was called while a write transaction was open — "
                      "this holds the SQLite write lock for the duration of "
                      "the API call and starves concurrent writers.")
        # And the row really did land
        self.assertEqual(self._email_count(), 1)
        self.assertEqual(self._signal_count(), 1)

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


class TestSchedulerBriefDedup(unittest.TestCase):
    """Regression test for the scheduler-level brief dedup query.

    email_scheduler.run_email_check has its own pre-check that skips
    invoking generate_morning_brief if a brief already exists for the
    new email. Phase 1b added a sent_at column so a brief that was
    stored but failed to send can be retried — the pre-check MUST
    honor sent_at, otherwise it skips the retry that stanley.py would
    have performed correctly.

    This test pins the SQL: a row with sent_at NULL must NOT cause
    the dedup to fire.
    """

    def setUp(self):
        self.conn = make_test_db()

    def tearDown(self):
        self.conn.close()

    def _seed_email_and_brief(self, *, sent: bool) -> int:
        cur = self.conn.execute(
            "INSERT INTO emails (message_id, subject, date_sent, date_parsed, "
            "email_type, raw_text) "
            "VALUES (?, ?, ?, datetime('now'), 'morning_update', 'b')",
            ("m1", "subj", "2026-04-26"),
        )
        email_id = cur.lastrowid
        self.conn.execute(
            "INSERT INTO stanley_briefs (email_id, brief_text, sent_at) "
            "VALUES (?, ?, ?)",
            (email_id, "brief text",
             "2026-04-26T08:30:00" if sent else None),
        )
        self.conn.commit()
        return email_id

    def _scheduler_dedup_query(self, email_id: int) -> bool:
        """Replicate the SQL the scheduler runs at email_scheduler.py:293."""
        row = self.conn.execute(
            "SELECT 1 FROM stanley_briefs "
            "WHERE email_id = ? AND sent_at IS NOT NULL LIMIT 1",
            (email_id,),
        ).fetchone()
        return row is not None

    def test_sent_brief_is_deduped(self):
        """Already-sent brief: pre-check must short-circuit."""
        email_id = self._seed_email_and_brief(sent=True)
        self.assertTrue(self._scheduler_dedup_query(email_id))

    def test_unsent_brief_is_NOT_deduped(self):
        """Stored-but-unsent brief: pre-check must allow generate_morning_brief
        to run so it can reuse the stored text and retry the email step."""
        email_id = self._seed_email_and_brief(sent=False)
        self.assertFalse(self._scheduler_dedup_query(email_id))

    def test_no_brief_at_all_is_NOT_deduped(self):
        """Fresh email: no row exists, pre-check must allow brief generation."""
        cur = self.conn.execute(
            "INSERT INTO emails (message_id, subject, date_sent, date_parsed, "
            "email_type, raw_text) VALUES ('fresh', 's', '2026-04-26', "
            "datetime('now'), 'morning_update', 'b')",
        )
        self.conn.commit()
        self.assertFalse(self._scheduler_dedup_query(cur.lastrowid))


if __name__ == "__main__":
    unittest.main()
