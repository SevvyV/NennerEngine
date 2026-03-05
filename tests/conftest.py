"""Shared test fixtures for NennerEngine test suite."""

import sqlite3
import unittest
from datetime import date, timedelta

import pytest

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from nenner_engine.db import init_db, migrate_db


# ---------------------------------------------------------------------------
# Helper: create a fully-migrated in-memory DB
# ---------------------------------------------------------------------------

def make_test_db() -> sqlite3.Connection:
    """Return an in-memory SQLite connection with the full NennerEngine schema."""
    conn = init_db(":memory:")
    migrate_db(conn)
    return conn


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def seed_current_state(conn, ticker="GC", instrument="Gold",
                       asset_class="Precious Metals",
                       signal="BUY", status="active",
                       origin_price=2650.0, cancel_level=2580.0,
                       cancel_direction="below", **kw):
    """Insert a row into current_state."""
    defaults = dict(
        ticker=ticker, instrument=instrument, asset_class=asset_class,
        effective_signal=signal, effective_status=status,
        origin_price=origin_price, cancel_direction=cancel_direction,
        cancel_level=cancel_level, trigger_direction=None,
        trigger_level=None, implied_reversal=0,
        source_signal_id=None, last_updated=date.today().isoformat(),
        last_signal_date=date.today().isoformat(),
    )
    defaults.update(kw)
    cols = ", ".join(defaults)
    placeholders = ", ".join(["?"] * len(defaults))
    conn.execute(
        f"INSERT OR REPLACE INTO current_state ({cols}) VALUES ({placeholders})",
        list(defaults.values()),
    )
    conn.commit()


def seed_cycles(conn, ticker="GC", directions=None):
    """Insert cycle rows. Default: daily/weekly/monthly all UP."""
    if directions is None:
        directions = {"daily": "UP", "weekly": "UP", "monthly": "UP"}
    for tf, d in directions.items():
        conn.execute(
            "INSERT INTO cycles (date, instrument, ticker, timeframe, direction, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (date.today().isoformat(), "", ticker, tf, d, "test"),
        )
    conn.commit()


def seed_price_history(conn, ticker="GC", close=2650.0,
                       price_date=None, source="test"):
    """Insert a price_history row."""
    if price_date is None:
        price_date = date.today().isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO price_history (ticker, date, close, source) "
        "VALUES (?, ?, ?, ?)",
        (ticker, price_date, close, source),
    )
    conn.commit()


def seed_fischer_recommendation(conn, ticker="AAPL", strike=220.0,
                                expiry=None, option_type="P",
                                entry_price=225.0, premium_per_share=2.50,
                                spot_at_recommend=225.0, settled=0,
                                intent="covered_put", **kw):
    """Insert a fischer_recommendations row and return the row id."""
    if expiry is None:
        expiry = (date.today() - timedelta(days=1)).isoformat()
    defaults = dict(
        report_date=date.today().isoformat(),
        ticker=ticker, strike=strike, expiry=expiry,
        option_type=option_type, entry_price=entry_price,
        premium_per_share=premium_per_share,
        spot_at_recommend=spot_at_recommend, settled=settled,
        intent=intent, scan_slot="opening",
    )
    defaults.update(kw)
    cols = ", ".join(defaults)
    placeholders = ", ".join(["?"] * len(defaults))
    cur = conn.execute(
        f"INSERT INTO fischer_recommendations ({cols}) VALUES ({placeholders})",
        list(defaults.values()),
    )
    conn.commit()
    return cur.lastrowid


# ---------------------------------------------------------------------------
# unittest mixin
# ---------------------------------------------------------------------------

class DBTestMixin:
    """Mixin for unittest.TestCase classes needing a fresh in-memory DB."""

    def setUp(self):
        super().setUp()
        self.conn = make_test_db()

    def tearDown(self):
        self.conn.close()
        super().tearDown()


# ---------------------------------------------------------------------------
# pytest fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def test_db():
    """Yield a fully-migrated in-memory DB, closed after test."""
    conn = make_test_db()
    yield conn
    conn.close()
