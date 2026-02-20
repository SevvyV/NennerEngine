"""
Database Layer
===============
SQLite schema, migrations, storage functions, and the signal state machine.
"""

import sqlite3
import logging
from datetime import datetime
from typing import Optional

log = logging.getLogger("nenner")


# ---------------------------------------------------------------------------
# Schema & Init
# ---------------------------------------------------------------------------

def init_db(db_path: str) -> sqlite3.Connection:
    """Initialize SQLite database with schema."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT UNIQUE,
            subject TEXT,
            date_sent TEXT,
            date_parsed TEXT,
            email_type TEXT,
            raw_text TEXT,
            signal_count INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id INTEGER REFERENCES emails(id),
            date TEXT,
            instrument TEXT,
            ticker TEXT,
            asset_class TEXT,
            signal_type TEXT,
            signal_status TEXT,
            origin_price REAL,
            cancel_direction TEXT,
            cancel_level REAL,
            trigger_direction TEXT,
            trigger_level REAL,
            price_target REAL,
            target_direction TEXT,
            note_the_change INTEGER DEFAULT 0,
            uses_hourly_close INTEGER DEFAULT 0,
            raw_text TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id INTEGER REFERENCES emails(id),
            date TEXT,
            instrument TEXT,
            ticker TEXT,
            timeframe TEXT,
            direction TEXT,
            until_description TEXT,
            raw_text TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS price_targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id INTEGER REFERENCES emails(id),
            date TEXT,
            instrument TEXT,
            ticker TEXT,
            target_price REAL,
            direction TEXT,
            condition TEXT,
            reached INTEGER DEFAULT 0,
            reached_date TEXT,
            raw_text TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        -- Current effective signal state per instrument (materialized view).
        -- Rebuilt by compute_current_state() after each email parse.
        CREATE TABLE IF NOT EXISTS current_state (
            ticker TEXT PRIMARY KEY,
            instrument TEXT,
            asset_class TEXT,
            effective_signal TEXT,
            effective_status TEXT,
            origin_price REAL,
            cancel_direction TEXT,
            cancel_level REAL,
            trigger_direction TEXT,
            trigger_level REAL,
            implied_reversal INTEGER DEFAULT 0,
            source_signal_id INTEGER REFERENCES signals(id),
            last_updated TEXT,
            last_signal_date TEXT
        );

        -- Price history for daily closes and real-time snapshots.
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            source TEXT NOT NULL,
            fetched_at TEXT DEFAULT (datetime('now')),
            UNIQUE(ticker, date, source)
        );

        CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(date);
        CREATE INDEX IF NOT EXISTS idx_signals_instrument ON signals(instrument);
        CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker);
        CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(signal_status);
        CREATE INDEX IF NOT EXISTS idx_cycles_date ON cycles(date);
        CREATE INDEX IF NOT EXISTS idx_cycles_instrument ON cycles(instrument);
        CREATE INDEX IF NOT EXISTS idx_emails_message_id ON emails(message_id);
        CREATE INDEX IF NOT EXISTS idx_emails_date ON emails(date_sent);
        CREATE INDEX IF NOT EXISTS idx_price_history_ticker_date
            ON price_history(ticker, date DESC);
    """)
    # Create views (outside executescript for compatibility)
    conn.execute("""
        CREATE VIEW IF NOT EXISTS latest_prices AS
        SELECT p.ticker, p.date, p.close, p.source, p.fetched_at
        FROM price_history p
        INNER JOIN (
            SELECT ticker, MAX(date) as max_date
            FROM price_history
            GROUP BY ticker
        ) lp ON p.ticker = lp.ticker AND p.date = lp.max_date
    """)
    conn.commit()
    return conn


def migrate_db(conn: sqlite3.Connection):
    """Apply schema migrations to an existing database.

    Safe to run multiple times -- each migration checks IF NOT EXISTS.
    """
    migrations = [
        # v2: Add current_state table
        """CREATE TABLE IF NOT EXISTS current_state (
            ticker TEXT PRIMARY KEY,
            instrument TEXT,
            asset_class TEXT,
            effective_signal TEXT,
            effective_status TEXT,
            origin_price REAL,
            cancel_direction TEXT,
            cancel_level REAL,
            trigger_direction TEXT,
            trigger_level REAL,
            implied_reversal INTEGER DEFAULT 0,
            source_signal_id INTEGER REFERENCES signals(id),
            last_updated TEXT,
            last_signal_date TEXT
        )""",
        "CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(signal_status)",
        # v3: Add price_history table
        """CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            source TEXT NOT NULL,
            fetched_at TEXT DEFAULT (datetime('now')),
            UNIQUE(ticker, date, source)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_price_history_ticker_date ON price_history(ticker, date DESC)",
        # v4: Add alert_log table for alert engine audit trail
        """CREATE TABLE IF NOT EXISTS alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            instrument TEXT,
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            message TEXT NOT NULL,
            current_price REAL,
            cancel_dist_pct REAL,
            trigger_dist_pct REAL,
            effective_signal TEXT,
            channels_sent TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )""",
        "CREATE INDEX IF NOT EXISTS idx_alert_log_ticker ON alert_log(ticker, alert_type, created_at DESC)",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # Already exists
    # Create views (idempotent)
    try:
        conn.execute("""
            CREATE VIEW IF NOT EXISTS latest_prices AS
            SELECT p.ticker, p.date, p.close, p.source, p.fetched_at
            FROM price_history p
            INNER JOIN (
                SELECT ticker, MAX(date) as max_date
                FROM price_history
                GROUP BY ticker
            ) lp ON p.ticker = lp.ticker AND p.date = lp.max_date
        """)
    except sqlite3.OperationalError:
        pass  # View already exists
    conn.commit()
    log.info("Database migrations applied")


# ---------------------------------------------------------------------------
# Signal State Machine
# ---------------------------------------------------------------------------

def compute_current_state(conn: sqlite3.Connection):
    """Rebuild the current_state table from signal history.

    For each instrument/ticker, walks the signal history chronologically and
    applies the Nenner state machine rules:

    1. An ACTIVE signal sets the current state to that signal (BUY/SELL).
    2. A CANCELLED signal flips the state to the OPPOSITE direction.
       - BUY cancelled -> effective SELL from the cancel level
       - SELL cancelled -> effective BUY from the cancel level
       This is because in Nenner's system, cancellation implies reversal.
    3. If the cancelled signal includes a trigger_level, that becomes the
       cancel_level for the new implied reversal signal (the level at which
       the implied signal would itself be cancelled).
    """
    # Get the latest signal per ticker, using date + id for proper ordering
    rows = conn.execute("""
        SELECT s.id, s.date, s.instrument, s.ticker, s.asset_class,
               s.signal_type, s.signal_status, s.origin_price,
               s.cancel_direction, s.cancel_level,
               s.trigger_direction, s.trigger_level
        FROM signals s
        INNER JOIN (
            SELECT ticker, MAX(date || '-' || printf('%010d', id)) as max_key
            FROM signals
            GROUP BY ticker
        ) latest ON s.ticker = latest.ticker
               AND (s.date || '-' || printf('%010d', s.id)) = latest.max_key
        ORDER BY s.ticker
    """).fetchall()

    conn.execute("DELETE FROM current_state")

    for row in rows:
        ticker = row["ticker"]
        signal_type = row["signal_type"]
        signal_status = row["signal_status"]

        if signal_status == "ACTIVE":
            conn.execute("""
                INSERT OR REPLACE INTO current_state
                (ticker, instrument, asset_class, effective_signal, effective_status,
                 origin_price, cancel_direction, cancel_level,
                 trigger_direction, trigger_level,
                 implied_reversal, source_signal_id, last_updated, last_signal_date)
                VALUES (?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, 0, ?, datetime('now'), ?)
            """, (ticker, row["instrument"], row["asset_class"],
                  signal_type, row["origin_price"],
                  row["cancel_direction"], row["cancel_level"],
                  row["trigger_direction"], row["trigger_level"],
                  row["id"], row["date"]))

        elif signal_status == "CANCELLED":
            # Cancellation implies reversal
            if signal_type == "BUY":
                implied_signal = "SELL"
            elif signal_type == "SELL":
                implied_signal = "BUY"
            else:
                implied_signal = "NEUTRAL"

            implied_origin = row["cancel_level"]
            implied_cancel_dir = row["trigger_direction"]
            implied_cancel_lvl = row["trigger_level"]

            conn.execute("""
                INSERT OR REPLACE INTO current_state
                (ticker, instrument, asset_class, effective_signal, effective_status,
                 origin_price, cancel_direction, cancel_level,
                 trigger_direction, trigger_level,
                 implied_reversal, source_signal_id, last_updated, last_signal_date)
                VALUES (?, ?, ?, ?, 'ACTIVE', ?, ?, ?, NULL, NULL, 1, ?, datetime('now'), ?)
            """, (ticker, row["instrument"], row["asset_class"],
                  implied_signal, implied_origin,
                  implied_cancel_dir, implied_cancel_lvl,
                  row["id"], row["date"]))

    conn.commit()
    log.info(f"Current state rebuilt: {len(rows)} instruments")


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def store_email(conn: sqlite3.Connection, message_id: str, subject: str,
                date_sent: str, email_type: str, raw_text: str) -> Optional[int]:
    """Store email metadata. Returns email_id or None if duplicate."""
    try:
        cur = conn.execute(
            "INSERT INTO emails (message_id, subject, date_sent, date_parsed, email_type, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (message_id, subject, date_sent, datetime.now().isoformat(), email_type, raw_text)
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        # Duplicate message_id
        return None


def store_parsed_results(conn: sqlite3.Connection, results: dict, email_id: int):
    """Store parsed signals, cycles, and price targets."""
    for sig in results["signals"]:
        conn.execute(
            "INSERT INTO signals (email_id, date, instrument, ticker, asset_class, "
            "signal_type, signal_status, origin_price, cancel_direction, cancel_level, "
            "trigger_direction, trigger_level, price_target, target_direction, "
            "note_the_change, uses_hourly_close, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (sig["email_id"], sig["date"], sig["instrument"], sig["ticker"],
             sig["asset_class"], sig["signal_type"], sig["signal_status"],
             sig["origin_price"], sig["cancel_direction"], sig["cancel_level"],
             sig["trigger_direction"], sig["trigger_level"],
             sig["price_target"], sig["target_direction"],
             sig["note_the_change"], sig["uses_hourly_close"], sig["raw_text"])
        )

    for cyc in results["cycles"]:
        conn.execute(
            "INSERT INTO cycles (email_id, date, instrument, ticker, timeframe, "
            "direction, until_description, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (cyc["email_id"], cyc["date"], cyc["instrument"], cyc["ticker"],
             cyc["timeframe"], cyc["direction"], cyc["until_description"], cyc["raw_text"])
        )

    for tgt in results["price_targets"]:
        conn.execute(
            "INSERT INTO price_targets (email_id, date, instrument, ticker, "
            "target_price, direction, condition, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (tgt["email_id"], tgt["date"], tgt["instrument"], tgt["ticker"],
             tgt["target_price"], tgt["direction"], tgt["condition"], tgt["raw_text"])
        )

    # Update signal count
    total = len(results["signals"]) + len(results["cycles"]) + len(results["price_targets"])
    conn.execute("UPDATE emails SET signal_count = ? WHERE id = ?", (total, email_id))
    conn.commit()

    # Rebuild current state after every email
    compute_current_state(conn)
