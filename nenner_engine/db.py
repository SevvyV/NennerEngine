"""
Database Layer
===============
SQLite schema, migrations, storage functions, and the signal state machine.
"""

import sqlite3
import logging
from datetime import datetime
from typing import Optional

log = logging.getLogger(__name__)

# Bump this when a migration is appended to the list in migrate_db().
# Used to short-circuit the per-connection migration dance that was
# previously paying a ~15-statement cost on every scheduler tick.
CURRENT_SCHEMA_VERSION = 16


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


def _get_schema_version(conn: sqlite3.Connection) -> int:
    """Return the stored schema version, or 0 if the tracker doesn't exist."""
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)"
        )
        row = conn.execute(
            "SELECT version FROM schema_version WHERE id = 1"
        ).fetchone()
        if row is None:
            return 0
        return int(row[0] if not isinstance(row, sqlite3.Row) else row["version"])
    except sqlite3.OperationalError:
        return 0


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO schema_version (id, version) VALUES (1, ?)",
        (version,),
    )
    conn.commit()


def migrate_db(conn: sqlite3.Connection):
    """Apply schema migrations to an existing database.

    Short-circuits when the stored schema_version already matches
    CURRENT_SCHEMA_VERSION (the common case on every scheduler tick and
    every dashboard callback). On first run against an existing DB, the
    version tracker is absent so we run through all migrations — each one
    is idempotent (IF NOT EXISTS / try-pass on ALTER) so it's safe.
    """
    stored = _get_schema_version(conn)
    if stored >= CURRENT_SCHEMA_VERSION:
        return

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
        # v5: Stanley knowledge base and briefs
        """CREATE TABLE IF NOT EXISTS stanley_knowledge (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            instrument TEXT,
            rule_text TEXT NOT NULL,
            confidence REAL DEFAULT 1.0,
            source TEXT DEFAULT 'user_correction',
            created_at TEXT DEFAULT (datetime('now')),
            active INTEGER DEFAULT 1
        )""",
        "CREATE INDEX IF NOT EXISTS idx_stanley_knowledge_active ON stanley_knowledge(active, category)",
        """CREATE TABLE IF NOT EXISTS stanley_briefs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id INTEGER REFERENCES emails(id),
            brief_text TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )""",
        "CREATE INDEX IF NOT EXISTS idx_stanley_briefs_email ON stanley_briefs(email_id)",
        # v6: Fischer daily recommendations tracker
        """CREATE TABLE IF NOT EXISTS fischer_recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            strike REAL NOT NULL,
            expiry TEXT NOT NULL,
            option_type TEXT NOT NULL DEFAULT 'P',
            bid REAL,
            ask REAL,
            iv REAL,
            delta REAL,
            p_otm REAL,
            p_win REAL,
            max_profit_per_share REAL,
            net_ev_per_contract REAL,
            nenner_score INTEGER,
            spot_at_recommend REAL,
            entry_price REAL,
            premium_per_share REAL,
            rank INTEGER,
            settled INTEGER DEFAULT 0,
            close_price_at_expiry REAL,
            itm_at_expiry INTEGER,
            pnl_per_share REAL,
            pnl_total REAL,
            settlement_date TEXT,
            settlement_notes TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )""",
        "CREATE INDEX IF NOT EXISTS idx_fischer_recs_date ON fischer_recommendations(report_date)",
        "CREATE INDEX IF NOT EXISTS idx_fischer_recs_expiry ON fischer_recommendations(expiry, settled)",
        # v7: Fischer multi-scan — add scan_slot column for 3 daily scans
        "ALTER TABLE fischer_recommendations ADD COLUMN scan_slot TEXT NOT NULL DEFAULT 'opening'",
        "CREATE INDEX IF NOT EXISTS idx_fischer_recs_date_slot ON fischer_recommendations(report_date, scan_slot)",
        # v8: Fischer covered calls — add intent column
        "ALTER TABLE fischer_recommendations ADD COLUMN intent TEXT NOT NULL DEFAULT 'covered_put'",
        # v9: Fischer subscription — portfolio definitions
        """CREATE TABLE IF NOT EXISTS fischer_portfolios (
            portfolio_name TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            tickers TEXT NOT NULL,
            share_alloc TEXT NOT NULL DEFAULT '{}',
            show_conviction INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        )""",
        # v10: Fischer subscription — subscriber registry
        """CREATE TABLE IF NOT EXISTS fischer_subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            first_name TEXT NOT NULL DEFAULT '',
            last_name TEXT NOT NULL DEFAULT '',
            portfolio_name TEXT NOT NULL DEFAULT 'fischer_daily',
            active INTEGER NOT NULL DEFAULT 1,
            max_daily_refreshes INTEGER NOT NULL DEFAULT 25,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (portfolio_name) REFERENCES fischer_portfolios(portfolio_name)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_fischer_sub_email ON fischer_subscribers(email)",
        "CREATE INDEX IF NOT EXISTS idx_fischer_sub_active ON fischer_subscribers(active)",
        # v11: Fischer subscription — refresh request audit log
        """CREATE TABLE IF NOT EXISTS fischer_refresh_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subscriber_id INTEGER NOT NULL,
            requested_at TEXT NOT NULL DEFAULT (datetime('now')),
            email_subject TEXT,
            portfolio_name TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            error_message TEXT,
            FOREIGN KEY (subscriber_id) REFERENCES fischer_subscribers(id)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_refresh_log_sub_date ON fischer_refresh_log(subscriber_id, requested_at)",
        # v12: Fischer v2 — premium ratio and theta for ranking
        "ALTER TABLE fischer_recommendations ADD COLUMN premium_ratio REAL",
        "ALTER TABLE fischer_recommendations ADD COLUMN theta_per_share REAL",
        # v13: Fischer v2 — update default portfolio to 17-ticker universe
        """UPDATE fischer_portfolios
           SET tickers = 'AAPL,AMZN,AVGO,GOOGL,IWM,META,MSFT,NVDA,QQQ,TSLA,GLD,IBIT,SLV,SPY,TLT,UNG,USO',
               share_alloc = '{"AAPL":1800,"AMZN":2100,"AVGO":2200,"GOOGL":1600,"IWM":2200,"META":700,"MSFT":1200,"NVDA":2800,"QQQ":900,"TSLA":1200,"GLD":1800,"IBIT":9000,"SLV":16000,"SPY":800,"TLT":5500,"UNG":20000,"USO":6000}',
               show_conviction = 1
           WHERE portfolio_name = 'fischer_daily'""",
        # v14: Replace IBIT with MSTR in default portfolio
        """UPDATE fischer_portfolios
           SET tickers = 'AAPL,AMZN,AVGO,GOOGL,IWM,META,MSFT,NVDA,QQQ,TSLA,GLD,MSTR,SLV,SPY,TLT,UNG,USO',
               share_alloc = '{"AAPL":1800,"AMZN":2100,"AVGO":2200,"GOOGL":1600,"IWM":2200,"META":700,"MSFT":1200,"NVDA":2800,"QQQ":900,"TSLA":1200,"GLD":1800,"MSTR":1400,"SLV":16000,"SPY":800,"TLT":5500,"UNG":20000,"USO":6000}'
           WHERE portfolio_name = 'fischer_daily'""",
        # v15: Prevent duplicate Stanley briefs for the same email
        # First, remove duplicates keeping the latest brief per email_id
        """DELETE FROM stanley_briefs WHERE id NOT IN (
            SELECT MAX(id) FROM stanley_briefs WHERE email_id IS NOT NULL GROUP BY email_id
        ) AND email_id IS NOT NULL""",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_stanley_briefs_email_unique ON stanley_briefs(email_id) WHERE email_id IS NOT NULL",
        # v16: Tag signals with their source ('nenner' for parsed emails,
        # 'auto_cancel' for close-breach synthetic CANCELLED rows). Lets
        # auto_cancel skip storing fake emails and makes its rows easy to
        # identify/regenerate on bad-data correction.
        "ALTER TABLE signals ADD COLUMN source TEXT NOT NULL DEFAULT 'nenner'",
        "CREATE INDEX IF NOT EXISTS idx_signals_source_ticker_date ON signals(source, ticker, date)",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError as e:
            # The only expected "already applied" error is duplicate-column
            # from ALTER TABLE ADD COLUMN — every CREATE/INDEX in this list
            # uses IF NOT EXISTS, every UPDATE/DELETE is idempotent. Any
            # other OperationalError is a real schema bug; surfacing it is
            # critical because the loop below stamps schema_version anyway,
            # so a swallowed error becomes a silently-corrupt schema that
            # never gets retried.
            if "duplicate column" not in str(e).lower():
                raise
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
    # Seed Stanley knowledge base on first run
    _seed_stanley_knowledge(conn)
    # Seed default Fischer portfolio on first run
    _seed_fischer_portfolios(conn)
    # Stamp the version so subsequent migrate_db() calls are no-ops
    _set_schema_version(conn, CURRENT_SCHEMA_VERSION)
    log.info(
        f"Database migrated from v{stored} to v{CURRENT_SCHEMA_VERSION}"
    )


def _seed_stanley_knowledge(conn: sqlite3.Connection):
    """Insert initial knowledge rules if the stanley_knowledge table is empty."""
    try:
        count = conn.execute("SELECT COUNT(*) FROM stanley_knowledge").fetchone()[0]
    except sqlite3.OperationalError:
        return  # Table doesn't exist yet
    if count > 0:
        return

    seeds = [
        ("pattern", None,
         "Cancellation implies reversal in Nenner's system. A cancelled BUY becomes an effective SELL and vice versa."),
        ("cross_instrument", None,
         "DXY (US Dollar Index) and EUR/USD are inverse. When DXY gets a BUY, expect EUR/USD to get a SELL."),
        ("pattern", None,
         "The 'note the change' flag means the cancel level was adjusted from a prior email. This often indicates Nenner is tightening or loosening the stop."),
        ("pattern", None,
         "3+ cancel level changes in a short period (1-2 weeks) for the same instrument often precedes a signal flip."),
        ("cross_instrument", None,
         "Daily cycle up + weekly cycle down = potential chop zone. Be cautious with new positions in this configuration."),
        ("cross_instrument", None,
         "Gold (GC) and Silver (SI) tend to move together. A cancellation in one often foreshadows a cancellation in the other within days."),
        ("preference", None,
         "Instruments with risk_flag AVOID should be highlighted prominently in the brief so the trader is warned."),
    ]
    for category, instrument, rule_text in seeds:
        conn.execute(
            "INSERT INTO stanley_knowledge (category, instrument, rule_text, confidence, source) "
            "VALUES (?, ?, ?, 1.0, 'system')",
            (category, instrument, rule_text)
        )
    conn.commit()
    log.info(f"Stanley knowledge base seeded with {len(seeds)} rules")


def _seed_fischer_portfolios(conn: sqlite3.Connection):
    """Seed the default Fischer Daily portfolio if the table is empty."""
    try:
        count = conn.execute("SELECT COUNT(*) FROM fischer_portfolios").fetchone()[0]
    except sqlite3.OperationalError:
        return  # Table doesn't exist yet
    if count > 0:
        return

    conn.execute(
        "INSERT INTO fischer_portfolios (portfolio_name, label, tickers, share_alloc, show_conviction) "
        "VALUES (?, ?, ?, ?, ?)",
        ("fischer_daily", "Fischer Daily",
         "AAPL,AMZN,AVGO,GOOGL,IWM,META,MSFT,NVDA,QQQ,TSLA,GLD,MSTR,SLV,SPY,TLT,UNG,USO",
         '{"AAPL":1800,"AMZN":2100,"AVGO":2200,"GOOGL":1600,"IWM":2200,"META":700,'
         '"MSFT":1200,"NVDA":2800,"QQQ":900,"TSLA":1200,"GLD":1800,"MSTR":1400,'
         '"SLV":16000,"SPY":800,"TLT":5500,"UNG":20000,"USO":6000}', 1)
    )
    conn.commit()
    log.info("Fischer portfolios seeded with default 'fischer_daily'")


# ---------------------------------------------------------------------------
# Signal State Machine
# ---------------------------------------------------------------------------

def is_cancel_breached(cancel_direction: Optional[str],
                       cancel_level: Optional[float],
                       close: Optional[float]) -> bool:
    """Nenner cancel-breach rule, centralized.

    A BUY signal (cancel_direction='BELOW') is breached by a close strictly
    below the cancel level. A SELL signal (cancel_direction='ABOVE') is
    breached by a close strictly above. Equality is NOT a breach — Nenner's
    rule requires the close to go through the level, not touch it.

    Returns False if any input is None or the direction is unknown.
    """
    if cancel_direction is None or cancel_level is None or close is None:
        return False
    if cancel_direction == "ABOVE":
        return close > cancel_level
    if cancel_direction == "BELOW":
        return close < cancel_level
    return False


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
    # Only rebuild state for tickers still in the instrument map
    from .instruments import INSTRUMENT_MAP
    active_tickers = {info["ticker"] for info in INSTRUMENT_MAP.values()}

    # Ensure row_factory is set for dict-style access
    conn.row_factory = sqlite3.Row

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
    rows = [r for r in rows if r["ticker"] in active_tickers]

    # Pre-fetch latest closing prices for cancel-breach detection.
    # If an ACTIVE signal's cancel level is already breached by the most
    # recent daily close, treat it as an implied reversal immediately
    # instead of waiting for the next auto-cancel run.
    # IMPORTANT: Exclude DATABENTO_EQUITY — those are intraday midpoint
    # snapshots, NOT daily closes.  Cancel levels require a confirmed
    # close (per Nenner rules), so only settled daily bars count.
    latest_prices: dict[str, float] = {}
    for price_row in conn.execute("""
        SELECT ticker, close FROM price_history
        WHERE (ticker, date) IN (
            SELECT ticker, MAX(date) FROM price_history
            WHERE source != 'DATABENTO_EQUITY'
            GROUP BY ticker
        ) AND close IS NOT NULL
        AND source != 'DATABENTO_EQUITY'
    """).fetchall():
        latest_prices[price_row["ticker"]] = price_row["close"]

    # Atomic refresh: the DELETE and all INSERTs commit together, or a
    # mid-loop exception rolls them back together. Without this, a crash
    # would leave the implicit transaction open with the DELETE pending —
    # a subsequent commit on the same connection from another code path
    # would commit the DELETE alone and silently empty current_state.
    with conn:
        conn.execute("DELETE FROM current_state")

        for row in rows:
            ticker = row["ticker"]
            signal_type = row["signal_type"]
            signal_status = row["signal_status"]

            if signal_status == "ACTIVE":
                # Check if the cancel level is already breached by latest close
                cancel_dir = row["cancel_direction"]
                cancel_level = row["cancel_level"]
                last_close = latest_prices.get(ticker)
                already_breached = is_cancel_breached(cancel_dir, cancel_level, last_close)

                if already_breached:
                    # Signal is ACTIVE per Nenner text but cancel is already
                    # breached — flip to implied reversal so the watchlist
                    # reflects reality instead of waiting for auto-cancel.
                    log.info(
                        f"State rebuild: {ticker} {signal_type} cancel "
                        f"{cancel_dir} {cancel_level} already breached "
                        f"(close={last_close:.2f}) — marking implied reversal"
                    )
                    signal_status = "CANCELLED"
                else:
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

            if signal_status == "CANCELLED":
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
