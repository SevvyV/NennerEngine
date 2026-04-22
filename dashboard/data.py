"""Dashboard data layer — DB queries, watchlist config, prev-close cache.

Pure-ish: every public function returns plain dicts/values from the DB or
yfinance. No Dash imports, no UI concerns. The DB_PATH module-level is
mutable so the CLI --db override in lifecycle.py works.
"""

import logging
import math
import sqlite3
import threading

from nenner_engine.config import DEFAULT_DB_PATH

log = logging.getLogger("nenner_engine")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Mutable on purpose — lifecycle.main() may override it from --db before
# any callback opens a connection.
DB_PATH = DEFAULT_DB_PATH

WATCHLIST_ROW1 = ["TSLA", "BAC", "MSFT", "AAPL", "GOOG", "NVDA"]
WATCHLIST_ROW2 = ["GDXJ", "GLD", "SLV", "USO", "UNG", "SOYB", "NEM"]
WATCHLIST_ROW3 = ["ES", "NQ", "GBTC", "ETHE"]
WATCHLIST_TICKERS = WATCHLIST_ROW1 + WATCHLIST_ROW2 + WATCHLIST_ROW3


# Previous close cache (refreshed once per day). Lock protects read-modify-
# write from concurrent Dash callback threads — the Market Data callback
# is invoked on the Flask threadpool and a naive dict update was racy.
_prev_close_cache: dict[str, float] = {}
_prev_close_date: str = ""
_prev_close_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    """Open a dashboard-scoped SQLite connection.

    Every callback takes one of these per invocation; we set busy_timeout
    so concurrent writers (AlertMonitor, EquityStream flush, scheduler) can
    never trigger a hard "database is locked" error that silently blanks
    the UI.  WAL mode is a database-level setting and was applied once at
    startup by init_db(); we reassert it here as a no-op safety.
    """
    conn = sqlite3.connect(DB_PATH, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ---------------------------------------------------------------------------
# Data Queries
# ---------------------------------------------------------------------------

def fetch_current_state():
    """Fetch current signal states, excluding stale signals (>3 months old)."""
    conn = get_db()
    rows = conn.execute("""
        SELECT ticker, instrument, asset_class, effective_signal,
               origin_price, cancel_direction, cancel_level,
               trigger_level, implied_reversal, last_signal_date
        FROM current_state
        WHERE last_signal_date >= date('now', '-3 months')
        ORDER BY asset_class, instrument
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fetch_recent_changes(days=7):
    """Fetch recent signal changes from signals table."""
    conn = get_db()
    rows = conn.execute("""
        SELECT s.date, s.instrument, s.ticker, s.signal_type, s.signal_status,
               s.origin_price, s.cancel_level, s.note_the_change
        FROM signals s
        WHERE s.date >= date('now', ?)
        ORDER BY s.date DESC, s.id DESC
        LIMIT 50
    """, (f"-{days} days",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fetch_watchlist():
    """Fetch watchlist instrument states enriched with live prices."""
    try:
        from nenner_engine.prices import get_prices_with_signal_context
        conn = get_db()
        rows = get_prices_with_signal_context(conn, tickers=WATCHLIST_TICKERS, try_t1=True)
        conn.close()
        return rows
    except Exception as e:
        log.error("fetch_watchlist price enrichment failed: %s", e, exc_info=True)
        # Fallback: signal-only (no prices)
        conn = get_db()
        placeholders = ",".join("?" for _ in WATCHLIST_TICKERS)
        rows = conn.execute(f"""
            SELECT ticker, instrument, asset_class, effective_signal,
                   origin_price, cancel_direction, cancel_level,
                   trigger_level, implied_reversal, last_signal_date
            FROM current_state
            WHERE ticker IN ({placeholders})
            ORDER BY instrument
        """, WATCHLIST_TICKERS).fetchall()
        conn.close()
        return [dict(r) for r in rows]


def fetch_positions():
    """Fetch live positions from Excel and enrich with Nenner signals."""
    try:
        from nenner_engine.positions import get_positions_with_signal_context
        conn = get_db()
        enriched = get_positions_with_signal_context(conn)
        conn.close()
        return enriched
    except Exception:
        return []


def fetch_db_stats():
    """Fetch database summary stats."""
    conn = get_db()
    active_filter = "WHERE last_signal_date >= date('now', '-3 months')"
    stats = {
        "emails": conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0],
        "signals": conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0],
        "cycles": conn.execute("SELECT COUNT(*) FROM cycles").fetchone()[0],
        "targets": conn.execute("SELECT COUNT(*) FROM price_targets").fetchone()[0],
        "instruments": conn.execute(f"SELECT COUNT(*) FROM current_state {active_filter}").fetchone()[0],
    }
    buys = conn.execute(f"SELECT COUNT(*) FROM current_state {active_filter} AND effective_signal='BUY'").fetchone()[0]
    sells = conn.execute(f"SELECT COUNT(*) FROM current_state {active_filter} AND effective_signal='SELL'").fetchone()[0]
    stats["buys"] = buys
    stats["sells"] = sells
    date_range = conn.execute("SELECT MIN(date_sent), MAX(date_sent) FROM emails").fetchone()
    stats["date_min"] = date_range[0] or "N/A"
    stats["date_max"] = date_range[1] or "N/A"
    conn.close()
    return stats


# ---------------------------------------------------------------------------
# Previous Close Helper (for Market Data page change calculation)
# ---------------------------------------------------------------------------

def _get_prev_closes(tickers: list[str]) -> dict[str, float]:
    """Fetch previous session close prices (cached once per day via yfinance).

    Thread-safe for concurrent Dash callbacks — the fetch happens under
    the lock only on a cache miss, and the happy path returns a defensive
    copy without touching the network.
    """
    global _prev_close_cache, _prev_close_date
    from datetime import date
    today = date.today().isoformat()

    # Fast path: hot cache, no lock needed for the happy case because dict
    # reads are atomic under CPython — but we still copy under lock to
    # avoid a rare torn read during an in-progress update.
    with _prev_close_lock:
        if _prev_close_date == today and _prev_close_cache:
            return dict(_prev_close_cache)

    # Cache miss — fetch outside the lock (network call), then acquire.
    result: dict[str, float] = {}
    try:
        import yfinance as yf
        data = yf.download(tickers, period="5d", progress=False, threads=True)
        if data is not None and not data.empty:
            closes = data["Close"]
            prev_data = closes[closes.index.strftime("%Y-%m-%d") < today]
            if not prev_data.empty:
                last_row = prev_data.iloc[-1]
                for t in tickers:
                    col = t if t in last_row.index else None
                    if col and not math.isnan(last_row[col]):
                        result[t] = float(last_row[col])
    except Exception as e:
        log.warning("Failed to fetch prev closes via yfinance: %s", e)

    with _prev_close_lock:
        if result:
            _prev_close_cache = result
            _prev_close_date = today
        return dict(_prev_close_cache)
