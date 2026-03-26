"""Signal query functions — server-side source of truth.

These queries are served by the FastAPI signals API (api.py).
FischerDaily consumes them via HTTP through its SignalClient.
"""

import sqlite3


def get_current_state(
    conn: sqlite3.Connection,
    tickers: list[str] | None = None,
) -> list[dict]:
    """Get current signal state, optionally filtered by tickers."""
    if tickers:
        placeholders = ",".join("?" for _ in tickers)
        rows = conn.execute(
            f"SELECT * FROM current_state WHERE ticker IN ({placeholders}) "
            "ORDER BY asset_class, instrument, ticker",
            tickers,
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM current_state ORDER BY asset_class, instrument, ticker"
        ).fetchall()
    return [dict(r) for r in rows]


def get_signal_history(
    conn: sqlite3.Connection,
    ticker: str,
    limit: int = 10,
) -> list[dict]:
    """Get recent signal history for a ticker."""
    rows = conn.execute(
        "SELECT * FROM signals WHERE ticker = ? "
        "ORDER BY date DESC, id DESC LIMIT ?",
        (ticker, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_latest_cycles(
    conn: sqlite3.Connection,
    ticker: str,
    limit: int = 6,
) -> list[dict]:
    """Get latest cycle entries for a ticker."""
    rows = conn.execute(
        "SELECT * FROM cycles WHERE ticker = ? "
        "ORDER BY date DESC, id DESC LIMIT ?",
        (ticker, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_latest_targets(
    conn: sqlite3.Connection,
    ticker: str,
) -> list[dict]:
    """Get active (unreached) price targets that align with current signal."""
    rows = conn.execute(
        "WITH recent_emails AS ("
        "  SELECT id FROM emails ORDER BY date_sent DESC, id DESC LIMIT 5"
        ") "
        "SELECT pt.* FROM price_targets pt "
        "JOIN current_state cs ON cs.ticker = pt.ticker "
        "WHERE pt.ticker = ? AND pt.reached = 0 "
        "  AND pt.date >= cs.last_signal_date "
        "  AND CASE "
        "       WHEN cs.effective_signal = 'SELL' THEN pt.direction IN ('DOWNSIDE', 'downside') "
        "       WHEN cs.effective_signal = 'BUY'  THEN pt.direction IN ('UPSIDE', 'upside') "
        "       ELSE 0 END "
        "  AND EXISTS ("
        "    SELECT 1 FROM price_targets pt2"
        "    WHERE pt2.ticker = pt.ticker"
        "      AND pt2.email_id IN (SELECT id FROM recent_emails)"
        "  ) "
        "ORDER BY pt.date DESC",
        (ticker,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_recent_ntc_count(
    conn: sqlite3.Connection,
    ticker: str,
    days: int = 30,
) -> int:
    """Count 'note the change' signals in the last N days."""
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM signals "
        "WHERE ticker = ? AND note_the_change = 1 "
        f"AND date >= date('now', '-{days} days')",
        (ticker,),
    ).fetchone()
    return row["cnt"] if row else 0


def snapshot_current_state(conn: sqlite3.Connection) -> dict[str, dict]:
    """Take a snapshot of all current states as a dict keyed by ticker."""
    rows = get_current_state(conn)
    result = {}
    for row in rows:
        result[row["ticker"]] = {
            "effective_signal": row["effective_signal"],
            "effective_status": row["effective_status"],
            "origin_price": row["origin_price"],
            "cancel_level": row["cancel_level"],
            "implied_reversal": row["implied_reversal"],
            "last_signal_date": row["last_signal_date"],
        }
    return result


def get_instruments_with_signals(conn: sqlite3.Connection) -> list[str]:
    """Get all tickers that have at least one signal."""
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM signals ORDER BY ticker"
    ).fetchall()
    return [row["ticker"] for row in rows]


def search_signals(
    conn: sqlite3.Connection,
    pattern: str,
    limit: int = 50,
) -> list[dict]:
    """Search signals by instrument or ticker name pattern."""
    rows = conn.execute(
        "SELECT date, instrument, ticker, signal_type, signal_status, "
        "origin_price, cancel_direction, cancel_level, "
        "trigger_direction, trigger_level, note_the_change, uses_hourly_close "
        "FROM signals "
        "WHERE instrument LIKE ? OR ticker LIKE ? "
        "ORDER BY date DESC, id DESC LIMIT ?",
        (f"%{pattern}%", f"%{pattern}%", limit),
    ).fetchall()
    return [dict(r) for r in rows]
