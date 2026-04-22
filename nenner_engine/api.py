"""Signals API — FastAPI server exposing NennerEngine signal data.

FischerDaily consumes this API via its SignalClient, replacing direct
SQLite access to nenner_signals.db.

Usage:
    python -m nenner_engine --api --api-port 8051
"""

import logging
import sqlite3
from contextlib import contextmanager

from fastapi import Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from .signal_queries import (
    get_current_state,
    get_instruments_with_signals,
    get_latest_cycles,
    get_latest_targets,
    get_recent_ntc_count,
    get_signal_history,
    search_signals,
    snapshot_current_state,
)

log = logging.getLogger(__name__)

# Module-level DB path — set by create_app()
_db_path: str = ""


def _get_conn():
    """Yield a read-only SQLite connection per request."""
    conn = sqlite3.connect(_db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
    finally:
        conn.close()


def create_app(db_path: str) -> FastAPI:
    """Create the FastAPI application."""
    global _db_path
    _db_path = db_path

    app = FastAPI(
        title="NennerEngine Signals API",
        description="Read-only API for Nenner signal data",
        version="1.0.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    @app.get("/health")
    def health(conn=Depends(_get_conn)):
        row = conn.execute("SELECT COUNT(*) AS cnt FROM current_state").fetchone()
        return {
            "status": "ok",
            "db": db_path,
            "instruments": row["cnt"] if row else 0,
        }

    # ------------------------------------------------------------------
    # Current State
    # ------------------------------------------------------------------

    @app.get("/signals/current-state")
    def current_state(
        tickers: str | None = Query(None, description="Comma-separated ticker filter"),
        conn=Depends(_get_conn),
    ):
        ticker_list = [t.strip() for t in tickers.split(",")] if tickers else None
        return {"data": get_current_state(conn, ticker_list)}

    # ------------------------------------------------------------------
    # Signal History
    # ------------------------------------------------------------------

    @app.get("/signals/history/{ticker}")
    def signal_history(
        ticker: str,
        limit: int = Query(10, ge=1, le=500),
        conn=Depends(_get_conn),
    ):
        return {"data": get_signal_history(conn, ticker, limit)}

    # ------------------------------------------------------------------
    # Cycles
    # ------------------------------------------------------------------

    @app.get("/signals/cycles/{ticker}")
    def cycles(
        ticker: str,
        limit: int = Query(6, ge=1, le=100),
        conn=Depends(_get_conn),
    ):
        return {"data": get_latest_cycles(conn, ticker, limit)}

    # ------------------------------------------------------------------
    # Price Targets
    # ------------------------------------------------------------------

    @app.get("/signals/targets/{ticker}")
    def targets(ticker: str, conn=Depends(_get_conn)):
        return {"data": get_latest_targets(conn, ticker)}

    # ------------------------------------------------------------------
    # Note-the-Change Count
    # ------------------------------------------------------------------

    @app.get("/signals/ntc-count/{ticker}")
    def ntc_count(
        ticker: str,
        days: int = Query(30, ge=1, le=365),
        conn=Depends(_get_conn),
    ):
        return {"count": get_recent_ntc_count(conn, ticker, days)}

    # ------------------------------------------------------------------
    # Snapshot (full current_state as dict)
    # ------------------------------------------------------------------

    @app.get("/signals/snapshot")
    def snapshot(conn=Depends(_get_conn)):
        return {"data": snapshot_current_state(conn)}

    # ------------------------------------------------------------------
    # Instruments List
    # ------------------------------------------------------------------

    @app.get("/signals/instruments")
    def instruments(conn=Depends(_get_conn)):
        return {"data": get_instruments_with_signals(conn)}

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    @app.get("/signals/search")
    def search(
        pattern: str = Query(..., min_length=1),
        limit: int = Query(50, ge=1, le=500),
        conn=Depends(_get_conn),
    ):
        return {"data": search_signals(conn, pattern, limit)}

    return app
