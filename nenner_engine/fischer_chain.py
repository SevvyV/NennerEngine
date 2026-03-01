"""
Fischer Chain Reader — Live option chain data from Thomson One
===============================================================
Spec reference: Fischer_Agent_Specification_v2.md §4, §5

Reads the Options_RT sheet from Nenner_DataCenter.xlsm via a subprocess
to isolate COM interactions. Output goes to a temp file (not pipes) to
avoid handle inheritance issues from the MCP server's JSON-RPC transport.

Auto-switches ticker and strike increment in the sheet — no manual input.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

log = logging.getLogger("fischer")

# Paths to subprocess helpers (same directory as this file)
_XL_READER = str(Path(__file__).parent / "_xl_reader.py")
_POS_READER = str(Path(__file__).parent / "_pos_reader.py")

# Python executable — use the same one running this process
_PYTHON = sys.executable


class StaleChainError(Exception):
    """Raised when chain data is unavailable or stale."""


@dataclass
class ChainMeta:
    """Metadata about the option chain read."""
    ticker: str
    spot: float
    rate: float
    div_yield: float
    source: str        # "LIVE" or "DELAYED"
    timestamp: datetime
    expiries: list[date]
    stale_warning: str | None = None


# Standard DataFrame columns
CHAIN_COLUMNS = [
    "expiry",      # date
    "strike",      # float
    "type",        # "P" or "C"
    "bid",         # float
    "ask",         # float
    "last",        # float
    "oi",          # int
    "volume",      # int
]


# ---------------------------------------------------------------------------
# In-memory cache (avoids redundant reads within the same scan)
# ---------------------------------------------------------------------------
_chain_cache: dict[str, tuple[pd.DataFrame, ChainMeta, float]] = {}
_CACHE_TTL_SECONDS = 30


# ---------------------------------------------------------------------------
# Live reader via subprocess (temp file output to avoid pipe issues)
# ---------------------------------------------------------------------------

def read_chain(
    ticker: str,
    timeout_seconds: int = 30,
    workbook_name: str = "Nenner_DataCenter.xlsm",
    sheet_name: str = "Options_RT",
) -> tuple[pd.DataFrame, ChainMeta]:
    """Read option chain from Thomson One via Options_RT sheet.

    Runs the Excel/COM interaction in a subprocess with a hard timeout.
    Output is written to a temp file to avoid pipe inheritance issues
    when called from the MCP server's JSON-RPC process.
    """
    cache_key = ticker.upper()

    # Check cache first
    if cache_key in _chain_cache:
        df, meta, ts = _chain_cache[cache_key]
        if time.time() - ts < _CACHE_TTL_SECONDS:
            log.info("Chain cache hit for %s (age %.0fs)", cache_key, time.time() - ts)
            return df, meta
        else:
            del _chain_cache[cache_key]

    # Create temp file for output (avoids pipe/handle inheritance)
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json", prefix="fischer_")
    os.close(tmp_fd)  # close the fd; subprocess will write by path

    log.info("Reading chain for %s via subprocess (timeout=%ds)", ticker, timeout_seconds)
    try:
        # Pass the temp file path as the 4th argument
        proc = subprocess.Popen(
            [_PYTHON, _XL_READER, ticker.upper(), workbook_name, sheet_name, tmp_path],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        _cleanup(tmp_path)
        raise StaleChainError(
            f"Excel COM timed out after {timeout_seconds}s — "
            f"Excel may be busy with RTD refreshes. "
            f"Try again in a few seconds."
        )
    except FileNotFoundError:
        _cleanup(tmp_path)
        raise StaleChainError(f"Cannot find _xl_reader.py at {_XL_READER}")

    # Read output from temp file
    try:
        raw = Path(tmp_path).read_text(encoding="utf-8").strip()
    except Exception as e:
        _cleanup(tmp_path)
        raise StaleChainError(f"Cannot read subprocess output: {e}")
    finally:
        _cleanup(tmp_path)

    if not raw:
        raise StaleChainError("Excel reader returned no output")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raise StaleChainError(f"Excel reader returned invalid JSON: {raw[:200]}")

    if not data.get("ok"):
        raise StaleChainError(f"Excel reader error: {data.get('error', 'unknown')}")

    # Build DataFrame
    rows = data["rows"]
    if not rows:
        raise StaleChainError(f"No option data found for {ticker} in Options_RT")

    for row in rows:
        row["expiry"] = date.fromisoformat(row["expiry"])

    chain_df = pd.DataFrame(rows, columns=CHAIN_COLUMNS)
    chain_df = chain_df.sort_values(["type", "expiry", "strike"]).reset_index(drop=True)

    expiry_dates = [date.fromisoformat(e) for e in data["expiries"]]

    meta = ChainMeta(
        ticker=data["ticker"],
        spot=data["spot"],
        rate=data["rate"],
        div_yield=data["div_yield"],
        source="LIVE",
        timestamp=datetime.now(),
        expiries=expiry_dates,
    )

    if data.get("switched"):
        log.info("Options_RT auto-switched to %s", ticker)

    _chain_cache[cache_key] = (chain_df, meta, time.time())
    return chain_df, meta


def _cleanup(path: str) -> None:
    """Remove temp file silently."""
    try:
        os.unlink(path)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Position reader — reads open positions from Nenner_Positions.xlsm
# ---------------------------------------------------------------------------

@dataclass
class PositionInfo:
    """Info about an open position from Nenner_Positions.xlsm."""
    ticker: str
    sheet: str
    entry_price: float
    direction: str       # "SHORT" or "LONG"
    shares: int
    intent: str           # "covered_put" or "covered_call"


def read_position(
    ticker: str,
    timeout_seconds: int = 10,
    workbook_name: str = "Nenner_Positions.xlsm",
) -> PositionInfo | None:
    """Read open position for ticker from Nenner_Positions.xlsm.

    Returns PositionInfo if a matching position is found, None otherwise.
    Runs in a subprocess to isolate COM interactions.
    """
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json", prefix="fischer_pos_")
    os.close(tmp_fd)

    log.info("Reading position for %s from %s", ticker, workbook_name)
    try:
        proc = subprocess.Popen(
            [_PYTHON, _POS_READER, ticker.upper(), workbook_name, tmp_path],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        _cleanup(tmp_path)
        log.warning("Position reader timed out for %s", ticker)
        return None
    except FileNotFoundError:
        _cleanup(tmp_path)
        log.warning("Cannot find _pos_reader.py at %s", _POS_READER)
        return None

    try:
        raw = Path(tmp_path).read_text(encoding="utf-8").strip()
    except Exception:
        _cleanup(tmp_path)
        return None
    finally:
        _cleanup(tmp_path)

    if not raw:
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None

    if not data.get("ok") or not data.get("found"):
        return None

    return PositionInfo(
        ticker=data["ticker"],
        sheet=data["sheet"],
        entry_price=data["entry_price"],
        direction=data["direction"],
        shares=data["shares"],
        intent=data["intent"],
    )
