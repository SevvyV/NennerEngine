"""Central Error Ledger — shared error log across FischerDaily, NennerEngine, DataCenter.

Writes ERROR+ records and retry-related WARNINGs to a single shared log file
at E:\\Workspace\\logs\\error_ledger.log.  Each system attaches this handler
to its root logger during startup.

The handler is additive — it does not replace existing log handlers.
Uses direct file append (not RotatingFileHandler) for multi-process safety.

NOTE: This file is a copy of fischer_daily/core/error_ledger.py.
Keep both copies in sync when modifying.
"""

import logging
import os
import threading
from datetime import datetime

from .tz import ET as _ET

LEDGER_PATH = os.path.join("E:\\Workspace\\logs", "error_ledger.log")
_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
_BACKUP_COUNT = 10

# WARNING messages containing any of these substrings are promoted to the ledger
RETRY_KEYWORDS = frozenset({
    "retry", "retrying", "failed", "failure", "backoff",
    "attempt", "stale", "timeout", "exhausted", "recover",
    "not running", "not responding", "dead", "died",
    "abort", "could not", "unavailable", "not connected",
})

_rotate_lock = threading.Lock()


def _write_line(path: str, line: str):
    """Append a single line to the ledger file, rotating if needed."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        _maybe_rotate(path)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line if line.endswith("\n") else line + "\n")
    except Exception:
        pass  # Never crash the host process


def _maybe_rotate(path: str):
    """Rotate if the file exceeds _MAX_BYTES.  Best-effort, multi-process safe."""
    try:
        if not os.path.exists(path) or os.path.getsize(path) < _MAX_BYTES:
            return
        if not _rotate_lock.acquire(blocking=False):
            return  # Another thread is rotating; skip
        try:
            # Re-check after acquiring lock
            if os.path.getsize(path) < _MAX_BYTES:
                return
            # Shift backups: .10 deleted, .9 → .10, ..., current → .1
            for i in range(_BACKUP_COUNT, 0, -1):
                src = f"{path}.{i}"
                dst = f"{path}.{i + 1}"
                if i == _BACKUP_COUNT and os.path.exists(src):
                    os.remove(src)
                elif os.path.exists(src):
                    os.replace(src, dst)
            os.replace(path, f"{path}.1")
        finally:
            _rotate_lock.release()
    except Exception:
        pass


def _format_component(logger_name: str, package_prefix: str) -> str:
    """Extract meaningful component from logger name.

    'fischer_daily.scheduling.coordinator' → 'scheduling.coordinator'
    'fischer_daily' → 'fischer_daily'
    'nenner' → 'nenner'
    """
    if logger_name.startswith(package_prefix + "."):
        return logger_name[len(package_prefix) + 1:]
    return logger_name


def _format_line(timestamp: datetime, source: str, component: str,
                 severity: str, message: str) -> str:
    """Build a pipe-delimited ledger line, escaping pipes in the message."""
    ts = timestamp.strftime("%Y-%m-%dT%H:%M:%S.") + f"{timestamp.microsecond // 1000:03d}"
    msg = message.replace("\r", "").replace("\n", "\\n").replace("|", "\\|")
    return f"{ts}|{source}|{component}|{severity}|{msg}"


class CentralErrorHandler(logging.Handler):
    """Filters ERROR+ and retry-related WARNINGs to a shared ledger file.

    Format: timestamp|SOURCE|component|SEVERITY|message  (one line per entry)
    Rotation: 5 MB max, 10 backups.  Direct file append for multi-process safety.
    """

    def __init__(self, source: str, path: str = LEDGER_PATH,
                 level: int = logging.WARNING, package_prefix: str = ""):
        super().__init__(level)
        self._source = source
        self._path = path
        self._package_prefix = package_prefix or source.lower()
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def _matches_retry_keyword(self, message: str) -> bool:
        msg_lower = message.lower()
        return any(kw in msg_lower for kw in RETRY_KEYWORDS)

    def filter(self, record: logging.LogRecord) -> bool:
        """Called by the logging framework before emit(). Controls what reaches the ledger."""
        if record.levelno >= logging.ERROR:
            return True
        if record.levelno >= logging.WARNING and self._matches_retry_keyword(
            record.getMessage()
        ):
            return True
        return False

    def emit(self, record: logging.LogRecord):
        try:
            now = datetime.now(_ET)
            component = _format_component(record.name, self._package_prefix)

            msg = record.getMessage()
            if record.exc_info and record.exc_info[1]:
                import traceback
                tb = "".join(traceback.format_exception(*record.exc_info))
                msg = f"{msg} | {tb}"

            line = _format_line(now, self._source, component, record.levelname, msg)
            _write_line(self._path, line)
        except Exception:
            self.handleError(record)


def log_alert(source: str, message: str, path: str = LEDGER_PATH):
    """Write an ALERT-level entry to the ledger (for Telegram hook)."""
    try:
        now = datetime.now(_ET)
        line = _format_line(now, source, "telegram", "ALERT", message)
        _write_line(path, line)
    except Exception:
        pass
