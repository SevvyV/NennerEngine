"""Centralized configuration — values shared across multiple modules."""

from pathlib import Path as _Path

# ── Project Layout ──────────────────────────────────────────────
PROJECT_ROOT = _Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = str(PROJECT_ROOT / "nenner_signals.db")
FISCHER_DEBUG_DB = str(PROJECT_ROOT / "fischer_scan_debug.db")
T1_WORKBOOK = r"E:\Workspace\DataCenter\Nenner_DataCenter.xlsm"

# ── Email Recipients ────────────────────────────────────────────
REPORT_RECIPIENT = "sevagv@vartaniancapital.com"
ADMIN_EMAIL = "sevagshop@gmail.com"

# ── Mail Servers ────────────────────────────────────────────────
IMAP_SERVER = "imap.gmail.com"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_TIMEOUT = 30
NENNER_SENDER = "newsletter@charlesnenner.com"

# ── LLM ─────────────────────────────────────────────────────────
LLM_MODEL = "claude-sonnet-4-5-20250929"
LLM_MAX_TOKENS_REPORT = 2048
LLM_MAX_TOKENS_STANLEY = 4096
LLM_RETRY_ATTEMPTS = 2

# ── Schedule (Eastern Time) ────────────────────────────────────
DAILY_CHECK_HOUR, DAILY_CHECK_MINUTE = 8, 0
INTERVAL_WINDOW_START = 8
INTERVAL_WINDOW_END = 11
STOCK_REPORT_HOUR, STOCK_REPORT_MINUTE = 7, 0
AUTO_CANCEL_HOUR, AUTO_CANCEL_MINUTE = 16, 30
FISCHER_SCAN_SCHEDULE: list[tuple[int, int, str]] = [
    (9, 45, "opening"),
]
SCHEDULER_TICK_SECONDS = 30
