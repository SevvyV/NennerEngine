"""Centralized configuration — values shared across multiple modules."""

from pathlib import Path as _Path

# ── Project Layout ──────────────────────────────────────────────
PROJECT_ROOT = _Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = str(_Path(r"E:\Workspace\DataCenter\nenner_signals.db"))
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
DAILY_CHECK_HOUR, DAILY_CHECK_MINUTE = 8, 35
INTERVAL_WINDOW_START = 8
INTERVAL_WINDOW_END = 11
STOCK_REPORT_HOUR, STOCK_REPORT_MINUTE = 8, 30
AUTO_CANCEL_HOUR, AUTO_CANCEL_MINUTE = 16, 30
SCHEDULER_TICK_SECONDS = 30

# ── Nenner Watchdog ───────────────────────────────────────────
# Nenner sends emails Mon/Wed/Fri. Alert if none parsed by noon ET.
NENNER_EXPECTED_DAYS = {0, 2, 4}  # Monday, Wednesday, Friday
WATCHDOG_HOUR, WATCHDOG_MINUTE = 12, 0
