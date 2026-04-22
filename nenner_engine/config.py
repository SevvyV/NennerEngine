"""Centralized configuration — values shared across multiple modules."""

import os as _os
from pathlib import Path as _Path

# ── Project Layout ──────────────────────────────────────────────
PROJECT_ROOT = _Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = str(_Path(r"E:\Workspace\DataCenter\nenner_signals.db"))


# ── Environment Loading ─────────────────────────────────────────
# Single canonical .env loader. Replaces the three near-duplicates that
# previously lived in alert_dispatch.py, imap_client.py, and llm_parser.py.
_ENV_LOADED = False


def load_env_once() -> None:
    """Populate os.environ from the project's .env file (first call only).

    Safe to call from multiple modules and multiple threads — the actual
    file read happens once per process. Uses setdefault() so real environment
    variables always win over .env values.
    """
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    candidates = [
        _Path(_os.getcwd()) / ".env",
        PROJECT_ROOT / ".env",
    ]
    for env_path in candidates:
        if not env_path.exists():
            continue
        try:
            with open(env_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, val = line.split("=", 1)
                    # Strip surrounding quotes — the old equity_stream
                    # inline loader did this, and we don't want to lose
                    # that forgiveness now that we've unified on this.
                    val = val.strip().strip('"').strip("'")
                    _os.environ.setdefault(key.strip(), val)
        except OSError:
            continue
        break

    _ENV_LOADED = True

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
LLM_MODEL = "claude-opus-4-6"
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
