"""
Alert Dispatch — Notification Delivery Infrastructure
======================================================
Handles HOW alerts are delivered: Telegram, Windows toast, DB logging,
and credential loading.

Telegram is used exclusively for Fischer refresh notifications.
Toast is available for the alert monitor when enabled.
"""

import json
import logging
import os
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("nenner")

ALERT_COOLDOWN_MINUTES = 60


# ---------------------------------------------------------------------------
# Environment / Credentials
# ---------------------------------------------------------------------------

def _load_env():
    """Load .env file into os.environ."""
    for search_dir in [os.getcwd(),
                       os.path.dirname(os.path.dirname(os.path.abspath(__file__)))]:
        env_path = os.path.join(search_dir, ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        key, val = line.split("=", 1)
                        os.environ.setdefault(key.strip(), val.strip())
            break


def get_telegram_config() -> tuple[Optional[str], Optional[str]]:
    """Return (bot_token, chat_id) from env vars, .env file, or Azure Key Vault.

    Returns (None, None) if not configured.
    """
    _load_env()
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if token and chat_id:
        return token, chat_id

    # Try Azure Key Vault
    vault_url = os.environ.get("AZURE_KEYVAULT_URL")
    if vault_url:
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vault_url, credential=credential)
            token_secret = os.environ.get("TELEGRAM_TOKEN_SECRET", "Telegram-NennerBot")
            chat_secret = os.environ.get("TELEGRAM_CHATID_SECRET", "telegram-chat-id")
            token = client.get_secret(token_secret).value
            chat_id = client.get_secret(chat_secret).value
            if token and chat_id:
                return token, chat_id
        except Exception as e:
            log.error(f"Azure Key Vault error (Telegram): {e}")

    return None, None


# ---------------------------------------------------------------------------
# Notification Channels
# ---------------------------------------------------------------------------

def send_telegram(message: str, bot_token: str, chat_id: str) -> bool:
    """Send a Telegram message via Bot API (HTTP POST, stdlib only)."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
    }).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                return True
            log.error(f"Telegram API error: {result}")
            return False
    except Exception as e:
        log.error(f"Telegram send failed: {e}")
        return False


def send_toast(title: str, message: str, severity: str = "INFO") -> bool:
    """Send a Windows 10 toast notification via winotify."""
    try:
        from winotify import Notification, audio
        toast = Notification(
            app_id="NennerEngine",
            title=title,
            msg=message,
            duration="long",
        )
        if severity == "DANGER":
            toast.set_audio(audio.LoopingAlarm, loop=False)
        else:
            toast.set_audio(audio.Default, loop=False)
        toast.show()
        return True
    except ImportError:
        log.warning("winotify not installed -- pip install winotify")
        return False
    except Exception as e:
        log.error(f"Toast notification failed: {e}")
        return False


def notify_fischer_refresh(sender_email: str) -> bool:
    """Send a Telegram alert when a Fischer Refresh email is received."""
    token, chat_id = get_telegram_config()
    if not token or not chat_id:
        log.debug("Fischer refresh notification skipped — Telegram not configured")
        return False
    msg = f"<b>Fischer Refresh</b>\n{sender_email} requested a refresh of the Fischer Daily Report"
    return send_telegram(msg, token, chat_id)


# ---------------------------------------------------------------------------
# Cooldown & Persistence
# ---------------------------------------------------------------------------

def is_cooled_down(cooldown_tracker: dict, ticker: str, alert_type: str,
                   cooldown_minutes: int = ALERT_COOLDOWN_MINUTES) -> bool:
    """Check if enough time has elapsed since last alert for this ticker+type."""
    key = (ticker, alert_type)
    last_fired = cooldown_tracker.get(key)
    if last_fired is None:
        return True
    return datetime.now() - last_fired >= timedelta(minutes=cooldown_minutes)


def log_alert(conn: sqlite3.Connection, alert: dict, channels: list[str]):
    """Persist alert to alert_log table."""
    conn.execute("""
        INSERT INTO alert_log (ticker, instrument, alert_type, severity, message,
                               current_price, cancel_dist_pct, trigger_dist_pct,
                               effective_signal, channels_sent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        alert["ticker"], alert["instrument"], alert["alert_type"],
        alert["severity"], alert["message"],
        alert["current_price"], alert.get("cancel_dist_pct"),
        alert.get("trigger_dist_pct"),
        alert.get("effective_signal"),
        ",".join(channels),
    ))
    conn.commit()
