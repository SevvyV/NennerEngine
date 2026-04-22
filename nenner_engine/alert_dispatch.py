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

from .config import load_env_once

log = logging.getLogger("nenner")

ALERT_COOLDOWN_MINUTES = 60


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def get_telegram_config() -> tuple[Optional[str], Optional[str]]:
    """Return (bot_token, chat_id) from env vars, .env file, or Azure Key Vault.

    Returns (None, None) if not configured.
    """
    load_env_once()
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
            chat_secret = os.environ.get("TELEGRAM_CHATID_SECRET", "nenner-engine-chat-id")
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

def send_telegram(message: str, bot_token: str, chat_id: str,
                   log_to_ledger: bool = True) -> bool:
    """Send a Telegram message via Bot API (HTTP POST, stdlib only).

    Args:
        log_to_ledger: If True (default), also write to the central error ledger.
            Set to False for routine notifications that are not system errors.
    """
    if log_to_ledger:
        from nenner_engine.error_ledger import log_alert
        log_alert("NENNER", message)

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
    return send_telegram(msg, token, chat_id, log_to_ledger=False)


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
