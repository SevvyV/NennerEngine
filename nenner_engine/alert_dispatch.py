"""
Alert Dispatch — Notification Delivery Infrastructure
======================================================
Handles HOW alerts are delivered: Telegram, Windows toast, DB logging,
and credential loading. Extracted from alerts.py to separate delivery
infrastructure from evaluation logic.

All public names are re-exported through alerts.py so existing imports
from ``nenner_engine.alerts`` continue to work.
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

# Duplicated here to avoid circular import (alerts → alert_dispatch → alerts).
# The canonical value lives in alerts.py as well.
ALERT_COOLDOWN_MINUTES = 60


# ---------------------------------------------------------------------------
# Environment / Credentials
# ---------------------------------------------------------------------------

def _load_env():
    """Load .env file into os.environ (same pattern as imap_client.get_credentials)."""
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

    Lookup order:
      1. Environment variables / .env file (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
      2. Azure Key Vault (using AZURE_KEYVAULT_URL + secret names)

    Returns (None, None) if neither source is configured.
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

TELEGRAM_SETUP_GUIDE = """
  Telegram Bot Setup:
  1. Open Telegram and message @BotFather
  2. Send /newbot, name it "NennerEngine Alerts"
  3. Copy the bot token -> TELEGRAM_BOT_TOKEN in .env
  4. Message your new bot (send any text)
  5. Visit https://api.telegram.org/bot<TOKEN>/getUpdates
  6. Find "chat":{"id": NNNNN} in the JSON -> TELEGRAM_CHAT_ID in .env
"""


def send_toast(title: str, message: str, severity: str = "INFO") -> bool:
    """Send a Windows 10 toast notification via winotify.

    Returns True if sent, False if winotify unavailable or error.
    """
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
    """Send a Telegram alert when a Fischer Refresh email is received.

    Returns True if sent, False if Telegram is not configured or send failed.
    """
    token, chat_id = get_telegram_config()
    if not token or not chat_id:
        log.debug("Fischer refresh notification skipped — Telegram not configured")
        return False
    msg = f"<b>Fischer Refresh</b>\n{sender_email} requested a refresh of the Fischer Daily Report"
    return send_telegram(msg, token, chat_id)


def send_telegram(message: str, bot_token: str, chat_id: str) -> bool:
    """Send a Telegram message via Bot API (HTTP POST, stdlib only).

    Returns True if sent successfully.
    """
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


# ---------------------------------------------------------------------------
# Cooldown & Persistence
# ---------------------------------------------------------------------------

def is_cooled_down(cooldown_tracker: dict, ticker: str, alert_type: str,
                   cooldown_minutes: int = ALERT_COOLDOWN_MINUTES) -> bool:
    """Check if enough time has elapsed since last alert for this ticker+type.

    Returns True if the alert should fire (cooldown elapsed or first time).
    """
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


def dispatch_alert(alert: dict, cooldown_tracker: dict,
                   conn: sqlite3.Connection,
                   telegram_token: Optional[str] = None,
                   telegram_chat_id: Optional[str] = None,
                   config=None) -> bool:
    """Check cooldown, send via enabled channels, log to DB.

    Respects AlertConfig for channel enable/disable.
    Returns True if alert was actually sent (not suppressed by cooldown).
    """
    if config is None:
        from .alerts import AlertConfig
        config = AlertConfig()

    ticker = alert["ticker"]
    alert_type = alert["alert_type"]

    if not is_cooled_down(cooldown_tracker, ticker, alert_type):
        log.debug(f"Cooldown active for {ticker}/{alert_type}, suppressing")
        return False

    channels_sent = []

    # Windows toast -- only if enabled in config
    if config.ENABLE_TOAST:
        title = f"Nenner {alert['severity']}: {ticker}"
        if send_toast(title, alert["message"], alert["severity"]):
            channels_sent.append("toast")

    # Telegram -- only if enabled in config
    if config.ENABLE_TELEGRAM and telegram_token and telegram_chat_id:
        tg_msg = f"<b>{alert['severity']}</b>: {alert['message']}"
        if send_telegram(tg_msg, telegram_token, telegram_chat_id):
            channels_sent.append("telegram")

    # Log to DB
    log_alert(conn, alert, channels_sent)

    # Update cooldown tracker
    cooldown_tracker[(ticker, alert_type)] = datetime.now()

    # Console output
    channels_str = ",".join(channels_sent) or "console-only"
    log.info(f"ALERT [{alert['severity']}] {alert['message']} -> {channels_str}")

    return True
