"""
Alert Engine
=============
Monitors signal state and price data, fires notifications when
alert conditions are met. Supports Windows toast and Telegram channels.

Usage:
    python -m nenner_engine --monitor
    python -m nenner_engine --monitor --interval 30
    python -m nenner_engine --alert-history
"""

import json
import logging
import os
import signal as signal_mod  # avoid shadowing Nenner "signal" terminology
import sqlite3
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("nenner")

# ---------------------------------------------------------------------------
# Alert Thresholds
# ---------------------------------------------------------------------------

PROXIMITY_DANGER_PCT = 0.5    # Cancel/trigger distance < 0.5% = DANGER
PROXIMITY_WARNING_PCT = 1.0   # Cancel/trigger distance < 1.0% = WATCH
ALERT_COOLDOWN_MINUTES = 60   # 1 hour per ticker per alert_type

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
# Alert Condition Evaluation
# ---------------------------------------------------------------------------

def _make_alert(row: dict, alert_type: str, severity: str, message: str) -> dict:
    """Build a standardized alert dict from a price context row."""
    return {
        "ticker": row["ticker"],
        "instrument": row.get("instrument", row["ticker"]),
        "alert_type": alert_type,
        "severity": severity,
        "message": message,
        "current_price": row.get("price"),
        "cancel_dist_pct": row.get("cancel_dist_pct"),
        "trigger_dist_pct": row.get("trigger_dist_pct"),
        "effective_signal": row.get("effective_signal"),
    }


def evaluate_price_alerts(rows: list[dict]) -> list[dict]:
    """Evaluate cancel and trigger proximity alerts.

    Args:
        rows: Output of get_prices_with_signal_context().

    Returns:
        List of alert dicts.
    """
    alerts = []

    for r in rows:
        ticker = r["ticker"]
        instrument = r.get("instrument", ticker)
        price = r.get("price")
        signal = r.get("effective_signal", "")

        if price is None:
            continue

        # --- Cancel proximity ---
        cancel_dist = r.get("cancel_dist_pct")
        if cancel_dist is not None:
            abs_dist = abs(cancel_dist)
            cancel_level = r.get("cancel_level")
            cancel_str = f"{cancel_level:,.2f}" if cancel_level else "?"

            if abs_dist < PROXIMITY_DANGER_PCT:
                alerts.append(_make_alert(
                    r, "CANCEL_DANGER", "DANGER",
                    f"DANGER {ticker} ({instrument}) cancel {abs_dist:.2f}% away! "
                    f"Price={price:,.2f} Cancel={cancel_str} Signal={signal}"
                ))
            elif abs_dist < PROXIMITY_WARNING_PCT:
                alerts.append(_make_alert(
                    r, "CANCEL_WATCH", "WARNING",
                    f"WATCH {ticker} ({instrument}) cancel {abs_dist:.2f}% away. "
                    f"Price={price:,.2f} Cancel={cancel_str} Signal={signal}"
                ))

        # --- Trigger proximity ---
        trigger_dist = r.get("trigger_dist_pct")
        if trigger_dist is not None:
            abs_dist = abs(trigger_dist)
            trigger_level = r.get("trigger_level")
            trigger_str = f"{trigger_level:,.2f}" if trigger_level else "?"

            if abs_dist < PROXIMITY_DANGER_PCT:
                alerts.append(_make_alert(
                    r, "TRIGGER_DANGER", "DANGER",
                    f"DANGER {ticker} ({instrument}) trigger {abs_dist:.2f}% away! "
                    f"Price={price:,.2f} Trigger={trigger_str}"
                ))
            elif abs_dist < PROXIMITY_WARNING_PCT:
                alerts.append(_make_alert(
                    r, "TRIGGER_WATCH", "WARNING",
                    f"WATCH {ticker} ({instrument}) trigger {abs_dist:.2f}% away. "
                    f"Price={price:,.2f} Trigger={trigger_str}"
                ))

    return alerts


def detect_signal_changes(conn: sqlite3.Connection,
                          last_seen_id: int) -> tuple[list[dict], int]:
    """Detect new signals since last_seen_id.

    Returns:
        (alerts_list, new_max_id)
    """
    rows = conn.execute("""
        SELECT id, date, instrument, ticker, signal_type, signal_status,
               origin_price, cancel_level, trigger_level
        FROM signals
        WHERE id > ?
        ORDER BY id ASC
    """, (last_seen_id,)).fetchall()

    if not rows:
        return [], last_seen_id

    alerts = []
    new_max_id = last_seen_id

    for row in rows:
        new_max_id = max(new_max_id, row["id"])
        ticker = row["ticker"]
        instrument = row["instrument"]
        sig_type = row["signal_type"]
        sig_status = row["signal_status"]
        origin = row["origin_price"]
        cancel = row["cancel_level"]

        if sig_status == "ACTIVE":
            origin_str = f"{origin:,.2f}" if origin else "?"
            cancel_str = f"{cancel:,.2f}" if cancel else "?"
            message = (
                f"NEW SIGNAL {ticker} ({instrument}) {sig_type} activated "
                f"from {origin_str}. Cancel at {cancel_str}"
            )
        elif sig_status == "CANCELLED":
            cancel_str = f"{cancel:,.2f}" if cancel else "?"
            message = (
                f"CANCELLED {ticker} ({instrument}) {sig_type} cancelled "
                f"at {cancel_str}. Implies reversal."
            )
        else:
            message = f"SIGNAL {ticker} ({instrument}) {sig_type} {sig_status}"

        alerts.append({
            "ticker": ticker,
            "instrument": instrument,
            "alert_type": "SIGNAL_CHANGE",
            "severity": "INFO",
            "message": message,
            "current_price": origin,
            "cancel_dist_pct": None,
            "trigger_dist_pct": None,
            "effective_signal": f"{sig_type}_{sig_status}",
        })

    return alerts, new_max_id


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
                   telegram_chat_id: Optional[str] = None) -> bool:
    """Check cooldown, send via all channels, log to DB.

    Returns True if alert was actually sent (not suppressed by cooldown).
    """
    ticker = alert["ticker"]
    alert_type = alert["alert_type"]

    if not is_cooled_down(cooldown_tracker, ticker, alert_type):
        log.debug(f"Cooldown active for {ticker}/{alert_type}, suppressing")
        return False

    channels_sent = []

    # Windows toast
    title = f"Nenner {alert['severity']}: {ticker}"
    if send_toast(title, alert["message"], alert["severity"]):
        channels_sent.append("toast")

    # Telegram
    if telegram_token and telegram_chat_id:
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


# ---------------------------------------------------------------------------
# Alert History Display
# ---------------------------------------------------------------------------

def show_alert_history(conn: sqlite3.Connection, limit: int = 50):
    """Display recent alerts from the alert_log table."""
    rows = conn.execute("""
        SELECT created_at, ticker, instrument, alert_type, severity,
               message, current_price, channels_sent
        FROM alert_log
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,)).fetchall()

    if not rows:
        print("No alerts recorded yet.")
        return

    print(f"\n{'=' * 110}")
    print("  ALERT HISTORY (most recent first)")
    print(f"{'=' * 110}")
    print(f"  {'Time':<20} {'Ticker':<8} {'Type':<16} {'Sev':<8} "
          f"{'Channels':<15} {'Message'}")
    print("  " + "-" * 105)

    for r in rows:
        msg = r["message"]
        if len(msg) > 55:
            msg = msg[:55] + "..."
        print(f"  {r['created_at']:<20} {r['ticker']:<8} {r['alert_type']:<16} "
              f"{r['severity']:<8} {r['channels_sent'] or 'none':<15} {msg}")

    print(f"\n  Showing {len(rows)} entries")


# ---------------------------------------------------------------------------
# Monitor Daemon
# ---------------------------------------------------------------------------

def _get_max_signal_id(conn: sqlite3.Connection) -> int:
    """Get the current max signal id (baseline for change detection)."""
    row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM signals").fetchone()
    return row[0]


def run_monitor(conn: sqlite3.Connection, interval: int = 60):
    """Run the alert monitoring daemon.

    Polls every `interval` seconds, evaluates all alert conditions,
    dispatches notifications, and handles graceful shutdown via Ctrl+C.
    """
    from .prices import get_prices_with_signal_context

    # Graceful shutdown
    shutdown = False

    def handle_sigint(signum, frame):
        nonlocal shutdown
        shutdown = True
        print("\nShutting down alert monitor...")

    signal_mod.signal(signal_mod.SIGINT, handle_sigint)

    # Load Telegram config
    tg_token, tg_chat_id = get_telegram_config()
    if tg_token and tg_chat_id:
        log.info("Telegram notifications enabled")
        send_telegram(
            "<b>NennerEngine</b> alert monitor started.",
            tg_token, tg_chat_id,
        )
    else:
        log.info("Telegram not configured (set TELEGRAM_BOT_TOKEN and "
                 "TELEGRAM_CHAT_ID in .env)")
        print(TELEGRAM_SETUP_GUIDE)

    # Check winotify availability
    try:
        import winotify  # noqa: F401
        log.info("Windows toast notifications enabled")
    except ImportError:
        log.warning("winotify not installed -- pip install winotify "
                    "(toast notifications disabled)")

    # Initialize state
    cooldown_tracker: dict[tuple[str, str], datetime] = {}
    last_signal_id = _get_max_signal_id(conn)

    log.info(f"Alert monitor started. Interval={interval}s, "
             f"baseline signal_id={last_signal_id}")
    log.info(f"Thresholds: DANGER<{PROXIMITY_DANGER_PCT}% "
             f"WATCH<{PROXIMITY_WARNING_PCT}% | "
             f"Cooldown={ALERT_COOLDOWN_MINUTES}min")

    check_count = 0
    total_alerts = 0

    while not shutdown:
        try:
            check_count += 1
            log.info(f"--- Alert check #{check_count} ---")

            # 1. Price-based alerts (cancel/trigger proximity)
            rows = get_prices_with_signal_context(conn, try_t1=True)
            price_alerts = evaluate_price_alerts(rows)

            # 2. Signal state change alerts
            signal_alerts, last_signal_id = detect_signal_changes(
                conn, last_signal_id
            )

            # 3. Dispatch all alerts
            all_alerts = price_alerts + signal_alerts
            fired = 0
            for alert in all_alerts:
                if dispatch_alert(alert, cooldown_tracker, conn,
                                  tg_token, tg_chat_id):
                    fired += 1

            total_alerts += fired
            if fired:
                log.info(f"Fired {fired} alerts this check "
                         f"({total_alerts} total)")
            else:
                log.debug(f"No alerts ({len(rows)} instruments checked)")

        except KeyboardInterrupt:
            break
        except Exception as e:
            log.error(f"Error in alert check: {e}", exc_info=True)

        # Sleep in 1s increments for responsive Ctrl+C
        for _ in range(interval):
            if shutdown:
                break
            time.sleep(1)

    log.info(f"Alert monitor stopped. {check_count} checks, "
             f"{total_alerts} alerts fired.")
    if tg_token and tg_chat_id:
        send_telegram(
            "<b>NennerEngine</b> alert monitor stopped.",
            tg_token, tg_chat_id,
        )
