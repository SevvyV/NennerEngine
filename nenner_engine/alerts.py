"""
Alert Engine
=============
Monitors signal state and price data, fires notifications when
alert conditions are met. Supports Windows toast and Telegram channels.

Configuration:
  - Channel enable/disable (ENABLE_TOAST, ENABLE_TELEGRAM)
  - Scheduled summary alerts at specific times (SCHEDULED_ALERT_TIMES)
  - Intraday ticker filter (INTRADAY_TICKERS) -- only these tickers
    fire proximity alerts between scheduled summaries
  - All settings are in the AlertConfig class below

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
from datetime import datetime, timedelta, time as dt_time
from typing import Optional

log = logging.getLogger("nenner")

# ---------------------------------------------------------------------------
# Alert Thresholds
# ---------------------------------------------------------------------------

PROXIMITY_DANGER_PCT = 0.5    # Cancel/trigger distance < 0.5% = DANGER
PROXIMITY_WARNING_PCT = 1.0   # Cancel/trigger distance < 1.0% = WATCH
ALERT_COOLDOWN_MINUTES = 60   # 1 hour per ticker per alert_type


# ---------------------------------------------------------------------------
# Alert Configuration
# ---------------------------------------------------------------------------

class AlertConfig:
    """Central configuration for alert channels, schedules, and filters.

    Modify these settings to control notification behavior.
    """

    # --- Channel Enable/Disable ---
    ENABLE_TOAST = False         # Windows toast notifications (with audio) -- DISABLED
    ENABLE_TELEGRAM = True       # Telegram bot notifications -- ENABLED

    # --- Scheduled Summary Alerts ---
    # Full portfolio summary sent via Telegram at these times (24hr format).
    # These fire regardless of ticker filters.
    SCHEDULED_ALERT_TIMES = [
        dt_time(8, 0),           # 8:00 AM  -- pre-market overview
        dt_time(9, 35),          # 9:35 AM  -- 5 min after US open
        dt_time(12, 0),          # 12:00 PM -- midday check
        dt_time(16, 15),         # 4:15 PM  -- post-close summary
    ]

    # Tolerance window: scheduled alert fires if current time is within
    # this many minutes of a scheduled time (to handle polling intervals).
    SCHEDULE_TOLERANCE_MINUTES = 2

    # --- Intraday Alert Ticker Filter ---
    # Between scheduled summaries, ONLY these tickers fire real-time
    # proximity alerts (cancel/trigger distance warnings).
    # This covers: Equity Indices, Equity ETFs, Single Stocks, and SOYB.
    #
    # Asset classes included:
    #   - Equity Index:          ES, NQ, YM, NYFANG, TSX, VIX
    #   - Equity Index (Europe): AEX, DAX, FTSE
    #   - Single Stock:          AAPL, BAC, GOOG, MSFT, NVDA, TSLA
    #   - Volatility:            VIX (already above)
    #   - Agriculture ETF:       SOYB (per user request)
    INTRADAY_TICKERS = {
        # US Equity Indices
        "ES", "NQ", "YM", "NYFANG",
        # International Equity Indices
        "TSX", "AEX", "DAX", "FTSE",
        # Volatility
        "VIX",
        # Single Stocks
        "AAPL", "BAC", "GOOG", "MSFT", "NVDA", "TSLA",
        # Equity ETFs
        "QQQ",
        # Special request: SOYB
        "SOYB",
    }

    # --- Intraday Asset Class Filter (alternative broad filter) ---
    # If a ticker is NOT in INTRADAY_TICKERS, we also check if its
    # asset_class starts with any of these prefixes.
    INTRADAY_ASSET_CLASSES = {
        "Equity Index",
        "Equity Index (Europe)",
        "Single Stock",
        "Volatility",
    }

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
        # REMOVED: TRIGGER_DANGER and TRIGGER_WATCH alerts are no longer useful
        # because average gains per trade are too close to trigger levels.
        # Cancel proximity alerts are retained as they indicate risk of reversal.

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
                   telegram_chat_id: Optional[str] = None,
                   config: Optional[AlertConfig] = None) -> bool:
    """Check cooldown, send via enabled channels, log to DB.

    Respects AlertConfig for channel enable/disable.
    Returns True if alert was actually sent (not suppressed by cooldown).
    """
    if config is None:
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


# ---------------------------------------------------------------------------
# Intraday Ticker Filtering
# ---------------------------------------------------------------------------

def is_intraday_ticker(ticker: str, asset_class: str = "",
                       config: Optional[AlertConfig] = None) -> bool:
    """Check if a ticker qualifies for intraday (between-schedule) alerts.

    Returns True if the ticker is in INTRADAY_TICKERS or its asset_class
    matches INTRADAY_ASSET_CLASSES.
    """
    if config is None:
        config = AlertConfig()

    if ticker in config.INTRADAY_TICKERS:
        return True

    for ac_prefix in config.INTRADAY_ASSET_CLASSES:
        if asset_class.startswith(ac_prefix):
            return True

    return False


# ---------------------------------------------------------------------------
# Scheduled Summary Alerts
# ---------------------------------------------------------------------------

def _is_scheduled_time(now: datetime, config: AlertConfig,
                       fired_times: set) -> bool:
    """Check if current time matches any scheduled alert time.

    Args:
        now: Current datetime.
        config: AlertConfig instance.
        fired_times: Set of already-fired (date, time) tuples to prevent dupes.

    Returns True if we should fire a scheduled summary now.
    """
    current_time = now.time()
    today = now.date()
    tolerance = timedelta(minutes=config.SCHEDULE_TOLERANCE_MINUTES)

    for sched_time in config.SCHEDULED_ALERT_TIMES:
        sched_dt = datetime.combine(today, sched_time)
        diff = abs(now - sched_dt)
        key = (today, sched_time)

        if diff <= tolerance and key not in fired_times:
            return True

    return False


def _get_matching_schedule_time(now: datetime, config: AlertConfig) -> Optional[dt_time]:
    """Return the scheduled time that matches the current time, or None."""
    today = now.date()
    tolerance = timedelta(minutes=config.SCHEDULE_TOLERANCE_MINUTES)

    for sched_time in config.SCHEDULED_ALERT_TIMES:
        sched_dt = datetime.combine(today, sched_time)
        if abs(now - sched_dt) <= tolerance:
            return sched_time
    return None


def build_scheduled_summary(conn: sqlite3.Connection,
                            rows: list[dict]) -> str:
    """Build a comprehensive portfolio summary message for scheduled alerts.

    Args:
        conn: Database connection.
        rows: Output of get_prices_with_signal_context().

    Returns:
        HTML-formatted summary string for Telegram.
    """
    now = datetime.now()
    time_str = now.strftime("%I:%M %p")
    date_str = now.strftime("%b %d, %Y")

    lines = [
        f"<b>NennerEngine Portfolio Summary</b>",
        f"{date_str} at {time_str}",
        "",
    ]

    # Categorize signals
    buy_signals = []
    sell_signals = []
    danger_alerts = []
    watch_alerts = []

    for r in rows:
        ticker = r["ticker"]
        signal = r.get("effective_signal", "")
        price = r.get("price")
        origin = r.get("origin_price")
        cancel_dist = r.get("cancel_dist_pct")
        pnl_pct = r.get("pnl_pct")

        if price is None:
            continue

        entry = {
            "ticker": ticker,
            "instrument": r.get("instrument", ticker),
            "signal": signal,
            "price": price,
            "origin": origin,
            "pnl_pct": pnl_pct,
            "cancel_dist": cancel_dist,
        }

        if signal == "BUY":
            buy_signals.append(entry)
        elif signal == "SELL":
            sell_signals.append(entry)

        # Check proximity
        if cancel_dist is not None:
            abs_dist = abs(cancel_dist)
            if abs_dist < PROXIMITY_DANGER_PCT:
                danger_alerts.append(entry)
            elif abs_dist < PROXIMITY_WARNING_PCT:
                watch_alerts.append(entry)

    # Danger alerts first
    if danger_alerts:
        lines.append("<b>!! DANGER ALERTS !!</b>")
        for a in danger_alerts:
            pnl_str = f"{a['pnl_pct']:+.1f}%" if a["pnl_pct"] is not None else "N/A"
            lines.append(
                f"  {a['ticker']} ({a['instrument']}) - "
                f"Cancel {abs(a['cancel_dist']):.2f}% away | "
                f"P/L: {pnl_str}"
            )
        lines.append("")

    if watch_alerts:
        lines.append("<b>Watch List (approaching cancel):</b>")
        for a in watch_alerts:
            pnl_str = f"{a['pnl_pct']:+.1f}%" if a["pnl_pct"] is not None else "N/A"
            lines.append(
                f"  {a['ticker']} - {abs(a['cancel_dist']):.2f}% to cancel | "
                f"P/L: {pnl_str}"
            )
        lines.append("")

    # BUY signals summary
    lines.append(f"<b>Active BUY Signals ({len(buy_signals)}):</b>")
    buy_signals.sort(key=lambda x: x.get("pnl_pct") or 0, reverse=True)
    for s in buy_signals:
        pnl_str = f"{s['pnl_pct']:+.1f}%" if s["pnl_pct"] is not None else "N/A"
        lines.append(f"  {s['ticker']:8s} {s['price']:>10,.2f}  P/L: {pnl_str}")

    lines.append("")
    lines.append(f"<b>Active SELL Signals ({len(sell_signals)}):</b>")
    sell_signals.sort(key=lambda x: x.get("pnl_pct") or 0, reverse=True)
    for s in sell_signals:
        pnl_str = f"{s['pnl_pct']:+.1f}%" if s["pnl_pct"] is not None else "N/A"
        lines.append(f"  {s['ticker']:8s} {s['price']:>10,.2f}  P/L: {pnl_str}")

    # Winning/losing count
    all_sigs = buy_signals + sell_signals
    winners = sum(1 for s in all_sigs if (s.get("pnl_pct") or 0) > 0)
    losers = sum(1 for s in all_sigs if (s.get("pnl_pct") or 0) < 0)
    lines.append("")
    lines.append(f"Winners: {winners} | Losers: {losers} | "
                 f"Total: {len(all_sigs)}")

    return "\n".join(lines)


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


def run_monitor(conn: sqlite3.Connection, interval: int = 60,
                config: Optional[AlertConfig] = None):
    """Run the alert monitoring daemon.

    Polls every `interval` seconds, evaluates all alert conditions,
    dispatches notifications, and handles graceful shutdown via Ctrl+C.

    Features:
      - Scheduled summary alerts at configured times (sent to all tickers)
      - Intraday proximity alerts filtered to equity/ETF tickers + SOYB
      - Signal change alerts for all tickers (always sent)
      - Channel enable/disable via AlertConfig
      - Background email scheduler (checks on startup + daily 8:00 AM ET)
      - Auto-cancel check at 4:30 PM ET daily
    """
    from .prices import get_prices_with_signal_context

    if config is None:
        config = AlertConfig()

    # Graceful shutdown
    shutdown = False

    def handle_sigint(signum, frame):
        nonlocal shutdown
        shutdown = True
        print("\nShutting down alert monitor...")

    signal_mod.signal(signal_mod.SIGINT, handle_sigint)

    # Load Telegram config
    tg_token, tg_chat_id = get_telegram_config()
    if config.ENABLE_TELEGRAM and tg_token and tg_chat_id:
        log.info("Telegram notifications ENABLED")
        send_telegram(
            "<b>NennerEngine</b> alert monitor started.\n\n"
            f"<b>Channels:</b> Toast={'ON' if config.ENABLE_TOAST else 'OFF'}, "
            f"Telegram=ON\n"
            f"<b>Scheduled alerts:</b> "
            + ", ".join(t.strftime("%I:%M %p") for t in config.SCHEDULED_ALERT_TIMES)
            + "\n"
            f"<b>Intraday tickers:</b> {', '.join(sorted(config.INTRADAY_TICKERS))}",
            tg_token, tg_chat_id,
        )
    elif config.ENABLE_TELEGRAM:
        log.info("Telegram ENABLED but not configured (set TELEGRAM_BOT_TOKEN and "
                 "TELEGRAM_CHAT_ID in .env)")
        print(TELEGRAM_SETUP_GUIDE)
    else:
        log.info("Telegram notifications DISABLED in config")

    if config.ENABLE_TOAST:
        try:
            import winotify  # noqa: F401
            log.info("Windows toast notifications ENABLED")
        except ImportError:
            log.warning("winotify not installed -- pip install winotify "
                        "(toast notifications disabled)")
    else:
        log.info("Windows toast notifications DISABLED in config")

    # Build asset_class lookup from database for intraday filtering
    asset_class_lookup = {}
    try:
        ac_rows = conn.execute(
            "SELECT ticker, asset_class FROM current_state"
        ).fetchall()
        for r in ac_rows:
            asset_class_lookup[r["ticker"]] = r["asset_class"]
    except Exception:
        pass

    # --- Start email scheduler (checks on launch + daily 8 AM ET) ---
    email_sched = None
    try:
        from .email_scheduler import EmailScheduler
        db_path = conn.execute("PRAGMA database_list").fetchone()[2]
        email_sched = EmailScheduler(
            db_path=db_path,
            check_on_start=True,
            daily_check=True,
        )
        email_sched.start()
        log.info("Email scheduler active (startup + daily 8:00 AM ET)")
    except Exception as e:
        log.warning(f"Email scheduler failed to start: {e}")
        log.warning("Monitor will run without automatic email checking")

    # Initialize state
    cooldown_tracker: dict[tuple[str, str], datetime] = {}
    last_signal_id = _get_max_signal_id(conn)
    fired_schedule_times: set[tuple] = set()  # (date, time) of fired summaries

    log.info(f"Alert monitor started. Interval={interval}s, "
             f"baseline signal_id={last_signal_id}")
    log.info(f"Thresholds: DANGER<{PROXIMITY_DANGER_PCT}% "
             f"WATCH<{PROXIMITY_WARNING_PCT}% | "
             f"Cooldown={ALERT_COOLDOWN_MINUTES}min")
    log.info(f"Scheduled times: "
             + ", ".join(t.strftime("%H:%M") for t in config.SCHEDULED_ALERT_TIMES))
    log.info(f"Intraday tickers: {sorted(config.INTRADAY_TICKERS)}")

    check_count = 0
    total_alerts = 0
    auto_cancel_ran_today = None  # Track date of last auto-cancel run

    while not shutdown:
        try:
            check_count += 1
            now = datetime.now()
            log.info(f"--- Alert check #{check_count} at {now.strftime('%H:%M:%S')} ---")

            # 0. Auto-cancel check at 4:30 PM ET (once per day)
            try:
                from zoneinfo import ZoneInfo
                from .auto_cancel import check_auto_cancellations
                now_et = datetime.now(ZoneInfo("US/Eastern"))
                today_str = now_et.strftime("%Y-%m-%d")
                if (now_et.hour >= 16 and now_et.minute >= 30
                        and auto_cancel_ran_today != today_str):
                    log.info("Running daily auto-cancel check (4:30 PM ET)...")
                    cancellations = check_auto_cancellations(conn)
                    auto_cancel_ran_today = today_str
                    for c in cancellations:
                        auto_alert = _make_alert(
                            {"ticker": c["ticker"],
                             "instrument": c["instrument"],
                             "price": c["close_price"],
                             "cancel_dist_pct": None,
                             "trigger_dist_pct": None,
                             "effective_signal": c["new_signal"]},
                            "AUTO_CANCEL", "DANGER",
                            f"AUTO-CANCEL {c['ticker']} ({c['instrument']}): "
                            f"{c['old_signal']} cancelled at {c['close_price']:.2f} "
                            f"(cancel {c['cancel_level']:.2f}). "
                            f"Now {c['new_signal']}."
                        )
                        dispatch_alert(auto_alert, cooldown_tracker, conn,
                                       tg_token, tg_chat_id, config)
                        total_alerts += 1
            except Exception as e:
                log.error(f"Auto-cancel check failed: {e}", exc_info=True)

            # 1. Fetch price context for all instruments
            rows = get_prices_with_signal_context(conn, try_t1=True)

            # 2. Check if this is a scheduled summary time
            sched_time = _get_matching_schedule_time(now, config)
            if sched_time and (now.date(), sched_time) not in fired_schedule_times:
                log.info(f"SCHEDULED SUMMARY firing for {sched_time.strftime('%H:%M')}")

                summary = build_scheduled_summary(conn, rows)

                # Send scheduled summary via Telegram
                if config.ENABLE_TELEGRAM and tg_token and tg_chat_id:
                    send_telegram(summary, tg_token, tg_chat_id)
                    log.info("Scheduled summary sent via Telegram")

                # Mark as fired so it doesn't repeat
                fired_schedule_times.add((now.date(), sched_time))

                # Log to DB as a scheduled alert
                sched_alert = {
                    "ticker": "ALL",
                    "instrument": "Portfolio",
                    "alert_type": "SCHEDULED_SUMMARY",
                    "severity": "INFO",
                    "message": f"Scheduled summary at {sched_time.strftime('%I:%M %p')}",
                    "current_price": None,
                    "cancel_dist_pct": None,
                    "trigger_dist_pct": None,
                    "effective_signal": None,
                }
                log_alert(conn, sched_alert, ["telegram"])
                total_alerts += 1

            # 3. Price-based proximity alerts (filtered to intraday tickers)
            price_alerts = evaluate_price_alerts(rows)

            # Filter: only intraday tickers get proximity alerts between schedules
            filtered_price_alerts = []
            for alert in price_alerts:
                ticker = alert["ticker"]
                ac = asset_class_lookup.get(ticker, "")
                if is_intraday_ticker(ticker, ac, config):
                    filtered_price_alerts.append(alert)
                else:
                    log.debug(f"Suppressed intraday alert for {ticker} "
                              f"(not in intraday filter)")

            # 4. Signal state change alerts (ALWAYS sent for ALL tickers)
            signal_alerts, last_signal_id = detect_signal_changes(
                conn, last_signal_id
            )

            # 5. Enrich alerts with position P/L for held instruments
            all_alerts = filtered_price_alerts + signal_alerts
            try:
                from .positions import (
                    read_positions, compute_position_pnl, get_held_tickers,
                )
                positions = read_positions()
                held = get_held_tickers(positions)
                if held:
                    price_by_ticker = {
                        r["ticker"]: r.get("price")
                        for r in rows if r.get("price")
                    }
                    for alert in all_alerts:
                        tk = alert.get("ticker")
                        if tk in held:
                            pos = next(
                                (p for p in positions if p["underlying"] == tk),
                                None,
                            )
                            cp = price_by_ticker.get(tk) or alert.get("current_price")
                            if pos and cp:
                                pnl = compute_position_pnl(pos, cp)
                                dollar = pnl["total_pnl_dollar"]
                                alert["message"] += (
                                    f" | Position P/L: ${dollar:+,.0f}"
                                )
            except Exception as e:
                log.debug(f"Position enrichment skipped: {e}")

            # 6. Dispatch alerts
            fired = 0
            for alert in all_alerts:
                if dispatch_alert(alert, cooldown_tracker, conn,
                                  tg_token, tg_chat_id, config):
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

    # Stop email scheduler
    if email_sched:
        email_sched.stop()

    log.info(f"Alert monitor stopped. {check_count} checks, "
             f"{total_alerts} alerts fired.")
    if config.ENABLE_TELEGRAM and tg_token and tg_chat_id:
        send_telegram(
            "<b>NennerEngine</b> alert monitor stopped.",
            tg_token, tg_chat_id,
        )
