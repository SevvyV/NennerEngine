"""Signal anomaly detection — catch potential typos before they corrupt state.

Compares incoming signal values (cancel_level, origin_price, trigger_level)
against the last 3 signals for each ticker. Flags deviations > 30% from the
recent median and sends a Telegram alert for human review.

Signals are still stored — this is a warning system, not a gate.
"""

import logging
import sqlite3
from statistics import median as _median

from .alert_dispatch import get_telegram_config, send_telegram

log = logging.getLogger("nenner")

# Fields to check and the deviation threshold (fraction, not percent)
_CHECKED_FIELDS = ["cancel_level", "origin_price", "trigger_level"]
_DEFAULT_THRESHOLD = 0.30  # 30%
_MIN_HISTORY = 2  # need at least 2 prior signals to compare


def check_signal_anomalies(
    conn: sqlite3.Connection,
    signals: list[dict],
    threshold: float = _DEFAULT_THRESHOLD,
) -> list[dict]:
    """Compare incoming signals against recent history and flag outliers.

    Parameters
    ----------
    conn : sqlite3.Connection
        Connection to nenner_signals.db (must have row_factory=sqlite3.Row).
    signals : list[dict]
        Parsed signal dicts from ``parse_email_signals_llm()``.
    threshold : float
        Fractional deviation threshold (0.30 = 30%).

    Returns
    -------
    list[dict]
        One dict per anomaly: ticker, field, incoming, recent_median, pct_diff, raw_text.
    """
    anomalies: list[dict] = []

    for sig in signals:
        ticker = sig.get("ticker")
        if not ticker:
            continue

        # Pull last 3 signals for this ticker (excluding the current batch)
        cur = conn.cursor()
        cur.row_factory = sqlite3.Row
        history = cur.execute(
            "SELECT origin_price, cancel_level, trigger_level "
            "FROM signals WHERE ticker = ? "
            "ORDER BY date DESC, id DESC LIMIT ?",
            (ticker, _MIN_HISTORY + 1),  # +1 in case one row is sparse
        ).fetchall()

        if len(history) < _MIN_HISTORY:
            continue

        for field in _CHECKED_FIELDS:
            incoming = sig.get(field)
            if incoming is None or incoming == 0:
                continue

            recent_values = [
                h[field] for h in history
                if h[field] is not None and h[field] != 0
            ]
            if len(recent_values) < _MIN_HISTORY:
                continue

            med = _median(recent_values)
            if med == 0:
                continue

            pct_diff = abs(incoming - med) / med
            if pct_diff > threshold:
                anomalies.append({
                    "ticker": ticker,
                    "field": field,
                    "incoming": incoming,
                    "recent_median": round(med, 2),
                    "pct_diff": round(pct_diff * 100, 1),
                    "recent_values": [round(v, 2) for v in recent_values],
                    "raw_text": (sig.get("raw_text") or "")[:200],
                })

    return anomalies


def alert_anomalies(anomalies: list[dict]) -> None:
    """Send a Telegram alert summarizing detected anomalies."""
    if not anomalies:
        return

    lines = ["\u26a0\ufe0f <b>Signal Anomaly Detected</b>\n"]
    for a in anomalies:
        lines.append(
            f"<b>{a['ticker']}</b> — {a['field']}\n"
            f"  Incoming: {a['incoming']}\n"
            f"  Recent median: {a['recent_median']} ({a['recent_values']})\n"
            f"  Deviation: {a['pct_diff']}%\n"
            f"  Raw: <i>{a['raw_text'][:120]}</i>\n"
        )
    lines.append(
        "Signal was stored — verify and correct if needed.\n"
        "Teach Stanley: <code>--stanley-teach pattern:...</code>"
    )
    message = "\n".join(lines)

    log.warning(f"Signal anomalies detected: {len(anomalies)} flag(s)")
    for a in anomalies:
        log.warning(
            f"  {a['ticker']} {a['field']}: incoming={a['incoming']}, "
            f"median={a['recent_median']}, deviation={a['pct_diff']}%"
        )

    try:
        token, chat_id = get_telegram_config()
        if token and chat_id:
            send_telegram(message, token, chat_id)
            log.info("Anomaly alert sent via Telegram")
        else:
            log.warning("Telegram not configured — anomaly alert logged only")
    except Exception as e:
        log.error(f"Failed to send anomaly alert: {e}")
