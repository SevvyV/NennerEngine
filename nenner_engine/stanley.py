"""
Stanley — Skills Agent for Interpreted Morning Briefs
======================================================
Generates an LLM-interpreted brief after each Nenner email parse.
Categorizes changes, explains implications, enriches with historical
context and trade statistics, and delivers via Telegram.

Named after Stanley Druckenmiller.
"""

import json
import logging
import sqlite3
import time
from datetime import datetime
from typing import Optional

from .config import LLM_MODEL, LLM_MAX_TOKENS_STANLEY, LLM_RETRY_ATTEMPTS, REPORT_RECIPIENT

log = logging.getLogger("nenner")


# ---------------------------------------------------------------------------
# Knowledge Base CRUD
# ---------------------------------------------------------------------------

def get_knowledge_base(conn: sqlite3.Connection) -> list[dict]:
    """Return all active knowledge rules, ordered by category then created_at."""
    rows = conn.execute(
        "SELECT id, category, instrument, rule_text, confidence, source, created_at "
        "FROM stanley_knowledge WHERE active = 1 "
        "ORDER BY category, created_at"
    ).fetchall()
    return [dict(r) for r in rows]


def add_knowledge(conn: sqlite3.Connection,
                  category: str,
                  rule_text: str,
                  instrument: Optional[str] = None,
                  source: str = "user_correction",
                  confidence: float = 1.0) -> int:
    """Add a new knowledge rule. Returns the new rule ID."""
    cur = conn.execute(
        "INSERT INTO stanley_knowledge (category, instrument, rule_text, confidence, source) "
        "VALUES (?, ?, ?, ?, ?)",
        (category, instrument, rule_text, confidence, source)
    )
    conn.commit()
    return cur.lastrowid


def deactivate_knowledge(conn: sqlite3.Connection, rule_id: int) -> bool:
    """Soft-delete a knowledge rule by setting active=0. Returns True if found."""
    cur = conn.execute(
        "UPDATE stanley_knowledge SET active = 0 WHERE id = ? AND active = 1",
        (rule_id,)
    )
    conn.commit()
    return cur.rowcount > 0


def list_knowledge(conn: sqlite3.Connection) -> list[dict]:
    """Return all knowledge rules (including inactive) for CLI display."""
    rows = conn.execute(
        "SELECT id, category, instrument, rule_text, confidence, source, created_at, active "
        "FROM stanley_knowledge ORDER BY category, id"
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Brief Storage
# ---------------------------------------------------------------------------

def store_brief(conn: sqlite3.Connection, brief_text: str,
                email_id: Optional[int] = None) -> int:
    """Store a generated brief in stanley_briefs table. Returns the brief ID."""
    cur = conn.execute(
        "INSERT INTO stanley_briefs (email_id, brief_text) VALUES (?, ?)",
        (email_id, brief_text)
    )
    conn.commit()
    return cur.lastrowid


def get_latest_brief(conn: sqlite3.Connection) -> Optional[dict]:
    """Retrieve the most recent brief for display."""
    row = conn.execute(
        "SELECT id, email_id, brief_text, created_at FROM stanley_briefs "
        "ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Context Gathering
# ---------------------------------------------------------------------------

def _gather_current_state(conn: sqlite3.Connection) -> list[dict]:
    """Fetch all rows from current_state for the LLM context."""
    rows = conn.execute(
        "SELECT ticker, instrument, asset_class, effective_signal, "
        "origin_price, cancel_direction, cancel_level, implied_reversal, last_signal_date "
        "FROM current_state ORDER BY asset_class, instrument"
    ).fetchall()
    return [dict(r) for r in rows]


def _gather_trade_stats(conn: sqlite3.Connection,
                        tickers: Optional[set] = None) -> dict:
    """Get trade stats, optionally highlighting specific tickers."""
    from .trade_stats import compute_instrument_stats, _risk_flag

    all_stats = compute_instrument_stats(conn, use_cache=True)
    result = {}
    for ticker, s in all_stats.items():
        result[ticker] = {
            "sharpe": round(s["sharpe"], 2),
            "kelly": round(s["kelly"], 3),
            "win_rate": round(s["win_rate"], 1),
            "trades": s["trades"],
            "profit_factor": round(s["profit_factor"], 2),
            "composite": round(s["composite"], 3),
            "risk_flag": _risk_flag(s),
        }
    return result


def _gather_recent_signals(conn: sqlite3.Connection,
                           tickers: set,
                           limit_per_ticker: int = 5) -> dict:
    """Fetch recent signal history for specific tickers."""
    result = {}
    for ticker in tickers:
        rows = conn.execute(
            "SELECT date, signal_type, signal_status, origin_price, "
            "cancel_level, note_the_change "
            "FROM signals WHERE ticker = ? "
            "ORDER BY date DESC, id DESC LIMIT ?",
            (ticker, limit_per_ticker)
        ).fetchall()
        if rows:
            result[ticker] = [dict(r) for r in rows]
    return result


def _gather_cycles(conn: sqlite3.Connection, tickers: set) -> dict:
    """Fetch latest cycle data for specific tickers."""
    result = {}
    for ticker in tickers:
        rows = conn.execute(
            "SELECT timeframe, direction, until_description "
            "FROM cycles WHERE ticker = ? "
            "ORDER BY date DESC, id DESC LIMIT 6",
            (ticker,)
        ).fetchall()
        if rows:
            result[ticker] = [dict(r) for r in rows]
    return result


def _extract_mentioned_tickers(changes: list, parsed_signals: dict) -> set:
    """Extract tickers mentioned in changes and parsed signals."""
    tickers = set()
    for ch in changes:
        tickers.add(ch["ticker"])
    for sig in parsed_signals.get("signals", []):
        if sig.get("ticker"):
            tickers.add(sig["ticker"])
    for cyc in parsed_signals.get("cycles", []):
        if cyc.get("ticker"):
            tickers.add(cyc["ticker"])
    return tickers


# ---------------------------------------------------------------------------
# System Prompt
# ---------------------------------------------------------------------------

STANLEY_SYSTEM_PROMPT = """\
You are Stanley, an expert trading signal interpreter for the Nenner Cycle \
Research system. You produce concise, insightful morning briefs that help a \
trader understand what changed and what it means.

## The Nenner System
- Charles Nenner issues BUY/SELL signals with cancel levels (the price at \
which a signal is invalidated).
- Cancellation implies reversal: if a BUY signal is cancelled, the system \
flips to SELL (and vice versa).
- "note the change" means the cancel level was adjusted from the prior email.
- Signals are evaluated on the daily close (4:15 PM ET), unless "hourly \
close" is specified.
- Trigger levels indicate where the NEXT opposite signal would be initiated \
after a cancellation.
- Cycles (daily, weekly, monthly, dominant) provide timing context but do \
not directly change signals.

## Your Knowledge Base
{knowledge_base}

## Current Portfolio State
{current_state}

## Trade Statistics (historical performance)
{trade_stats}

## Recent Signal History (changed instruments)
{recent_signals}

## Cycle Data (changed instruments)
{cycle_data}

## Output Format
Produce an HTML-formatted brief for Telegram (use <b>, <i> tags only — \
no markdown, no <br> tags, use newlines). Structure:

1. <b>Stanley's Brief</b> with today's date
2. <b>Top 3 Things That Matter Today</b> — numbered list of the most \
important takeaways
3. <b>Categorized Changes</b>:
   - Cancelled SELL → now BUY (bullish reversals)
   - Cancelled BUY → now SELL (bearish reversals)
   - Active signals updated (cancel level changes, new signals)
   - Other observations
4. <b>Per-Instrument Context</b> — for the most significant 3-5 changes, \
add a line about historical performance (win rate, risk flag) and cycle \
alignment
5. <b>Cross-Instrument Observations</b> — correlations, alignments, \
divergences
6. End with: "Anything I'm reading wrong? Teach me with --stanley-teach"

Keep the total brief under 3500 characters. Be opinionated but transparent \
about uncertainty. Use only these emoji: \U0001f7e2 for BUY, \U0001f534 \
for SELL.
"""


def _format_knowledge(knowledge: list) -> str:
    """Format knowledge rules as numbered text for the system prompt."""
    if not knowledge:
        return "(No learned rules yet)"
    lines = []
    for i, rule in enumerate(knowledge, 1):
        instr = f" [{rule['instrument']}]" if rule.get("instrument") else ""
        lines.append(f"{i}. [{rule['category']}]{instr} {rule['rule_text']}")
    return "\n".join(lines)


def _format_current_state(state: list) -> str:
    """Format current state as compact table text."""
    if not state:
        return "(No active signals)"
    lines = ["Ticker | Signal | Origin | Cancel | Implied | Date"]
    for s in state:
        origin = f"{s['origin_price']:.2f}" if s.get("origin_price") else "—"
        cancel = f"{s['cancel_level']:.2f}" if s.get("cancel_level") else "—"
        implied = "yes" if s.get("implied_reversal") else "no"
        lines.append(
            f"{s['ticker']} | {s['effective_signal']} | {origin} | "
            f"{cancel} | {implied} | {s.get('last_signal_date', '—')}"
        )
    return "\n".join(lines)


def _format_trade_stats(stats: dict, highlight: Optional[set] = None) -> str:
    """Format trade stats as compact table text."""
    if not stats:
        return "(No trade statistics available)"
    lines = ["Ticker | Sharpe | WR% | Kelly | PF | Score | Flag"]
    # Show highlighted tickers first, then a summary count
    shown = set()
    if highlight:
        for ticker in sorted(highlight):
            if ticker in stats:
                s = stats[ticker]
                score = int(s["composite"] * 100)
                flag = f" {s['risk_flag']}" if s["risk_flag"] else ""
                lines.append(
                    f"{ticker} | {s['sharpe']:.2f} | {s['win_rate']:.0f}% | "
                    f"{s['kelly']:.3f} | {s['profit_factor']:.2f} | {score}{flag}"
                )
                shown.add(ticker)
    # Add remaining as summary
    remaining = len(stats) - len(shown)
    if remaining > 0:
        lines.append(f"... plus {remaining} other instruments in portfolio")
    return "\n".join(lines)


def _format_recent_signals(signals: dict) -> str:
    """Format recent signals per ticker."""
    if not signals:
        return "(No recent signal history for changed instruments)"
    lines = []
    for ticker, sigs in signals.items():
        lines.append(f"\n{ticker}:")
        for s in sigs:
            origin = f"{s['origin_price']:.2f}" if s.get("origin_price") else "—"
            cancel = f"{s['cancel_level']:.2f}" if s.get("cancel_level") else "—"
            ntc = " (note the change)" if s.get("note_the_change") else ""
            lines.append(
                f"  {s['date']} {s['signal_type']} {s['signal_status']} "
                f"from {origin} cancel {cancel}{ntc}"
            )
    return "\n".join(lines)


def _format_cycles(cycles: dict) -> str:
    """Format cycle data per ticker."""
    if not cycles:
        return "(No cycle data for changed instruments)"
    lines = []
    for ticker, cycs in cycles.items():
        lines.append(f"\n{ticker}:")
        for c in cycs:
            until = c.get("until_description") or "—"
            lines.append(f"  {c['timeframe']} {c['direction']} until {until}")
    return "\n".join(lines)


def _build_stanley_system_prompt(
    knowledge: list,
    current_state: list,
    trade_stats: dict,
    recent_signals: dict,
    cycles: dict,
    mentioned_tickers: Optional[set] = None,
) -> str:
    """Build the full system prompt with injected context."""
    return STANLEY_SYSTEM_PROMPT.format(
        knowledge_base=_format_knowledge(knowledge),
        current_state=_format_current_state(current_state),
        trade_stats=_format_trade_stats(trade_stats, highlight=mentioned_tickers),
        recent_signals=_format_recent_signals(recent_signals),
        cycle_data=_format_cycles(cycles),
    )


# ---------------------------------------------------------------------------
# LLM Call
# ---------------------------------------------------------------------------

def _call_stanley_llm(system_prompt: str,
                      user_message: str,
                      api_key: str,
                      model: str = LLM_MODEL) -> str:
    """Call the Anthropic API for Stanley's interpretation.

    Returns HTML-formatted brief text, or fallback error message.
    """
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)

    last_error = None
    for attempt in range(LLM_RETRY_ATTEMPTS + 1):
        try:
            message = client.messages.create(
                model=model,
                max_tokens=LLM_MAX_TOKENS_STANLEY,
                system=[{
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_message}],
            )

            response_text = message.content[0].text

            # Log cache performance
            if hasattr(message, "usage"):
                usage = message.usage
                cache_read = getattr(usage, "cache_read_input_tokens", 0)
                cache_create = getattr(usage, "cache_creation_input_tokens", 0)
                if cache_read or cache_create:
                    log.debug(f"Stanley cache: read={cache_read}, created={cache_create}")

            return response_text

        except Exception as e:
            last_error = e
            if attempt < LLM_RETRY_ATTEMPTS:
                wait = 2 ** (attempt + 1)
                log.warning(f"Stanley LLM error (attempt {attempt + 1}), "
                            f"retrying in {wait}s: {e}")
                time.sleep(wait)
            else:
                log.error(f"Stanley LLM failed after {LLM_RETRY_ATTEMPTS + 1} "
                          f"attempts: {e}")

    return f"<b>Stanley Brief</b>\n\nFailed to generate brief: {last_error}"


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

def generate_morning_brief(
    conn: sqlite3.Connection,
    raw_email_text: str,
    parsed_signals: dict,
    changes: list,
    db_path: str,
    email_id: Optional[int] = None,
    send_telegram_flag: bool = True,
) -> str:
    """Generate and optionally send Stanley's morning brief.

    This is the primary integration point called from email_scheduler.py.
    """
    from .llm_parser import _get_cached_api_key
    from .alert_dispatch import get_telegram_config, send_telegram

    try:
        api_key = _get_cached_api_key()
    except ValueError as e:
        log.error(f"Stanley cannot generate brief: {e}")
        return ""

    # 1. Determine which tickers need detailed context
    mentioned_tickers = _extract_mentioned_tickers(changes, parsed_signals)

    # 2. Gather all context
    current_state = _gather_current_state(conn)
    trade_stats = _gather_trade_stats(conn, mentioned_tickers)
    recent_signals = _gather_recent_signals(conn, mentioned_tickers)
    cycles = _gather_cycles(conn, mentioned_tickers)
    knowledge = get_knowledge_base(conn)

    # 3. Build system prompt
    system_prompt = _build_stanley_system_prompt(
        knowledge, current_state, trade_stats, recent_signals, cycles,
        mentioned_tickers=mentioned_tickers,
    )

    # 4. Build user message
    changes_text = json.dumps(changes, indent=2, default=str) if changes else "None detected"
    user_message = (
        f"Here is today's Nenner research email:\n\n"
        f"{raw_email_text}\n\n"
        f"---\n\n"
        f"Direction changes detected by the engine:\n"
        f"{changes_text}\n\n"
        f"Please generate the morning brief."
    )

    # 5. Call LLM
    brief = _call_stanley_llm(system_prompt, user_message, api_key)

    # 6. Store brief
    try:
        store_brief(conn, brief, email_id)
    except Exception as e:
        log.error(f"Failed to store Stanley brief: {e}")

    # 7. Send via Telegram (respects AlertConfig.ENABLE_TELEGRAM)
    if send_telegram_flag:
        from .alerts import AlertConfig
        config = AlertConfig()
        if not config.ENABLE_STANLEY_BRIEF:
            log.info("Stanley brief: ENABLE_STANLEY_BRIEF is False, skipping Telegram send")
        else:
            try:
                token, chat_id = get_telegram_config()
                if token and chat_id:
                    if len(brief) > 4096:
                        brief_to_send = brief[:4080] + "\n\n<i>(truncated)</i>"
                    else:
                        brief_to_send = brief
                    send_telegram(brief_to_send, token, chat_id)
                    log.info("Stanley brief sent via Telegram")
                else:
                    log.warning("Stanley: Telegram not configured")
            except Exception as e:
                log.error(f"Stanley Telegram send failed: {e}")

    # 8. Send via email
    try:
        from .postmaster import markdown_to_html, send_email as _send_email
        html_brief = markdown_to_html(brief)
        today_str = datetime.now().strftime("%b %d, %Y")
        _send_email(
            f"Stanley Morning Brief — {today_str}",
            html_brief,
            to_addr=REPORT_RECIPIENT,
        )
        log.info("Stanley brief sent via email")
    except Exception as e:
        log.error(f"Stanley email send failed: {e}", exc_info=True)

    return brief


# ---------------------------------------------------------------------------
# CLI Helper
# ---------------------------------------------------------------------------

def generate_brief_on_demand(conn: sqlite3.Connection, db_path: str) -> str:
    """Generate a brief from the most recent email (for CLI testing)."""
    row = conn.execute(
        "SELECT id, raw_text FROM emails ORDER BY date_sent DESC LIMIT 1"
    ).fetchone()

    if not row:
        return "No emails in database."

    email_id = row["id"]
    raw_text = row["raw_text"]

    # Get parsed signals for this email
    sigs = conn.execute(
        "SELECT * FROM signals WHERE email_id = ?", (email_id,)
    ).fetchall()
    cycs = conn.execute(
        "SELECT * FROM cycles WHERE email_id = ?", (email_id,)
    ).fetchall()
    tgts = conn.execute(
        "SELECT * FROM price_targets WHERE email_id = ?", (email_id,)
    ).fetchall()

    parsed = {
        "signals": [dict(s) for s in sigs],
        "cycles": [dict(c) for c in cycs],
        "price_targets": [dict(t) for t in tgts],
    }

    return generate_morning_brief(
        conn, raw_text, parsed, changes=[],
        db_path=db_path, email_id=email_id,
        send_telegram_flag=False,
    )
