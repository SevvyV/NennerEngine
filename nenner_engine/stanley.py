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
import math
import random
import re
import sqlite3
import time
from datetime import datetime
from typing import Optional

from .config import LLM_MODEL, LLM_MAX_TOKENS_STANLEY, LLM_RETRY_ATTEMPTS, REPORT_RECIPIENT

log = logging.getLogger(__name__)


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
                email_id: Optional[int] = None) -> Optional[int]:
    """Store a generated brief in stanley_briefs table. Returns the brief ID,
    or None if a brief for this email_id already exists."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO stanley_briefs (email_id, brief_text) VALUES (?, ?)",
        (email_id, brief_text)
    )
    conn.commit()
    if cur.rowcount == 0:
        log.info(f"Stanley dedup: brief for email_id={email_id} already stored, skipping")
        return None
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


def _gather_market_snapshot(conn: sqlite3.Connection) -> dict:
    """Gather current prices, daily changes, and cancel proximity.

    Returns dict with 'movers' (|change| >= 3%), 'all' rows, and 'as_of' date.
    """
    # Current prices (latest per ticker, prefer T1).
    # Exclude weekend dates — DataBento can write garbage indicative prices
    # on Sat/Sun that poison daily-change calculations.
    current = {}
    rows = conn.execute(
        "SELECT ticker, close, date, source FROM price_history p "
        "INNER JOIN ("
        "  SELECT ticker AS t, MAX(date) AS d FROM price_history "
        "  WHERE CASE CAST(strftime('%w', date) AS INTEGER) "
        "    WHEN 0 THEN 0 WHEN 6 THEN 0 ELSE 1 END = 1 "
        "  GROUP BY ticker"
        ") m ON p.ticker = m.t AND p.date = m.d "
        "ORDER BY p.ticker, CASE WHEN p.source = 'T1' THEN 0 ELSE 1 END"
    ).fetchall()
    for r in rows:
        t = r["ticker"] if hasattr(r, "keys") else r[0]
        if t not in current:
            current[t] = {
                "price": r["close"] if hasattr(r, "keys") else r[1],
                "date": r["date"] if hasattr(r, "keys") else r[2],
            }

    if not current:
        return {"movers": [], "all": [], "as_of": "N/A"}

    as_of = max(v["date"] for v in current.values())

    # Prior close (most recent date before current date, per ticker)
    prior = {}
    for ticker, info in current.items():
        row = conn.execute(
            "SELECT close FROM price_history "
            "WHERE ticker = ? AND date < ? ORDER BY date DESC LIMIT 1",
            (ticker, info["date"]),
        ).fetchone()
        if row:
            prior[ticker] = row["close"] if hasattr(row, "keys") else row[0]

    # Signal state for cancel proximity
    signals = {}
    for r in conn.execute(
        "SELECT ticker, effective_signal, cancel_level FROM current_state"
    ).fetchall():
        t = r["ticker"] if hasattr(r, "keys") else r[0]
        signals[t] = {
            "signal": r["effective_signal"] if hasattr(r, "keys") else r[1],
            "cancel": r["cancel_level"] if hasattr(r, "keys") else r[2],
        }

    # Build snapshot rows
    all_rows = []
    for ticker, info in current.items():
        price = info["price"]
        if not price or not math.isfinite(price):
            continue
        prior_close = prior.get(ticker)
        if prior_close and math.isfinite(prior_close) and prior_close > 0:
            change_pct = (price - prior_close) / prior_close * 100
        else:
            change_pct = None

        sig = signals.get(ticker, {})
        cancel = sig.get("cancel")
        if cancel and math.isfinite(cancel) and price > 0:
            cancel_dist = (cancel - price) / price * 100
        else:
            cancel_dist = None

        all_rows.append({
            "ticker": ticker,
            "price": price,
            "change_pct": change_pct,
            "signal": sig.get("signal", "—"),
            "cancel": cancel,
            "cancel_dist_pct": cancel_dist,
        })

    # Sort by |change| descending
    all_rows.sort(key=lambda r: abs(r["change_pct"] or 0), reverse=True)
    movers = [r for r in all_rows if r["change_pct"] is not None and abs(r["change_pct"]) >= 3.0]

    return {"movers": movers, "all": all_rows, "as_of": as_of}


def _format_market_snapshot(snapshot: dict) -> str:
    """Format market snapshot as compact text for the system prompt."""
    if not snapshot["all"]:
        return "(No price data available)"

    lines = []

    # Movers section
    if snapshot["movers"]:
        lines.append("MOVERS (>3% daily change):")
        for r in snapshot["movers"]:
            chg = f"{r['change_pct']:+.1f}%"
            cancel_str = ""
            if r["cancel"] and r["cancel_dist_pct"] is not None:
                cancel_str = f" | cancel {r['cancel']:.2f} ({r['cancel_dist_pct']:+.1f}% away)"
            lines.append(f"  {r['ticker']} {chg} @ {r['price']:.2f} | {r['signal']}{cancel_str}")
        lines.append("")
    else:
        lines.append("No major movers today (all <3%).")
        lines.append("")

    # Cancel proximity alerts
    danger = [r for r in snapshot["all"]
              if r["cancel_dist_pct"] is not None and abs(r["cancel_dist_pct"]) < 0.2]
    if danger:
        lines.append("CANCEL PROXIMITY (<0.2% away):")
        for r in danger:
            chg = f"{r['change_pct']:+.1f}%" if r["change_pct"] is not None else "N/A"
            lines.append(
                f"  {r['ticker']} @ {r['price']:.2f} ({chg}) | {r['signal']} "
                f"cancel {r['cancel']:.2f} ({r['cancel_dist_pct']:+.1f}%)"
            )
        lines.append("")

    # Compact full snapshot (top 25 by |change|, then summary)
    lines.append("Ticker | Price | Chg% | Signal | Cancel | Dist%")
    shown = 0
    for r in snapshot["all"]:
        if shown >= 25:
            remaining = len(snapshot["all"]) - shown
            lines.append(f"... plus {remaining} more instruments")
            break
        chg = f"{r['change_pct']:+.1f}%" if r["change_pct"] is not None else "—"
        cancel = f"{r['cancel']:.2f}" if r["cancel"] else "—"
        dist = f"{r['cancel_dist_pct']:+.1f}%" if r["cancel_dist_pct"] is not None else "—"
        lines.append(f"{r['ticker']} | {r['price']:.2f} | {chg} | {r['signal']} | {cancel} | {dist}")
        shown += 1

    return "\n".join(lines)


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

## Market Snapshot (prices as of {snapshot_date})
{market_snapshot}

IMPORTANT: If there are significant movers (>3% daily change), lead your \
"Top 3 Things That Matter Today" with price action. Flag any instrument \
approaching its cancel level (<0.2% away) — these are potential signal flips.

## Trade Statistics (historical performance)
{trade_stats}

## Recent Signal History (changed instruments)
{recent_signals}

## Cycle Data (changed instruments)
{cycle_data}

## Output Format — CRITICAL
You MUST output plain markdown. NEVER use HTML tags (<b>, <i>, <br>, etc.). \
Use markdown syntax only: ## for headers, **bold**, *italic*, - for bullets.

## Stanley's Brief — {today_date}

## Top 3 Things That Matter Today
Numbered list of the most important takeaways.

## Categorized Changes
- Cancelled SELL → now BUY (bullish reversals)
- Cancelled BUY → now SELL (bearish reversals)
- Active signals updated (cancel level changes, new signals)
- Other observations

## Per-Instrument Context
For the most significant 3-5 changes, add a line about historical \
performance (win rate, risk flag) and cycle alignment.

## Cross-Instrument Observations
Correlations, alignments, divergences.

End with: "Anything I'm reading wrong? Teach me with --stanley-teach"

Keep the total brief under 3500 characters. Be opinionated but transparent \
about uncertainty. Use only these emoji: \U0001f7e2 for BUY, \U0001f534 \
for SELL.
"""


def _strip_html_to_markdown(text: str) -> str:
    """Convert stray HTML tags to markdown equivalents.

    Safety net in case the LLM outputs Telegram-style HTML instead of markdown.
    """
    text = re.sub(r"<b>(.*?)</b>", r"**\1**", text)
    text = re.sub(r"<strong>(.*?)</strong>", r"**\1**", text)
    text = re.sub(r"<i>(.*?)</i>", r"*\1*", text)
    text = re.sub(r"<em>(.*?)</em>", r"*\1*", text)
    text = re.sub(r"<br\s*/?>", "\n", text)
    return text


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
    market_snapshot: dict | None = None,
    mentioned_tickers: Optional[set] = None,
) -> str:
    """Build the full system prompt with injected context."""
    snapshot_text = _format_market_snapshot(market_snapshot) if market_snapshot else "(No price data)"
    snapshot_date = market_snapshot["as_of"] if market_snapshot else "N/A"
    return STANLEY_SYSTEM_PROMPT.format(
        knowledge_base=_format_knowledge(knowledge),
        current_state=_format_current_state(current_state),
        market_snapshot=snapshot_text,
        snapshot_date=snapshot_date,
        trade_stats=_format_trade_stats(trade_stats, highlight=mentioned_tickers),
        recent_signals=_format_recent_signals(recent_signals),
        cycle_data=_format_cycles(cycles),
        today_date=datetime.now().strftime("%B %d, %Y"),
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
                # Add jitter so concurrent retries don't lockstep.
                base = 2 ** (attempt + 1)
                wait = base + random.uniform(0, base / 2)
                log.warning(f"Stanley LLM error (attempt {attempt + 1}), "
                            f"retrying in {wait:.1f}s: {e}")
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
) -> str:
    """Generate and optionally send Stanley's morning brief.

    This is the primary integration point called from email_scheduler.py.
    """
    from .llm_parser import _get_cached_api_key

    try:
        api_key = _get_cached_api_key()
    except ValueError as e:
        log.error(f"Stanley cannot generate brief: {e}")
        return ""

    # 0. DB-level dedup. Three states for a given email_id:
    #   - row exists, sent_at set     → already delivered, skip entirely
    #   - row exists, sent_at NULL    → previous send failed; REUSE the stored
    #                                   brief text and retry the email step
    #                                   (cheaper than another LLM call, and
    #                                   side-steps the UNIQUE(email_id) index
    #                                   that would block a fresh INSERT)
    #   - no row                      → fresh generation
    brief: str = ""
    brief_id: Optional[int] = None
    if email_id is not None:
        existing = conn.execute(
            "SELECT id, brief_text, sent_at FROM stanley_briefs "
            "WHERE email_id = ? LIMIT 1",
            (email_id,),
        ).fetchone()
        if existing:
            existing_id = existing["id"] if hasattr(existing, "keys") else existing[0]
            existing_text = existing["brief_text"] if hasattr(existing, "keys") else existing[1]
            existing_sent = existing["sent_at"] if hasattr(existing, "keys") else existing[2]
            if existing_sent:
                log.info(f"Stanley dedup: brief already sent for email_id={email_id}, skipping")
                return ""
            log.info(
                f"Stanley dedup: prior brief for email_id={email_id} not yet sent — "
                "reusing stored text and retrying delivery"
            )
            brief = existing_text or ""
            brief_id = existing_id

    if not brief:
        # 1. Determine which tickers need detailed context
        mentioned_tickers = _extract_mentioned_tickers(changes, parsed_signals)

        # 2. Gather all context
        current_state = _gather_current_state(conn)
        trade_stats = _gather_trade_stats(conn, mentioned_tickers)
        recent_signals = _gather_recent_signals(conn, mentioned_tickers)
        cycles = _gather_cycles(conn, mentioned_tickers)
        knowledge = get_knowledge_base(conn)
        market_snapshot = _gather_market_snapshot(conn)

        # 3. Build system prompt
        system_prompt = _build_stanley_system_prompt(
            knowledge, current_state, trade_stats, recent_signals, cycles,
            market_snapshot=market_snapshot,
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
            brief_id = store_brief(conn, brief, email_id)
        except Exception as e:
            log.error(f"Failed to store Stanley brief: {e}")
            brief_id = None

        if brief_id is None:
            return ""

    # 7. Send via email — only mark sent_at if delivery actually succeeded,
    # so a store-success / send-failure can be retried on the next attempt.
    sent_ok = False
    try:
        from .postmaster import markdown_to_html, send_email as _send_email
        email_brief = _strip_html_to_markdown(brief)
        html_brief = markdown_to_html(email_brief)
        today_str = datetime.now().strftime("%b %d, %Y")
        sent_ok = bool(_send_email(
            f"Stanley Morning Brief — {today_str}",
            html_brief,
            to_addr=REPORT_RECIPIENT,
        ))
        if sent_ok:
            log.info("Stanley brief sent via email")
        else:
            log.error(
                "Stanley brief NOT sent — send_email returned False "
                "(brief_id=%s will be retried on next run)", brief_id,
            )
    except Exception as e:
        log.error(f"Stanley email send failed: {e}", exc_info=True)

    if sent_ok and brief_id is not None:
        try:
            conn.execute(
                "UPDATE stanley_briefs SET sent_at = datetime('now') WHERE id = ?",
                (brief_id,),
            )
            conn.commit()
        except Exception as e:
            log.error(f"Failed to mark Stanley brief sent_at: {e}")

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
    )
