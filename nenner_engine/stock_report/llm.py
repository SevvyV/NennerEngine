"""Stock report LLM commentary — Stanley's Take generation.

Calls the Anthropic API with a structured portfolio summary and returns
the rendered HTML snippet for the Take section. Returns "" on failure
so the report still ships without commentary.
"""

import logging
import time

from ..config import LLM_MODEL, LLM_MAX_TOKENS_REPORT, LLM_RETRY_ATTEMPTS
from .html import _fmt_price, _fmt_pct

log = logging.getLogger(__name__)


REPORT_SYSTEM_PROMPT = """\
You are Stanley, a trading advisor modeled after Stanley Druckenmiller. \
You write the "Stanley's Take" section of a daily stock report for a \
portfolio manager who trades 6 individual stocks using the Nenner Cycle \
Research system.

## Druckenmiller Principles
- Bet big when conviction is high; cut quickly when wrong
- The best traders protect capital above all else
- Timing exits is harder than timing entries — use systematic signals
- Don't get married to a position — when the thesis changes, get out
- Concentration beats diversification when you have an edge

## Your Task
Write 3-4 paragraphs analyzing today's portfolio state. Be SPECIFIC — \
reference actual prices, levels, and percentages. Focus on:
1. Which positions to hold vs exit, with clear reasoning
2. Where the inflection points are (cancel proximity, cycle turns)
3. Risk management — which positions have deteriorating R:R
4. Trade duration — if a trade is approaching or past its average duration, the edge is fading; don't recommend initiating new trades late in the cycle
5. Cross-stock observations (correlations, sector moves)

## Formatting
Write in HTML using only <b>, <i>, <br> tags. Use \\n for paragraph breaks.
Keep the total under 2000 characters. Be direct and opinionated — do not \
hedge with "consider" or "may want to." Give clear guidance.
"""


def _build_llm_context(stocks_data: list[dict]) -> str:
    """Build a structured text summary for the LLM."""
    lines = ["PORTFOLIO SNAPSHOT:"]
    for s in stocks_data:
        flags = ", ".join(s.get("inflection_flags", [])) or "none"
        rr_val = s.get("reward_risk")
        rr_str = f"{rr_val:.1f}:1" if rr_val is not None else "N/A"
        implied_str = " (implied)" if s["implied_reversal"] else ""
        lines.append(
            f"  {s['ticker']} | {s['signal']}{implied_str} "
            f"from {_fmt_price(s['origin_price']).replace('&mdash;', '—')} | "
            f"Price: {_fmt_price(s['price']).replace('&mdash;', '—')} | "
            f"P/L: {_fmt_pct(s['pnl_pct']).replace('&mdash;', '—')} | "
            f"Cancel: {_fmt_price(s['cancel_level']).replace('&mdash;', '—')} "
            f"({_fmt_pct(s['cancel_dist_pct']).replace('&mdash;', '—')} dist) | "
            f"Target: {_fmt_price(s['target_price']).replace('&mdash;', '—')} | "
            f"R:R: {rr_str} | "
            f"NTC 30d: {s['ntc_count_30d']} | "
            f"Cycles: {s['cycle_alignment']} | "
            f"Flags: {flags}"
        )

        # Trade duration
        age_days = s.get("trade_age_days")
        avg_dur = s.get("avg_duration")
        age_ratio = s.get("trade_age_ratio")
        if age_days is not None:
            dur_str = f"Day {age_days}"
            if avg_dur:
                dur_str += f", avg={avg_dur:.0f}d, ratio={age_ratio:.2f}" if age_ratio else f", avg={avg_dur:.0f}d"
            lines.append(f"    Duration: {dur_str}")

        ts = s.get("trade_stats")
        if ts:
            lines.append(
                f"    Stats: Sharpe {ts['sharpe']:.2f}, WR {ts['win_rate']:.0f}%, "
                f"Kelly {ts['kelly']:.0%}, {ts['trades']} trades, Flag: {s['risk_flag'] or 'none'}"
            )

        staircase = s.get("target_staircase", {})
        if staircase.get("is_staircasing"):
            prev_tp = staircase.get("previous_target")
            new_tp = staircase.get("latest_target")
            reached_n = staircase.get("targets_reached", 0)
            sc_dir = staircase.get("staircase_direction", "")
            lines.append(
                f"    Target Staircase: {reached_n} target(s) reached in 60 days, "
                f"prev={prev_tp}, new={new_tp}, direction={sc_dir}"
            )

    return "\n".join(lines)


def _generate_stanley_take(stocks_data: list[dict], api_key: str) -> str:
    """Call the Anthropic API for Stanley's interpretive commentary."""
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    context = _build_llm_context(stocks_data)

    user_message = (
        f"Here is today's portfolio state for the 6 focus stocks:\n\n"
        f"{context}\n\n"
        f"Write Stanley's Take for today's report."
    )

    last_error = None
    for attempt in range(LLM_RETRY_ATTEMPTS + 1):
        try:
            message = client.messages.create(
                model=LLM_MODEL,
                max_tokens=LLM_MAX_TOKENS_REPORT,
                system=[{
                    "type": "text",
                    "text": REPORT_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_message}],
            )
            return message.content[0].text
        except Exception as e:
            last_error = e
            if attempt < LLM_RETRY_ATTEMPTS:
                wait = 2 ** (attempt + 1)
                log.warning(f"Stock report LLM error (attempt {attempt + 1}), "
                            f"retrying in {wait}s: {e}")
                time.sleep(wait)

    log.error(f"Stock report LLM failed after {LLM_RETRY_ATTEMPTS + 1} attempts: {last_error}")
    return ""
