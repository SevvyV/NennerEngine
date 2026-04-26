"""Stanley's Daily Stock Report.

Generates and emails a comprehensive daily report for focus stocks
(AAPL, BAC, GOOG, MSFT, NVDA, TSLA) and equity index futures (ES, NQ).

Sections:
  1. Portfolio Heat Map — quick-scan table with P/L, cancel distance, alerts
  2. Inflection Alerts — stocks at/through cancel levels, fresh reversals, churn
  3. Stock-by-Stock Detail — full context per instrument
  4. Exit Timing Framework — systematic ranking by exit urgency
  5. Stanley's Take — LLM-generated Druckenmiller-lens commentary

Module layout:
  data.py — DB queries, dict assembly, inflection-flag detection
  html.py — Section builders, formatting, full-report assembly
  llm.py  — Stanley's Take generation
"""

import logging
import sqlite3

# Re-export public surface so external imports keep working unchanged.
# Tests and scripts import from nenner_engine.stock_report directly.
from .data import (  # noqa: F401
    FOCUS_STOCKS,
    STOCK_NAMES,
    DISPLAY_TICKER,
    CANCEL_DANGER_PCT,
    CANCEL_WATCH_PCT,
    _get_cancel_trajectory,
    _count_ntc,
    _get_latest_target,
    _get_cycles,
    _assess_cycle_alignment,
    _compute_reward_risk,
    _get_target_progression,
    _detect_target_staircase,
    _get_signal_history,
    _detect_inflection_flags,
    gather_report_data,
)
from .html import (  # noqa: F401
    build_stock_report_html,
    build_report_subject,
)
from .llm import (  # noqa: F401
    _generate_stanley_take,
    _build_llm_context,
    REPORT_SYSTEM_PROMPT,
)

# send_email is re-exported from postmaster (was always a re-export here).
# Import name into this namespace so test patches @patch("nenner_engine.
# stock_report.send_email") continue to work AND the orchestrator below
# resolves the call through this module's namespace.
from ..postmaster import send_email  # noqa: F401

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Entry Points
# ---------------------------------------------------------------------------

def generate_and_send_stock_report(
    conn: sqlite3.Connection,
    db_path: str,
    send_email_flag: bool = True,
    include_llm: bool = True,
) -> str:
    """Generate the full stock report and optionally send via email.

    This is the primary integration point called from the scheduler or CLI.
    Returns the generated HTML.
    """
    log.info("Generating Stanley's Stock Report...")

    # 1. Gather data
    stocks_data = gather_report_data(conn)
    if not stocks_data:
        log.warning("Stock report: no data available for focus stocks")
        return ""

    # 2. Generate LLM commentary
    stanley_take = ""
    if include_llm:
        try:
            from ..llm_parser import _get_cached_api_key
            api_key = _get_cached_api_key()
            stanley_take = _generate_stanley_take(stocks_data, api_key)
        except Exception as e:
            log.error(f"Stock report LLM commentary failed: {e}")

    # 3. Build HTML
    html = build_stock_report_html(stocks_data, stanley_take)
    subject = build_report_subject(stocks_data)

    # 4. Send email — check return value so a silent SMTP failure surfaces
    if send_email_flag:
        ok = send_email(subject, html)
        if not ok:
            log.error("Stock report send_email returned False — email NOT delivered")

    log.info(f"Stock report generated: {len(html)} chars, "
             f"{len(stocks_data)} stocks, LLM={'yes' if stanley_take else 'no'}")
    return html


def generate_stock_report_on_demand(conn: sqlite3.Connection,
                                    db_path: str) -> str:
    """Generate the report and send it (for CLI testing).

    Returns the HTML body.
    """
    return generate_and_send_stock_report(
        conn, db_path, send_email_flag=True, include_llm=True,
    )
