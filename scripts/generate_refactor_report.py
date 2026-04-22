"""Generate the NennerEngine Refactor Report PDF.

Produces a multi-page PDF summarizing the refactor session work so it
can be referenced later. Run from the project root:

    python scripts/generate_refactor_report.py
"""

from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "docs" / \
    "NennerEngine_Refactor_Report_2026-04-22.pdf"


# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------

NAVY = colors.HexColor("#1a365d")
ACCENT = colors.HexColor("#2c5282")
MUTED = colors.HexColor("#4a5568")
LIGHT = colors.HexColor("#f7fafc")
SUCCESS = colors.HexColor("#2f855a")
WARN = colors.HexColor("#c05621")
MONO_BG = colors.HexColor("#edf2f7")

styles = getSampleStyleSheet()

H_TITLE = ParagraphStyle(
    "Title", parent=styles["Title"], fontSize=24, leading=28,
    textColor=NAVY, spaceAfter=6, alignment=TA_LEFT,
)
H_SUBTITLE = ParagraphStyle(
    "Subtitle", parent=styles["Normal"], fontSize=12, leading=16,
    textColor=MUTED, spaceAfter=20,
)
H1 = ParagraphStyle(
    "H1", parent=styles["Heading1"], fontSize=16, leading=20,
    textColor=NAVY, spaceBefore=16, spaceAfter=10,
)
H2 = ParagraphStyle(
    "H2", parent=styles["Heading2"], fontSize=12, leading=16,
    textColor=ACCENT, spaceBefore=12, spaceAfter=6,
)
BODY = ParagraphStyle(
    "Body", parent=styles["BodyText"], fontSize=9.5, leading=13,
    textColor=colors.black, spaceAfter=6,
)
MUTED_BODY = ParagraphStyle(
    "Muted", parent=BODY, textColor=MUTED, fontSize=9, leading=12,
)
CODE = ParagraphStyle(
    "Code", parent=styles["Code"], fontSize=8.5, leading=11,
    textColor=NAVY, fontName="Courier",
    backColor=MONO_BG, borderPadding=4, spaceAfter=6,
)
BULLET = ParagraphStyle(
    "Bullet", parent=BODY, leftIndent=14, bulletIndent=2, spaceAfter=3,
)


# ---------------------------------------------------------------------------
# Reusable components
# ---------------------------------------------------------------------------

def fix_block(hash_: str, title: str, problem: str, fix: str, files: str) -> list:
    """A 2-column table describing one fix."""
    header = Paragraph(
        f"<font color='#2c5282'><b>{hash_}</b></font> &nbsp; <b>{title}</b>",
        BODY,
    )
    data = [
        [header, ""],
        [Paragraph("<b>Problem</b>", MUTED_BODY), Paragraph(problem, BODY)],
        [Paragraph("<b>Fix</b>", MUTED_BODY), Paragraph(fix, BODY)],
        [Paragraph("<b>Files</b>", MUTED_BODY), Paragraph(f"<font name='Courier' size='8'>{files}</font>", BODY)],
    ]
    t = Table(data, colWidths=[0.9 * inch, 5.6 * inch])
    t.setStyle(TableStyle([
        ("SPAN", (0, 0), (1, 0)),
        ("BACKGROUND", (0, 0), (1, 0), LIGHT),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.HexColor("#cbd5e0")),
    ]))
    return [t, Spacer(1, 8)]


def bullet(text: str) -> Paragraph:
    return Paragraph(f"• {text}", BULLET)


def kv_row(label: str, value: str, value_color=None) -> list:
    v = value if value_color is None else f"<font color='{value_color}'><b>{value}</b></font>"
    return [Paragraph(f"<b>{label}</b>", BODY), Paragraph(v, BODY)]


# ---------------------------------------------------------------------------
# Page content
# ---------------------------------------------------------------------------

def build_story() -> list:
    story = []

    # ---- Cover ----
    story.append(Paragraph("NennerEngine Refactor Report", H_TITLE))
    story.append(Paragraph(
        "Sessions of April 21–22, 2026 &nbsp; | &nbsp; Claude Opus 4.7",
        H_SUBTITLE,
    ))

    summary_data = [
        kv_row("Session duration", "~7.5 hours (Apr 21 21:00 – 00:20 + Apr 22 08:40 – 12:35 EDT)"),
        kv_row("Commits landed", "34 (20 overnight + 14 morning of Apr 22)"),
        kv_row("Tests passing", "385 across tests/ + test_nenner_engine.py", SUCCESS.hexval()),
        kv_row("Net new tests", "74 (across 10 new test files)"),
        kv_row("Phases complete",
               "1 (Safety), 2 (Stability), 3.2 + 3.3 (dashboard + stock_report split), "
               "3.4–3.6 (Architecture), 4 (Hygiene)"),
        kv_row("Phases remaining", "3.1 only (Canonical query layer)"),
        kv_row("Working tree", "Clean", SUCCESS.hexval()),
        kv_row("Known regressions from this work", "None", SUCCESS.hexval()),
    ]
    summary = Table(summary_data, colWidths=[2.0 * inch, 4.5 * inch])
    summary.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("LINEBELOW", (0, 0), (-1, -2), 0.25, colors.HexColor("#e2e8f0")),
    ]))
    story.append(summary)
    story.append(Spacer(1, 18))

    # ---- Executive summary ----
    story.append(Paragraph("Executive Summary", H1))
    story.append(Paragraph(
        "This session took NennerEngine from a working-but-fragile state "
        "to a materially more reliable one. The focus was on "
        "<b>silent-failure bugs</b> and <b>tight-loop hazards</b> — the class "
        "of problems that don't crash the daemon but degrade it invisibly.",
        BODY,
    ))
    story.append(Paragraph(
        "The biggest wins, in rough order of morning-impact:",
        BODY,
    ))
    story.append(bullet(
        "<b>JSON salvage</b> on truncated LLM responses was broken — its "
        "allowlist had collapsed to the single character <font name='Courier'>'e'</font>. "
        "Every LLM response that hit the max_tokens ceiling silently "
        "salvaged to garbage."
    ))
    story.append(bullet(
        "<b>UTC-5 DST fallback</b> in the email scheduler ran whenever "
        "<font name='Courier'>zoneinfo</font> raised an ImportError — which silently mis-fired every "
        "scheduled job by one hour for ~8 months a year."
    ))
    story.append(bullet(
        "<b>/health endpoint</b> lied when the external monitor wasn't "
        "running: the missing email scheduler silently disappeared from "
        "the threads dict and the endpoint reported healthy. It now probes "
        "the <font name='Courier'>emails</font> table directly and fails 503 on staleness."
    ))
    story.append(bullet(
        "<b>Empty signal parse</b> on a morning Nenner email used to "
        "mark it read and commit an empty result; now retries once and, "
        "if still empty, deletes the email row and alerts the admin."
    ))
    story.append(bullet(
        "<b>Restart storms</b> in the scheduler and equity-stream loops "
        "now have exponential backoff and an admin Telegram after 3 failures."
    ))
    story.append(bullet(
        "<b>Auto-cancel</b> no longer pollutes the <font name='Courier'>emails</font> table with synthetic "
        "rows that blocked retries when yFinance corrected a close."
    ))

    story.append(PageBreak())

    # ---- Phase 1 ----
    story.append(Paragraph("Phase 1 — Safety", H1))
    story.append(Paragraph(
        "Eight commits fixing silent-failure bugs and tight-loop hazards. "
        "Each landed with test coverage where applicable.",
        MUTED_BODY,
    ))

    story.extend(fix_block(
        "9caee00", "Single .env loader (foundation)",
        "Three near-duplicate <font name='Courier'>_load_env()</font> helpers lived in alert_dispatch.py, "
        "imap_client.py, and llm_parser.py, plus an inline parser in "
        "equity_stream.py. Each had slightly different behavior.",
        "Introduced <font name='Courier'>config.load_env_once()</font> — idempotent, thread-safe, uses "
        "<font name='Courier'>setdefault()</font> so real env vars still override .env.",
        "nenner_engine/config.py",
    ))
    story.extend(fix_block(
        "4c5e52f", "Fix _salvage_truncated_json bracket balancing",
        "The allowlist of valid JSON terminators collapsed to the single "
        "char <font name='Courier'>'e'</font> via an erroneous [-1:] slice. Also closed brackets in "
        "FIFO order rather than LIFO, producing invalid JSON on nested "
        "structures.",
        "Rewrote with a real bracket-tracking stack (respects strings and "
        "escapes), closes in LIFO order, strips trailing chars until a "
        "candidate parses cleanly. <b>7 new tests.</b>",
        "nenner_engine/llm_parser.py, tests/test_llm_parser.py",
    ))
    story.extend(fix_block(
        "4e9b40c", "Centralize cancel-breach rule",
        "The close-vs-cancel comparison was inlined in two places "
        "(state rebuild and scheduled 4:30 PM auto-cancel) with slightly "
        "different forms. Semantics were already drifting.",
        "Single <font name='Courier'>db.is_cancel_breached()</font> helper — strict inequality, "
        "None-safe. Both callers now route through it. <b>10 new tests.</b>",
        "nenner_engine/db.py, nenner_engine/auto_cancel.py, tests/test_cancel_breach.py",
    ))
    story.extend(fix_block(
        "1d8d0e9", "Migrate imap/alert/equity to shared env loader",
        "Follow-up to 9caee00 — consolidate the remaining callers onto "
        "<font name='Courier'>load_env_once()</font>.",
        "<font name='Courier'>grep \"def _load_env|load_dotenv(\"</font> in the package now returns no "
        "matches.",
        "alert_dispatch.py, imap_client.py, llm_parser.py, equity_stream.py",
    ))
    story.extend(fix_block(
        "7a26d79", "Dashboard DB hygiene + honest /health probe",
        "Callbacks opened SQLite with no busy_timeout and no WAL reassertion; "
        "schema migrations ran on every connection. /health silently "
        "dropped the email_scheduler key when _email_sched was None (the "
        "normal case in this deployment), so it reported healthy even "
        "when nothing was ingesting email.",
        "get_db() now sets busy_timeout=5000; migrations run once at startup. "
        "/health reports all three threads explicitly, uses \"external\" sentinel "
        "for the scheduler, and fails 503 on ingestion staleness "
        "(72h Mon/Wed/Fri PM, 120h otherwise).",
        "dashboard.py",
    ))
    story.extend(fix_block(
        "985f2a9", "Retry + rollback on signal-bearing email with 0 signals",
        "If a morning email parsed to zero signals (transient Anthropic "
        "outage, salvage-to-empty on truncation), the email was stored, "
        "IMAP UID marked \\Seen, and the message_id dedupe permanently "
        "blocked a retry.",
        "For signal-bearing types (morning_update, stocks_update, "
        "sunday_cycles, weekly_overview), retry once; if still empty, "
        "<font name='Courier'>DELETE FROM emails</font>, return False (keeps UID unread), and "
        "Telegram-alert the admin.",
        "nenner_engine/imap_client.py",
    ))
    story.extend(fix_block(
        "863029c", "Scheduler restart backoff",
        "If the email scheduler thread died and restart also failed "
        "(DB locked, vault rotation), the prior code retried on every "
        "alert-monitor tick with no cap — logging the same traceback "
        "every 60s forever.",
        "Exponential backoff (10s → 60s → 300s), single admin Telegram "
        "after 3 consecutive failures, state resets cleanly on recovery.",
        "nenner_engine/alerts.py",
    ))
    story.extend(fix_block(
        "cedd215", "Equity stream reconnect backoff",
        "Same pattern as the scheduler — persistent DataBento auth "
        "failure would spin the reconnect loop forever at a fixed 10s "
        "interval, never surfacing to the admin.",
        "Backoff 10s → 30s → 60s → 300s, admin Telegram after 3 failures.",
        "nenner_engine/equity_stream.py",
    ))

    story.append(PageBreak())

    # ---- Phase 2 ----
    story.append(Paragraph("Phase 2 — Stability", H1))
    story.append(Paragraph(
        "Five commits focused on race safety, shutdown correctness, and "
        "the &quot;can we re-run this?&quot; question.",
        MUTED_BODY,
    ))

    story.extend(fix_block(
        "295e401", "Schema version tracker + migration 16",
        "migrate_db() re-ran ~15 idempotent ALTER/CREATE statements on "
        "every scheduler tick and every dashboard callback. Each wrapped "
        "in try/except-pass, silently swallowing real migration errors.",
        "New <font name='Courier'>CURRENT_SCHEMA_VERSION = 16</font> + schema_version table. "
        "migrate_db short-circuits when stored == target. 100 no-op calls "
        "now take 0.3ms. Migration 16 adds <font name='Courier'>signals.source</font> column.",
        "nenner_engine/db.py",
    ))
    story.extend(fix_block(
        "16fa14c", "Auto-cancel: no more synthetic emails",
        "Each close-breach wrote a fake <font name='Courier'>emails</font> row with "
        "message_id=&quot;auto-cancel-{ticker}-{date}&quot;. The dedupe on "
        "message_id made it impossible to re-run after yFinance corrected "
        "a historical close.",
        "Writes directly to signals with email_id=NULL and source='auto_cancel'. "
        "Dedupe is an existence query on (ticker, date, source). New "
        "<font name='Courier'>regenerate=True</font> parameter for forced re-runs. <b>4 new tests.</b>",
        "nenner_engine/auto_cancel.py, tests/test_auto_cancel.py",
    ))
    story.extend(fix_block(
        "9230c92", "Thread-safe global caches",
        "<font name='Courier'>_prev_close_cache</font> (dashboard) and "
        "<font name='Courier'>_cache_all/_cache_tradeable</font> (trade_stats) were read/written from "
        "multiple Dash callback threads with no locks. Torn reads possible "
        "under concurrent update.",
        "Lock around cache check/store. Network fetch happens outside the "
        "lock so callbacks don't serialize on yfinance. Concurrent misses "
        "may both recompute (idempotent) but neither sees torn state.",
        "dashboard.py, nenner_engine/trade_stats.py",
    ))
    story.extend(fix_block(
        "91c3873", "Responsive equity-stream shutdown",
        "The DataBento Live iterator blocks on socket recv inside "
        "<font name='Courier'>for record in live:</font>. On low-volume weekends, shutdown could "
        "stall tens of seconds waiting for the next tick to unblock the "
        "stop-event check.",
        "Daemon watchdog thread calls <font name='Courier'>live.stop()</font> the moment stop_event "
        "fires — closes the socket, unblocks the iterator, shutdown "
        "completes immediately.",
        "nenner_engine/equity_stream.py",
    ))
    story.extend(fix_block(
        "4abb199", "Remove unused detect_signal_changes",
        "Exported from __init__ but never registered as an evaluator, "
        "never called from any dispatch path. Per user preference, "
        "Telegram is removed from signal alerts, so wiring it up would "
        "contradict policy.",
        "Deleted the function, its __init__.py re-export, and its test.",
        "alerts.py, __init__.py, test_nenner_engine.py",
    ))

    story.append(PageBreak())

    # ---- Phase 3.4-3.6 ----
    story.append(Paragraph("Phase 3.4–3.6 — Architecture", H1))
    story.append(Paragraph(
        "Three smaller architectural wins that unlock cleaner tests and "
        "per-module log filtering.",
        MUTED_BODY,
    ))

    story.extend(fix_block(
        "beebc12", "Centralize Eastern Time in nenner_engine.tz",
        "Five different call sites re-derived ET on their own. "
        "email_scheduler had a UTC-5 fallback that ran whenever "
        "<font name='Courier'>zoneinfo</font> raised ImportError — silently off by one hour for "
        "~8 months/year during Daylight Saving Time.",
        "New <font name='Courier'>nenner_engine/tz.py</font> — single source. ET, now_et(), today_et(). "
        "The UTC-5 fallback is gone; if zoneinfo fails we crash loud "
        "instead of silently wrong. <b>4 new tests</b> pin DST behavior.",
        "nenner_engine/tz.py (new), email_scheduler.py, error_ledger.py, fischer_engine.py, dashboard.py, tests/test_tz.py",
    ))
    story.extend(fix_block(
        "4398d30", "Per-module logger hierarchy via __name__",
        "Every module called <font name='Courier'>getLogger(&quot;nenner&quot;)</font> so all logs went "
        "through one flat logger. No way to raise the scheduler to DEBUG "
        "without drowning in dashboard and LLM noise.",
        "17 modules migrated to <font name='Courier'>getLogger(__name__)</font>. cli.py attaches "
        "CentralErrorHandler at the package root (<font name='Courier'>nenner_engine</font>) so "
        "every child propagates up. Opens the door to "
        "<font name='Courier'>NENNER_LOG_SCHEDULER=DEBUG</font>-style filtering.",
        "17 files in nenner_engine/",
    ))
    story.extend(fix_block(
        "880b2ac", "Trim __init__.py — legacy regex parser demoted",
        "<font name='Courier'>RE_ACTIVE</font>, <font name='Courier'>parse_price</font>, <font name='Courier'>parse_email_signals</font> etc. "
        "were marked &quot;Legacy — kept for reference&quot; in the public API "
        "for 8+ months. Superseded by the LLM parser, never used in the "
        "active pipeline.",
        "Removed from <font name='Courier'>__init__.py</font> re-exports and <font name='Courier'>__all__</font>. "
        "test_nenner_engine.py now imports them directly from the "
        "submodule. <font name='Courier'>classify_email</font> and "
        "<font name='Courier'>extract_text_from_email</font> remain public (still in pipeline).",
        "nenner_engine/__init__.py, test_nenner_engine.py",
    ))

    # ---- Audit corrections ----
    story.append(Paragraph("Audit Corrections", H1))
    story.append(Paragraph(
        "After each phase I re-read my own changes adversarially. These "
        "two flaws surfaced on the second pass and were fixed before "
        "wrap.",
        MUTED_BODY,
    ))

    story.extend(fix_block(
        "6cb6fd9", "Equity-stream watchdog: session-local sentinel",
        "The watchdog from 91c3873 blocked on the <b>global</b> stop_event. "
        "When <font name='Courier'>_run_stream</font> returned (reconnect scenario), "
        "the watchdog kept blocking forever. Next <font name='Courier'>_run_stream</font> created "
        "another. Result: one leaked thread per reconnect, accumulating "
        "over the day.",
        "Added a session-local <font name='Courier'>threading.Event</font> set in the finally block. "
        "Watchdog polls global stop_event with 1s timeout and exits "
        "immediately when session_done fires.",
        "nenner_engine/equity_stream.py",
    ))
    story.extend(fix_block(
        "36900e4", "load_env_once: strip surrounding quotes from values",
        "The three <font name='Courier'>_load_env()</font> helpers I removed in 9caee00 didn't "
        "strip quotes, but the inline parser in equity_stream.py (also "
        "consolidated) did. Unification lost quote-stripping for "
        "the DataBento key.",
        "Restored <font name='Courier'>.strip('&quot;').strip(\"'\")</font> as a defensive backport. "
        "Current .env uses unquoted values (no live regression) but a "
        "future edit that pastes a quoted value would have silently "
        "broken auth.",
        "nenner_engine/config.py",
    ))

    story.append(PageBreak())

    # =======================================================================
    # Day 2 — Morning of April 22
    # =======================================================================
    story.append(Paragraph("Day 2 — April 22 Morning Session", H1))
    story.append(Paragraph(
        "~4 hours, 14 more commits. The overnight batch (53 commits including "
        "the 20 from Day 1) was pushed to origin first. The session then "
        "moved through the Phase-3 splits the Day-1 report marked as "
        "&quot;bigger refactors, recommended after a few trading days&quot; — "
        "the risk calculus flipped once the defensive hardening landed cleanly "
        "and morning ingest ran green.",
        BODY,
    ))
    story.append(Paragraph(
        "Highlights in landing order:",
        BODY,
    ))
    story.append(bullet(
        "<b>Phase 3.3 split stock_report.py (1,399 lines)</b> into "
        "<font name='Courier'>stock_report/{data,html,llm,__init__}.py</font>. "
        "Characterization test pins build_stock_report_html byte-for-byte "
        "against a committed golden file — the split changed zero rendered output."
    ))
    story.append(bullet(
        "<b>Phase 3.2 split dashboard.py (1,246 lines)</b> into a "
        "<font name='Courier'>dashboard/</font> package of 5 modules + thin root "
        "<font name='Courier'>dashboard.py</font> shim (launcher contract preserved). "
        "First attempt crashed production with a name-shadow bug (<font name='Courier'>app.py</font> "
        "submodule collided with re-exported Dash instance). Fixed "
        "via rename to <font name='Courier'>dash_app.py</font> + regression test (006ae6a) within "
        "7 minutes of the pause — NSSM auto-restart kicked in cleanly "
        "after the hotfix."
    ))
    story.append(bullet(
        "<b>All 5 pre-existing risks hardened</b> (a95d9ca). Migration error "
        "masking narrowed to duplicate-column only; compute_current_state "
        "wrapped in <font name='Courier'>with conn:</font> for atomicity; scheduler minute-window "
        "replaced with a timedelta-based helper that spans hour boundaries; "
        "alerts.py guards against <font name='Courier'>_thread is None</font>; IMAP now logs a WARN "
        "when Gmail's Authentication-Results header fails to show "
        "dkim=pass or dmarc=pass."
    ))
    story.append(bullet(
        "<b>API SQLite threading fix</b> (ca89510). Surfaced in live api_stderr "
        "at 09:38 this morning: FastAPI's sync-handler threadpool was closing "
        "a per-request connection on a different thread than it was opened on. "
        "One-line <font name='Courier'>check_same_thread=False</font>."
    ))
    story.append(bullet(
        "<b>Phase 4 hygiene</b> (2dd09c7, ffb9bd9). Deleted 4 stale DB files "
        "at repo root and redirected 5 scripts + the TestLiveDatabaseValidation "
        "class to the canonical DataCenter copy. Consolidated 5 magic numbers "
        "(PROXIMITY_DANGER_PCT, PROXIMITY_WARNING_PCT, ALERT_COOLDOWN_MINUTES, "
        "YF_CACHE_TTL_SECONDS, DASHBOARD_REFRESH_MS) into config.py. Enabled "
        "NSSM <font name='Courier'>AppRotateOnline=1</font> to fix the 158 MB dashboard_stderr.log "
        "drift (needs admin one-liner to apply to running services)."
    ))
    story.append(bullet(
        "<b>Stale test fix</b> (f841b59). Three tests in test_nenner_engine.py "
        "were silently failing on main: test_bac_sell pinned BAC's direction "
        "to a Feb 18 snapshot, and TestCancelTrajectory hardcoded a date that "
        "rolled out of the rolling 30-day SQL window. Relaxed and rolling respectively."
    ))
    story.append(bullet(
        "<b>Silent email leak caught</b> (33169c9). TestStanleyBriefGeneration's "
        "three methods patched the LLM but not the SMTP send — every test "
        "run fired real Stanley Morning Briefs to REPORT_RECIPIENT via "
        "<font name='Courier'>postmaster.send_email</font>, which is lazy-imported inside "
        "generate_morning_brief. User reported 6+ unexpected briefs in inbox "
        "before it was diagnosed. Now patched; suite runtime dropped from ~10s "
        "to ~6s."
    ))
    story.append(bullet(
        "<b>49 net new tests across 6 new files.</b> "
        "<font name='Courier'>tests/test_stock_report.py</font> "
        "(3: byte-for-byte HTML golden), "
        "<font name='Courier'>tests/test_compute_current_state.py</font> "
        "(4: atomicity, breach-flip, DATABENTO exclusion), "
        "<font name='Courier'>tests/test_within_window.py</font> "
        "(11: hour-boundary regressions for Risk #3), "
        "<font name='Courier'>tests/test_evaluate_price_alerts.py</font> "
        "(12: DANGER/WATCH thresholds + payload), "
        "<font name='Courier'>tests/test_dashboard_split.py</font> "
        "(15: import smoke, public API, layout component IDs, fetchers), "
        "<font name='Courier'>tests/test_get_prices_with_signal_context.py</font> "
        "(11: pnl, SELL inversion, target matching), "
        "<font name='Courier'>tests/test_email_scheduler_state_machine.py</font> "
        "(18: one-fire-per-day dedup + weekday/window gating)."
    ))

    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "<b>Lessons from Day 2 production incidents:</b>",
        BODY,
    ))
    story.append(bullet(
        "<i>Import smoke tests are necessary but not sufficient.</i> The "
        "dashboard-split smoke test (<font name='Courier'>import dashboard</font>) passed on the "
        "broken commit because it never exercised <font name='Courier'>lifecycle.main()</font>'s "
        "<font name='Courier'>_app_module.app.run(...)</font> path. Added "
        "<font name='Courier'>test_lifecycle_app_module_is_a_module</font> using "
        "<font name='Courier'>isinstance(x, types.ModuleType)</font> — the cheapest test that "
        "would have failed loudly on the name-shadow bug."
    ))
    story.append(bullet(
        "<i>Slow tests are a signal.</i> The Stanley brief test was adding "
        "~4 seconds per run for real SMTP. That should have been a tell for "
        "unpatched network I/O months ago. Now on the radar: audit test "
        "runtimes, investigate anything &gt;100ms for pure unit tests."
    ))
    story.append(bullet(
        "<i>Production addresses in test code is a pattern to lint.</i> "
        "<font name='Courier'>REPORT_RECIPIENT</font> in config.py flowing through an "
        "unpatched send path reached a real inbox. A simple assertion that "
        "test runs never send real email (e.g. via an SMTP monkeypatch in "
        "conftest) would prevent a whole class of this."
    ))

    story.append(PageBreak())

    # ---- Known risks for Phase 4 (now fixed) ----
    story.append(Paragraph("Pre-Existing Risks — Status: All Fixed (Day 2)", H1))
    story.append(Paragraph(
        "These five issues surfaced during the Day-1 red-team audit and "
        "were held for Phase 4. All five landed in commit <font name='Courier'>a95d9ca</font> "
        "on Day 2 (Apr 22 AM). Listed here for historical completeness.",
        MUTED_BODY,
    ))

    risks = [
        (
            "Migration errors silently swallowed",
            "db.py:351-355",
            "<font name='Courier'>for sql in migrations: try: conn.execute(sql) except (OperationalError, IntegrityError): pass</font>. "
            "A genuine schema bug (not &quot;already applied&quot;) is caught and ignored. "
            "After 295e401 the DB stamps CURRENT_SCHEMA_VERSION anyway, "
            "which now more completely masks the issue.",
        ),
        (
            "email_sched._thread can be None if .start() raised",
            "alerts.py:494",
            "<font name='Courier'>if email_sched and not email_sched._thread.is_alive():</font> "
            "crashes with AttributeError if EmailScheduler() succeeded "
            "but .start() raised before setting _thread. Outer try/except "
            "catches, but logs every tick. Thread creation failure is "
            "rare — theoretical issue.",
        ),
        (
            "compute_current_state is not atomic",
            "db.py:compute_current_state",
            "DELETE FROM current_state + loop-of-INSERTs in one transaction. "
            "If the loop crashes AND a subsequent commit fires on the "
            "same connection (from another code path), the DELETE "
            "commits too — leaving current_state empty until the next "
            "full rebuild.",
        ),
        (
            "IMAP FROM header is spoofable",
            "imap_client.py:95",
            "Search filters by <font name='Courier'>FROM \"newsletter@charlesnenner.com\"</font>. "
            "Anyone who can get an email past Gmail's SPF/DKIM filters "
            "with that spoofed sender could inject signals. Mitigation "
            "would require DKIM signature verification in our code.",
        ),
        (
            "Scheduler minute window overflow",
            "email_scheduler.py:411, 425, etc.",
            "<font name='Courier'>MINUTE &lt;= m &lt; MINUTE + 5</font> works for all current hardcoded "
            "minutes (30, 35) but breaks if someone edits MINUTE > 54 — "
            "the +5 overflows the hour boundary and the window never "
            "fires at minute 0-4 of the next hour.",
        ),
    ]
    for title, loc, desc in risks:
        story.append(Paragraph(f"<b>{title}</b>", H2))
        story.append(Paragraph(f"<font name='Courier' size='8'>{loc}</font>", MUTED_BODY))
        story.append(Paragraph(desc, BODY))
        story.append(Spacer(1, 4))

    story.append(PageBreak())

    # ---- Roadmap ----
    story.append(Paragraph("What's Next", H1))

    story.append(Paragraph("Phase 3 — only 3.1 remaining", H2))
    story.append(Paragraph(
        "Day 2 landed Phase 3.2 (dashboard split) and Phase 3.3 (stock_report "
        "split). Phase 3.1 is the last structural item, and the most sensitive "
        "one: FischerDaily's SignalClient consumes NennerEngine's API and "
        "hard-codes <font name='Courier'>result[\"data\"]</font> / <font name='Courier'>result[\"count\"]</font> response shapes. "
        "A query-layer refactor that restructures <font name='Courier'>api.py</font> could break "
        "conviction scoring, Stanley brief, stock report, and auto-cancel "
        "simultaneously on the FischerDaily side.",
        BODY,
    ))
    story.append(bullet(
        "<b>3.1 Canonical query layer.</b> Promote signal_queries.py to "
        "queries.py and migrate all inline SQL from dashboard, stanley, "
        "stock_report, alerts, auto_cancel, anomaly_check. <b>Must not touch "
        "api.py response shapes.</b> Recommend doing this on a branch with a "
        "before/after response-shape diff as the merge gate, and coordinating "
        "with FischerDaily's staging tests."
    ))

    story.append(Paragraph("Phase 4 — status", H2))
    story.append(Paragraph(
        "All items from the original Day-1 Phase-4 list landed on Day 2 "
        "except ruff/mypy enforcement (enabled in pyproject.toml but not "
        "run in CI).",
        MUTED_BODY,
    ))
    story.append(bullet(
        "<font color='#2f855a'><b>[DONE]</b></font> Delete stale DB files at "
        "repo root — commit 2dd09c7."
    ))
    story.append(bullet(
        "<font color='#2f855a'><b>[DONE]</b></font> Dependency pinning + "
        "ruff/mypy config — commit 85fb2a4 (pyproject.toml now declares "
        "16 pinned deps, [tool.ruff], [tool.mypy] with module overrides)."
    ))
    story.append(bullet(
        "<font color='#2f855a'><b>[DONE]</b></font> Test coverage for "
        "compute_current_state (3dc8ebe), get_prices_with_signal_context "
        "(85fb2a4), evaluate_price_alerts + _within_window (02a7c51), "
        "email_scheduler state machine (85fb2a4)."
    ))
    story.append(bullet(
        "<font color='#2f855a'><b>[DONE]</b></font> 5 pre-existing risks "
        "hardened — commit a95d9ca."
    ))
    story.append(bullet(
        "<font color='#2f855a'><b>[DONE]</b></font> Magic numbers centralized "
        "into config.py — commit 2dd09c7."
    ))
    story.append(bullet(
        "<font color='#2f855a'><b>[DONE]</b></font> positions.py and parser.py "
        "audit — decision: keep both. positions.py is intentionally dormant "
        "(workbook path = None); parser.py mixes active helpers with "
        "legacy-regex baseline for LLM parser comparison."
    ))

    story.append(Paragraph("Operational follow-ups (non-code)", H2))
    story.append(bullet(
        "<b>Apply NSSM AppRotateOnline=1 to running services.</b> The "
        "install_service.ps1 update (commit ffb9bd9) covers future installs, "
        "but the current services need the one-time admin command: "
        "<font name='Courier'>nssm set NennerEngineDashboard AppRotateOnline 1</font> "
        "(and same for Monitor + API)."
    ))
    story.append(bullet(
        "<b>Enable ruff/mypy in a pre-commit hook or CI step.</b> Config "
        "is in pyproject.toml; tooling isn't enforced anywhere yet. "
        "<font name='Courier'>pip install -e .[dev]</font> to install them in the venv."
    ))
    story.append(bullet(
        "<b>Truncate or archive the 158 MB dashboard_stderr.log</b> once "
        "AppRotateOnline is live — it'll auto-rotate on the next write, but "
        "the 158 MB historical copy sits there indefinitely."
    ))

    story.append(PageBreak())

    # ---- Commit log ----
    story.append(Paragraph("Commit Log", H1))
    story.append(Paragraph(
        "In reverse chronological order, newest at top:",
        MUTED_BODY,
    ))

    commits = [
        # Day 2 — April 22 morning session
        ("33169c9", "test: stop TestStanleyBriefGeneration from sending real emails"),
        ("85fb2a4", "Phase 4: tests for prices/scheduler + pin deps + ruff/mypy config"),
        ("ffb9bd9", "ops: enable NSSM AppRotateOnline so dashboard log doesn't grow unbounded"),
        ("006ae6a", "fix(dashboard): rename app.py → dash_app.py to break shadow that crashed startup"),
        ("437071f", "Phase 3.2: split dashboard.py into dashboard/ package"),
        ("2dd09c7", "Phase 4 hygiene: stale DB cleanup, magic-number consolidation, dead-module audit"),
        ("02a7c51", "test: cover _within_window + evaluate_price_alerts"),
        ("3dc8ebe", "test: cover atomicity + breach-flip + DATABENTO exclusion in compute_current_state"),
        ("a95d9ca", "Harden 5 pre-existing risks from refactor report"),
        ("f841b59", "test: stop pinning live BAC direction + use rolling date"),
        ("ca89510", "api: check_same_thread=False on per-request SQLite connection"),
        ("a1784aa", "Phase 3.3: split stock_report.py into data/html/llm package"),
        # Day 1 — April 21 overnight session
        ("36900e4", "load_env_once: strip surrounding quotes from values"),
        ("6cb6fd9", "Equity stream: session-local watchdog sentinel"),
        ("880b2ac", "Trim __init__.py: stop re-exporting legacy regex parser"),
        ("a07d9e8", "Ignore Excel artifacts (*.xlsx, *.xlsm)"),
        ("6ead3df", "Untrack accidentally-committed NennerSignals.xlsx"),
        ("4398d30", "Per-module logger hierarchy via getLogger(__name__)"),
        ("beebc12", "Centralize Eastern Time handling in nenner_engine.tz"),
        ("4abb199", "Remove unused detect_signal_changes from alerts"),
        ("91c3873", "Responsive equity-stream shutdown via watchdog thread"),
        ("9230c92", "Thread-safe global caches (dashboard + trade_stats)"),
        ("16fa14c", "Auto-cancel no longer writes synthetic emails"),
        ("295e401", "Schema version tracker + signals.source column (migration 16)"),
        ("cedd215", "Exponential backoff + admin alert on equity stream reconnect"),
        ("863029c", "Exponential backoff for email scheduler restart loop"),
        ("985f2a9", "Retry + rollback on signal-bearing email that parses to 0 signals"),
        ("7a26d79", "Harden dashboard DB connections + honest /health probe"),
        ("9d67945", "Add Market Data page with DataBento live quotes (pre-existing)"),
        ("1d8d0e9", "Migrate imap/alert/equity modules to config.load_env_once()"),
        ("4e9b40c", "Centralize cancel-breach rule in db.is_cancel_breached()"),
        ("4c5e52f", "Fix _salvage_truncated_json bracket balancing"),
        ("9caee00", "Add config.load_env_once() for single .env load"),
    ]
    commit_rows = [["Hash", "Message"]]
    for h, msg in commits:
        commit_rows.append([
            Paragraph(f"<font name='Courier' size='8'><b>{h}</b></font>", BODY),
            Paragraph(msg, BODY),
        ])
    commit_table = Table(commit_rows, colWidths=[0.75 * inch, 5.75 * inch])
    commit_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.HexColor("#e2e8f0")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, LIGHT]),
    ]))
    story.append(commit_table)
    story.append(Spacer(1, 18))

    # ---- Test additions ----
    story.append(Paragraph("New Test Files", H1))
    tests = [
        # Day 2 additions
        ("tests/test_stock_report.py", "3",
         "Byte-for-byte characterization via committed golden HTML/subject files. "
         "Built pre-split to protect the Phase 3.3 refactor; frozen-time clock."),
        ("tests/test_compute_current_state.py", "4",
         "Risk-#2 atomicity rollback pin, already-breached-cancel → implied-reversal "
         "flip, DATABENTO_EQUITY exclusion from cancel-breach check."),
        ("tests/test_within_window.py", "11",
         "Risk-#3 fix pin: hour-boundary span cases (target 8:58 firing at 9:00-9:02) "
         "that would fail on the old MINUTE &lt;= m &lt; MINUTE+5 form."),
        ("tests/test_evaluate_price_alerts.py", "12",
         "DANGER vs WATCH band thresholds, abs() handling for both signal "
         "directions, None-skip paths, alert-payload field contract."),
        ("tests/test_dashboard_split.py", "15",
         "Phase 3.2 safety net: import smoke, public API surface, layout "
         "component-id coverage, card snapshots, data-layer fetchers. Includes "
         "<font name='Courier'>test_lifecycle_app_module_is_a_module</font> — regression for the "
         "name-shadow bug that crashed production."),
        ("tests/test_get_prices_with_signal_context.py", "11",
         "Dashboard-refresh + alert-tick critical path: pnl/cancel_dist/trigger_dist "
         "math, SELL inversion, tickers filter, target UPSIDE/DOWNSIDE matching."),
        ("tests/test_email_scheduler_state_machine.py", "18",
         "One-fire-per-day dedup + weekday/window gating for stock report, "
         "auto-cancel, watchdog, startup catch-up. Side effects patched at module level."),
        # Day 1 additions
        ("tests/test_llm_parser.py", "7",
         "Salvage edge cases — truncated mid-value, trailing comma, nested array, "
         "null literal, complete JSON roundtrip, garbage input, digit terminator regression"),
        ("tests/test_cancel_breach.py", "10",
         "is_cancel_breached — ABOVE/BELOW strict inequality, equality boundary, "
         "None handling for all three inputs, unknown direction"),
        ("tests/test_auto_cancel.py", "4",
         "Breach writes signals row with no emails pollution, dedupe default, "
         "regenerate=True overwrites, close-equal-to-cancel is not a breach"),
        ("tests/test_tz.py", "4",
         "now_et returns tz-aware datetime, today_et returns date, DST observed "
         "in April (UTC-4), EST in January (UTC-5)"),
    ]
    test_rows = [["File", "Count", "Coverage"]]
    for path, count, desc in tests:
        test_rows.append([
            Paragraph(f"<font name='Courier' size='8'>{path}</font>", BODY),
            Paragraph(f"<b>{count}</b>", BODY),
            Paragraph(desc, BODY),
        ])
    test_table = Table(test_rows, colWidths=[2.0 * inch, 0.5 * inch, 4.0 * inch])
    test_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, LIGHT]),
    ]))
    story.append(test_table)
    story.append(Spacer(1, 18))

    story.append(Paragraph(
        f"<i>Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} ET "
        f"by scripts/generate_refactor_report.py</i>",
        MUTED_BODY,
    ))
    return story


def main():
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    doc = SimpleDocTemplate(
        str(OUTPUT_PATH),
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
        title="NennerEngine Refactor Report — April 22, 2026",
        author="Claude Opus 4.7",
    )
    doc.build(build_story())
    print(f"Wrote {OUTPUT_PATH}")
    print(f"Size: {OUTPUT_PATH.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
