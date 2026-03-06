# NennerEngine Architecture

## Signal Database
- **Path:** `E:\Workspace\NennerEngine\nenner_signals.db` (SQLite, WAL mode)
- **CLI:** `sqlite3 "E:/Workspace/NennerEngine/nenner_signals.db"`

| Table | Purpose |
|-------|---------|
| current_state | Materialized effective signal per ticker (rebuilt after each email parse) |
| signals | Full history of every parsed signal with origin price, cancel/trigger levels |
| cycles | Daily/weekly/monthly cycle direction and duration per instrument |
| price_targets | Upside/downside price targets per ticker |
| price_history | OHLC from yFinance + T1 RTD snapshots |
| stanley_knowledge | Learned trading rules and patterns |
| stanley_briefs | Generated morning briefs (deduped by email_id) |
| alert_log | Fired alert history |
| custom_price_alerts | User-defined price threshold alerts |
| fischer_recommendations | Fischer Options scan results |

## Key Files

| Module | Purpose |
|--------|---------|
| `nenner_engine/prices.py` | yFinance + T1 RTD price feeds |
| `nenner_engine/trade_stats.py` | Scoring model, trade extraction, Kelly/Sharpe |
| `nenner_engine/postmaster.py` | ALL email formatting and delivery |
| `nenner_engine/stanley.py` | LLM morning brief generator |
| `nenner_engine/alerts.py` | Alert engine with evaluator registry |
| `nenner_engine/alert_dispatch.py` | Notification plumbing (Telegram for Fischer, toast, DB logging) |
| `nenner_engine/email_scheduler.py` | Background scheduler (email check, auto-cancel, Fischer, stock report) |
| `nenner_engine/instruments.py` | 89-instrument map |
| `nenner_engine/llm_parser.py` | Haiku email parser |
| `nenner_engine/positions.py` | Position tracker (disabled: POSITIONS_WORKBOOK=None) |
| `nenner_engine/config.py` | Shared constants (emails, LLM model, SMTP, schedule, paths) |
| `nenner_engine/auto_cancel.py` | Automatic signal cancellation on close breach |
| `dashboard.py` | Plotly dashboard, port 8050 |
| `Newsfeed/market_intelligence.py` | Market intelligence feed |

## Commands
```
python -m nenner_engine                        # Parse new emails
python -m nenner_engine --backfill             # Full backfill
python -m nenner_engine --monitor --interval 30  # Alert monitor
python -m nenner_engine --status               # Current state
python -m nenner_engine --export               # Export CSV
python -m nenner_engine --history Gold         # Signal history
python dashboard.py                            # Launch dashboard
```

## Email Routing — Postmaster
ALL email formatting and delivery MUST go through `nenner_engine/postmaster.py`.
- Import `CLR_*` and `FONT` for colors/fonts
- Call `wrap_document(body_html, title=..., subtitle=...)` for document shell
- Call `send_email(subject, html_body)` for delivery
- Call `markdown_to_html(md_text)` for markdown reports
- Never build standalone HTML templates or use `smtplib` directly

## Alert Engine
Evaluator registry pattern. Add new alert types without touching the monitor loop:

```python
from nenner_engine.alerts import register_evaluator, make_alert

@register_evaluator
def check_something(conn, prices):
    # prices = {ticker: float}
    if condition:
        return [make_alert("TKR", "Name", "MY_TYPE", "DANGER", "msg", price)]
    return []
```

Built-in evaluators: custom price alerts (above/below thresholds from DB).
Dispatch: cooldown check → optional toast → DB log.

## Fischer Options — Daily Portfolio v2
Covered-put-only options overlay on high-vol names with tri-weekly (Mon/Wed/Fri) expiries.

**Universe:** 15 tickers, flat list (no always/macro split):
AAPL, AMZN, AVGO, GOOGL, IWM, META, MSFT, NVDA, QQQ, TSLA, GLD, MSTR, SLV, UNG, USO

**Ranking:** Top 10 by `|total_theta_cost| / max_profit` ascending.
**All metrics in dollars** — Premium $, MaxProfit $, ThetaCost $ (theta x shares x DTE).
**Strikes:** 7 per ticker (OTM-biased, at most 1 ITM). Puts bias down, calls bias up.

| File | Purpose |
|------|---------|
| `fischer_engine.py` | BSM/BAW pricing, Greeks, EV |
| `fischer_chain.py` | Option chain fetcher |
| `fischer_scanner.py` | Universe, ranking, top picks selection |
| `fischer_daily_report.py` | Email report generator |
| `fischer_signals.py` | Nenner signal integration |
| `fischer_subscribers.py` | Subscriber CRUD, IMAP polling |
| `fischer_reliability.py` | 9 safeguards (queue, cache, dedup, shutdown, health, market hours, TZ) |
| `strike_increments.json` | Per-ticker strike step sizes |

**Workbook:** `E:\Workspace\DataCenter\OptionChains_Beta.xlsm`
**Builder:** `E:\Workspace\DataCenter\build_option_chains.py`
**Readers:** `_oc_reader.py` (NE) and `oc_reader.py` (FD) — BLOCK_SPACING=10, STRIKES=7, ROWS_PER_BLOCK=9

### Covered Put P&L
- Covered put = Short stock + Sell put simultaneously
- If assigned: buy at strike → closes the short → FLAT
- Max profit = (Spot - Strike) + Premium
- Assignment is the BEST outcome (not worst)

### Fischer Reliability
Initialized in `EmailScheduler.__init__()`. All integration uses `if rel:` guards.

| # | Safeguard | What it does |
|---|-----------|-------------|
| S1 | RequestQueue | Serialized job queue (depth 10), drops oldest on overflow |
| S2 | ResultCache | 90s TTL cache, avoids redundant scans |
| S3 | ResilientIMAPPoller | Exponential backoff (30s→300s), admin alert after 3 failures |
| S4 | ScanGuard | Aborts scan + alerts admin when >8/15 tickers fail |
| S5 | SendDeduplicator | Prevents duplicate sends per (email, report_type, job_id) |
| S6 | GracefulShutdown | SIGINT/SIGTERM handler, 120s drain |
| S7 | HealthLogger | 1 line/min to logs/, 7-day rotation |
| S8 | MarketHoursGuard | Defers off-hours requests, flags stale tickers 4:00–4:15 PM |
| S9 | TZ Enforcement | All timestamps tz-aware America/New_York |

## DataCenter
- **Workbook:** `E:\Workspace\DataCenter\Nenner_DataCenter.xlsm`
- **OptionChains:** `E:\Workspace\DataCenter\OptionChains_Beta.xlsm`
- **Builder:** `E:\Workspace\DataCenter\build_option_chains.py`
