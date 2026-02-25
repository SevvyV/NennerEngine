# NennerEngine Trading System

## Trader Profile
- Background: Fixed Income & CDS trading, bulge bracket investment banks
- Current: Private capital, self-directed, active long/short management
- Signal source: Charles Nenner Research (proprietary cycle-based timing)
- Edge: Institutional risk discipline layered on Nenner cycle timing
- Style: High churn, directional, both sides of the market
- Instruments: Futures, ETFs, single stocks, options overlays

## Signal Database
- Path: E:\Workspace\NennerEngine\nenner_signals.db (SQLite, WAL mode)
- Always use: `sqlite3 "E:/Workspace/NennerEngine/nenner_signals.db"` for queries
- Key tables: current_state, signals, cycles, price_targets, price_history, stanley_knowledge
- current_state = materialized effective signal per ticker (rebuilt after each email parse)
- signals = full history of every parsed signal with origin price, cancel/trigger levels
- cycles = daily/weekly/monthly cycle direction and duration per instrument
- price_history = OHLC from yFinance + T1 RTD snapshots
- stanley_knowledge = learned trading rules and patterns (category, instrument, rule_text, confidence)

## Nenner Signal Grammar (how to interpret signals)
Nenner publishes cycle-based timing signals via email. The LLM parser (Haiku) extracts structured data.

- **Active signal:** "Continues on a BUY signal from X as long as no close below Y"
  - Direction = BUY, origin_price = X, cancel_level = Y
  - The signal stays active UNTIL price CLOSES below Y (intraday doesn't count)
- **Cancelled signal:** "Cancelled the BUY signal from X with the close below Y"
  - Previous BUY is dead. Implied reversal to SELL direction.
- **Trigger (pending):** "A close below Y will give a new SELL"
  - Not active yet. Becomes active when price closes below Y.
- **Price targets:** "There is an upside price target at Z"
  - Profit-taking level. NOT a stop — an objective.
- **Note the change:** "(note the change)" appended to any signal
  - HIGHEST CONVICTION. Nenner explicitly flagging a direction change.
  - These deserve full position size and immediate attention.
- **Cycles:** "The daily cycle is up into Friday. The weekly cycle is down into next week."
  - Daily, weekly, monthly timeframes with direction + approximate duration.
  - Cycles aligned (all same direction) = strongest signal.
  - Cycles conflicting = reduce size or wait.

## Risk Rules (HARD LIMITS)
These are non-negotiable. Every analysis must respect these constraints.

| Rule | Limit | Notes |
|------|-------|-------|
| Max single position | $800,000 | Hard dollar cap regardless of conviction |
| Max gross exposure | $15,000,000 | Total longs + total shorts combined |
| Max correlated cluster | 40% of gross | e.g., all precious metals combined |
| Target book size | 5 positions | Growing from current 1-2; do not over-diversify |
| Stop policy | Nenner cancel levels ARE the stops | No discretionary overrides |
| Cancel respect | Close through = EXIT | Intraday breach without close = HOLD |

## Correlation Clusters (positions that move together)
When checking cluster limits, group these together:
- **Precious Metals:** GC, SI, HG, GLD, SLV, GDXJ, NEM, SIL
- **Equity Indices:** ES, NQ, YM, NYFANG, QQQ, NYA
- **Fixed Income:** ZB, ZN, TLT, FGBL (inverse to equities in risk-off)
- **Energy:** CL, NG, USO, UNG
- **Agriculture:** ZC, ZS, ZW, LBS, CORN, SOYB, WEAT
- **Currencies:** DXY, EUR/USD, GBP/USD, USD/JPY (DXY is the anchor)
- **Crypto:** BTC, ETH, GBTC, ETHE, BITO
- **Single Stocks:** Group by sector — NVDA/TSLA (tech), BAC/C/GS (financials)

## Position Sizing Framework
Used by /position-size skill. Sizes are in dollars.

1. **BASE SIZE** = min(Kelly Fraction x Portfolio, $800,000)
   - Half-Kelly is the default (full Kelly is too aggressive)
   - If Kelly < 0 or insufficient trade history, use minimum size ($100,000)
2. **CYCLE ADJUSTMENT:**
   - Daily + Weekly + Monthly aligned = 1.0x (full base)
   - Daily + Weekly aligned, Monthly opposing = 0.75x
   - Only Daily aligned = 0.50x
   - Cycles conflicting = 0.25x or SKIP
3. **CANCEL DISTANCE ADJUSTMENT:**
   - < 1.0% to cancel = 0.5x (too tight, whipsaw risk)
   - 1.0% - 3.5% to cancel = 1.0x (sweet spot)
   - > 3.5% to cancel = 0.7x (wide stop, large dollar risk if wrong)
4. **CORRELATION PENALTY:**
   - If adding to a cluster already > 25% of gross = reduce to 0.5x
   - If adding to a cluster already > 35% of gross = SKIP (would breach 40%)
5. **CONVICTION MULTIPLIER:**
   - "Note the change" = 1.25x
   - Fresh signal (< 3 days) = 1.0x
   - Aged signal (> 14 days, no target progress) = 0.75x
   - Re-entry on same instrument after cancellation = 0.85x

## Scoring Model (from trade_stats.py)
```
Score = Sharpe(35%) + Kelly(20%) + EV/MaxDD(20%) + WinRate(15%) + Confidence(10%)
```
- Confidence = min(trade_count / 50, 1.0) — penalizes thin data
- Tradeable classes: Single Stock, ETFs (Ag, Crypto, Currency, Energy, FI, PM), PM Stock, Volatility
- Macro cutoff: 2023-02-21, Single stock cutoff: 2025-11-01

## Key Files & Commands
| What | Where |
|------|-------|
| Signal DB | `E:/Workspace/NennerEngine/nenner_signals.db` |
| Prices module | `nenner_engine/prices.py` (yFinance + T1 RTD) |
| Trade stats | `nenner_engine/trade_stats.py` |
| Stanley agent | `nenner_engine/stanley.py` |
| Alert engine | `nenner_engine/alerts.py` |
| Positions | `nenner_engine/positions.py` (disabled: POSITIONS_WORKBOOK=None) |
| Instrument map | `nenner_engine/instruments.py` (89 instruments) |
| LLM parser | `nenner_engine/llm_parser.py` (Haiku) |
| Dashboard | `dashboard.py` (Plotly, port 8050) |
| Market intel | `Newsfeed/market_intelligence.py` |
| DataCenter | `E:/Workspace/DataCenter/Nenner_DataCenter.xlsm` |

**Commands:**
- Parse new emails: `python -m nenner_engine`
- Full backfill: `python -m nenner_engine --backfill`
- Monitor (30s): `python -m nenner_engine --monitor --interval 30`
- Status: `python -m nenner_engine --status`
- Export CSV: `python -m nenner_engine --export`
- History: `python -m nenner_engine --history Gold`
- Dashboard: `python dashboard.py`

## Analysis Conventions
- All trade analysis is for personal use — no compliance overlay needed
- When presenting trade ideas: always show cancel level, distance to cancel, dollar risk, cycle alignment
- When comparing instruments: rank by Score from trade_stats.py
- Default timeframe: daily closes. Intraday only matters for cancel/trigger evaluation.
- Currency: USD. All P&L in dollars.
- Benchmark: S&P 500 total return for equity trades; flat cash for non-equity
- When I say "what does Nenner say about X" → query current_state + cycles + price_targets for that ticker

## Rules Files
Additional trading rules are in `.claude/rules/`:
@.claude/rules/risk-management.md
@.claude/rules/signal-interpretation.md
@.claude/rules/statistical-analysis.md
