# NennerEngine Trading System

## Trader Profile
- Background: Fixed Income & CDS trading, bulge bracket investment banks
- Current: Private capital, self-directed, active long/short management
- Signal source: Charles Nenner Research (proprietary cycle-based timing)
- Edge: Institutional risk discipline layered on Nenner cycle timing
- Style: High churn, directional, both sides of the market
- Instruments: ETFs, single stocks, options overlays (no direct futures)

## Signal Grammar
Nenner publishes cycle-based timing signals via email. The LLM parser (Haiku) extracts structured data.

- **Active signal:** "Continues on a BUY signal from X as long as no close below Y"
  - Direction = BUY, origin_price = X, cancel_level = Y
  - Stays active UNTIL price CLOSES through Y (intraday doesn't count)
- **Cancelled signal:** "Cancelled the BUY signal from X with the close below Y"
  - Previous signal is dead. Implied reversal to opposite direction.
- **Trigger (pending):** "A close below Y will give a new SELL"
  - Not active yet. Becomes active only when price closes through Y.
- **Price targets:** Profit-taking levels, NOT stops.
- **"Note the change":** HIGHEST CONVICTION. Full size, immediate attention.
- **Cycles:** Daily/weekly/monthly direction + duration. All aligned = strongest signal.

Detailed interpretation rules: `.claude/rules/signal-interpretation.md`

## Risk Rules (HARD LIMITS)

| Rule | Limit |
|------|-------|
| Max single position | $800,000 |
| Max gross exposure | $15,000,000 |
| Max correlated cluster | 40% of gross |
| Target book size | 5 positions |
| Stop policy | Nenner cancel levels ARE the stops — no discretionary overrides |
| Cancel respect | Close through = EXIT. Intraday breach without close = HOLD |

Detailed risk rules: `.claude/rules/risk-management.md`

## Correlation Clusters
- **Precious Metals:** GC, SI, HG, GLD, SLV, GDXJ, NEM, SIL
- **Equity Indices:** ES, NQ, YM, NYFANG, QQQ, NYA
- **Fixed Income:** ZB, ZN, TLT, FGBL (inverse to equities in risk-off)
- **Energy:** CL, NG, USO, UNG
- **Agriculture:** ZC, ZS, ZW, LBS, CORN, SOYB, WEAT
- **Currencies:** DXY, EUR/USD, GBP/USD, USD/JPY
- **Crypto:** BTC, ETH, GBTC, IBIT, ETHE, BITO
- **Single Stocks:** Group by sector — NVDA/TSLA (tech), BAC/C/GS (financials)

## Futures-to-ETF Proxy Map
Nenner signals are on futures tickers. Trading uses ETF proxies. Signal data (direction, cancel, cycles, stats) always comes from the futures ticker in the DB.

| Futures | ETF | | Futures | ETF |
|---------|-----|-|---------|-----|
| GC | GLD | | ZB/ZN | TLT |
| SI | SLV | | ES | SPY |
| CL | USO | | NQ | QQQ |
| NG | UNG | | YM | DIA |
| ZC | CORN | | DXY | UUP |
| ZS | SOYB | | EUR/USD | FXE |
| ZW | WEAT | | BTC | GBTC |
| | | | ETH | ETHE |

**No proxy (excluded):** HG, LBS, FGBL, NYFANG, VIX
**Dedup:** ZB + ZN both map to TLT — use the better-scoring signal source.
**Precedence:** When both futures and ETF have signals, prefer the ETF's own signal.

## Watchlist Groups (for /portfolio-construct)
- **equities:** TSLA, BAC, GOOG, MSFT, NVDA, AAPL
- **indices:** QQQ, SPY
- **equities_and_indices:** equities + indices
- **macro:** GLD, SLV, TLT, USO, UNG, CORN, SOYB, WEAT, FXE, UUP, GBTC, IBIT, BITO, ETHE, GDXJ, NEM, SIL, DIA
- **all:** union of all groups

## Position Sizing Framework
1. **BASE** = min(Half-Kelly x Portfolio, $800,000). If Kelly < 0 or thin data → $100,000.
2. **CYCLES:** D+W+M aligned = 1.0x | D+W only = 0.75x | D only = 0.50x | conflicting = 0.25x/SKIP
3. **CANCEL DISTANCE:** <1% = 0.5x (whipsaw) | 1-3.5% = 1.0x (sweet spot) | >3.5% = 0.7x (wide)
4. **CORRELATION:** Cluster >25% gross = 0.5x | >35% = SKIP
5. **CONVICTION:** "Note the change" = 1.25x | Fresh <3d = 1.0x | Aged >14d = 0.75x | Re-entry = 0.85x

## Scoring Model
```
Score = Sharpe(35%) + Kelly(20%) + EV/MaxDD(20%) + WinRate(15%) + Confidence(10%)
```
Confidence = min(trade_count / 50, 1.0). Cutoffs: macro 2023-02-21, single stock 2025-11-01.

## Analysis Conventions
- When I say "what does Nenner say about X" → query current_state + cycles + price_targets
- Present trade ideas with: cancel level, distance to cancel, dollar risk, cycle alignment
- Compare instruments by Score from trade_stats.py
- Currency: USD. Benchmark: SPY for equity, flat cash for non-equity.

## Rules Files
`.claude/rules/risk-management.md` — pre-trade checklist, exits, drawdown, correlation
`.claude/rules/signal-interpretation.md` — signal hierarchy, cycles, freshness decay
`.claude/rules/statistical-analysis.md` — SQS framework, momentum, vol regime, backtest

## DataBento Library Reference

When modifying any DataBento-related code, ALWAYS read the relevant source files in `E:\Workspace\FischerDaily\docs\databento_lib\` first:

- Live client: `docs/databento_lib/databento/live/client.py`
- Session: `docs/databento_lib/databento/live/session.py`
- Protocol: `docs/databento_lib/databento/live/protocol.py`
- Symbology: `docs/databento_lib/databento/common/symbology.py`
- Enums: `docs/databento_lib/databento/common/enums.py`

Key facts already discovered:
- `db.Live` uses CLASS-LEVEL shared asyncio event loop (`_loop`, `_thread`) — all instances in one process share it
- `stop()` is non-blocking — must `block_for_close()` then `terminate()` for full cleanup
- `LiveIterator.__del__` calls `terminate()` — GC timing creates race conditions
- For long-running processes, use subprocess isolation for clean event loops

Strike discovery:
- Strike increments are variable per ticker AND price range (e.g. NVDA: $5 far OTM, $1 near ATM, $2.50 transitional)
- `strike_increments.json` is the production source — instant lookup
- DataBento definition schema (`schema="definition"`, `stype_in="parent"`, `symbols="TICKER.OPT"`) can discover all strikes dynamically but is too slow for real-time use (~48s per ticker)
- If a ticker's increment is wrong in the JSON, update it manually — do NOT add slow API calls to the hot path

## Architecture Reference
Codebase structure, DB schema, file map, Fischer details, commands: `ARCHITECTURE.md`
