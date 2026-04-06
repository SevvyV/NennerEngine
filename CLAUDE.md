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

## Analysis Conventions
- When I say "what does Nenner say about X" → query current_state + cycles + price_targets
- Present trade ideas with: cancel level, distance to cancel, dollar risk, cycle alignment
- Compare instruments by Score from trade_stats.py
- Currency: USD. Benchmark: SPY for equity, flat cash for non-equity.

## Rules Files
`.claude/rules/signal-interpretation.md` — signal hierarchy, cycles, freshness decay

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
