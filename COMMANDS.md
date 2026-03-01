# NennerEngine — Command Reference

## CLI Commands (Claude Code only)

| Command | Arguments | Email |
|---------|-----------|:-----:|
| `/morning-scan` | — | Yes |
| `/signal-check` | `TICKER` | Yes |
| `/stat-confirm` | `TICKER` | No |
| `/position-size` | `TICKER DIRECTION` | No |
| `/risk-report` | — | Yes |
| `/trade-journal` | `TICKER ACTION "notes"` | No |
| `/backtest-pattern` | `"description"` | No |
| `/portfolio-construct` | `[GROUP] [COUNT]` | Yes |

### Examples

```
/morning-scan
/signal-check TSLA
/signal-check GLD
/stat-confirm NVDA
/position-size TSLA long
/position-size BAC short
/risk-report
/trade-journal TSLA opened "initial position, cycles aligned"
/trade-journal GLD closed "target hit at 2950"
/trade-journal BAC adjusted "trimmed 50%, cancel tightening"
/backtest-pattern "gold buys when silver aligned"
/backtest-pattern "sell signals after note-the-change"
/portfolio-construct
/portfolio-construct equities 3
/portfolio-construct macro 1
/portfolio-construct equities_and_indices 4
/portfolio-construct all 7
```

## Watchlist Groups (for /portfolio-construct)

| Group | Tickers |
|-------|---------|
| **equities** | TSLA, BAC, GOOG, MSFT, NVDA, AAPL |
| **indices** | QQQ, SPY |
| **equities_and_indices** | TSLA, BAC, GOOG, MSFT, NVDA, AAPL, QQQ, SPY |
| **macro** | GLD, SLV, TLT, USO, UNG, CORN, SOYB, WEAT, FXE, UUP, GBTC, BITO, ETHE, GDXJ, NEM, SIL, DIA |
| **all** | union of equities + indices + macro (default) |

Count: 1-10 (default: 5)

## Futures-to-ETF Proxy Map

| Futures | Trade As | Asset |
|---------|----------|-------|
| GC | GLD | Gold |
| SI | SLV | Silver |
| CL | USO | Crude Oil |
| NG | UNG | Natural Gas |
| ZC | CORN | Corn |
| ZS | SOYB | Soybeans |
| ZW | WEAT | Wheat |
| ZB | TLT | 30Y Bonds |
| ZN | TLT | 10Y Notes |
| ES | SPY | S&P 500 |
| NQ | QQQ | Nasdaq |
| YM | DIA | Dow Jones |
| DXY | UUP | Dollar |
| EUR/USD | FXE | Euro |
| BTC | GBTC | Bitcoin |
| ETH | ETHE | Ethereum |
| HG | — | Copper (excluded) |
| LBS | — | Lumber (excluded) |
| FGBL | — | Bunds (excluded) |
| NYFANG | — | FANG Index (excluded) |
| VIX | — | VIX (excluded) |

## Desktop Queries (Claude Desktop via MCP)

```
"What are the active Nenner signals?"
"Show me signal detail for GC"
"Current prices for GC, ES, SI?"
"Show me trade stats"
"Cycles for ES?"
"Stanley knowledge rules about gold?"
"Refresh prices for GC, ES"
"30 days of price history for NQ"
```

## Desktop Shortcuts

| Shortcut | Action |
|----------|--------|
| **NennerEngine Trading Desk** | Launch Claude Code CLI |
| **Kill Claude** | Force-kill all Claude processes |
