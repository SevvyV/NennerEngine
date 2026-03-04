# FISCHER

## Options Intelligence Agent

Named for Fischer Black | Black-Scholes-Merton Pricing Framework
Vartanian Capital Management, LLC
Technical Specification | v2.0

---

## 1. Mission & Scope

Fischer is a set of options intelligence tools built to serve Vartanian Capital Management's short-dated equity option strategies. His primary mandate is generating optimal trade parameters for **covered puts** on ETFs and large-cap individual equities, with a focus on the 0DTE through 7-day expiry window.

Fischer integrates Black-Scholes-Merton and Barone-Adesi-Whaley pricing models with live Thomson One option chain data and Charles Nenner Research signal intelligence to produce expected-value-ranked trade recommendations.

**Fischer operates as an MCP tool provider** registered alongside the existing `nenner_trading` MCP server. Claude Code — the existing conversational interface already used for Nenner signal analysis — invokes Fischer tools naturally within the same conversation. There is no separate agent process.

Fischer does not execute trades. He recommends, ranks, explains, and warns.

### 1.1 Scope Boundaries

- **In scope (v2.0):** Covered puts, covered calls — single-leg strategies only.
- **Planned for later:** Vertical spreads, collars, diagonals, multi-leg strategies.
- **Permanently out of scope:** Trade execution, broker API connectivity.

---

## 2. Theoretical Framework

### 2.1 Pricing Models

**Black-Scholes-Merton (BSM) — European-Style Baseline**

Fischer uses BSM as the foundational pricing model. For short-dated options on non-dividend-paying underlyings, BSM provides sufficient accuracy and fast computation required for real-time chain scanning.

The BSM call price formula: `C = S·N(d1) − K·e^(−rT)·N(d2)`
The BSM put price formula: `P = K·e^(−rT)·N(−d2) − S·N(−d1)`

Where:

```
d1 = [ln(S/K) + (r + σ²/2)·T] / (σ·√T)
d2 = d1 − σ·√T
```

- S = underlying spot (mid of bid/ask)
- K = strike
- r = risk-free rate
- T = time to expiry in years (see §2.5 for precision rules)
- σ = implied volatility
- N(·) = standard normal CDF

**Barone-Adesi-Whaley (BAW) — American Early Exercise Premium**

For American puts on dividend-paying ETFs (SPY, QQQ, GLD) and individual stocks, BAW is applied to capture the early exercise premium. This matters most for deep ITM puts near ex-dividend dates and for high-rate environments. Fischer automatically selects BAW when: (a) the underlying pays dividends, and (b) the put is more than 2% ITM.

**0DTE Volatility Smile Adjustment**

BSM assumes a flat volatility surface. With zero or near-zero time to expiry, gamma becomes extreme near ATM and the lognormal distribution systematically underestimates tail probabilities. Fischer applies a smile interpolation from the live option chain: for each expiry, Fischer fits a second-order polynomial to the IV surface across strikes (IV as a function of moneyness) and uses the smile-adjusted IV at the target strike rather than a flat surface estimate. This produces more accurate probability estimates for the short-dated strikes that matter most.

### 2.2 The Greeks — Fischer's Instrument Panel

| Greek | Symbol | Definition | Fischer Usage |
|-------|--------|-----------|---------------|
| Delta | Δ | Rate of change of option price vs. underlying | Primary strike selector; proxy for probability ITM |
| Gamma | Γ | Rate of change of delta vs. underlying | Critical for 0DTE; flags gamma risk near ATM |
| Theta | Θ | Time decay per calendar day | Daily premium erosion; key for short-dated positions |
| Vega | ν | Sensitivity to 1% change in IV | IV environment scoring; avoid selling into IV crush |
| Rho | ρ | Sensitivity to interest rate change | Minor for <30 DTE; included for completeness |

**Special 0DTE Gamma Warning**

When DTE < 1 and the strike is within 0.5% of spot, Fischer automatically flags `Gamma Risk: ELEVATED`. At this proximity with near-zero time, gamma can exceed 50 and small moves produce large delta swings. Fischer will display the dollar P&L impact of a 0.5% and 1.0% adverse move against the position before confirming any recommendation.

### 2.3 Implied Volatility Solver

Thomson One publishes bid, ask, last, OI, and volume for option chains but does not export IV. Fischer owns IV calculation entirely. The IV solver uses Newton-Raphson iteration:

```
σ_(n+1) = σ_n − [BSM_Price(σ_n) − Market_Price] / Vega(σ_n)
```

- Convergence tolerance: `|BSM_Price(σ) − Market_Price| < $0.001`
- Maximum iterations: 100
- Initial seed: `σ_0 = √(2π/T) · (Market_Price / S)` — reasonable starting point regardless of moneyness
- Fischer uses the mid-market price `(bid+ask)/2` as the target market price for IV calculation
- Bid-IV and ask-IV are computed and stored separately for spread width diagnostics

### 2.4 Implied Volatility Surface Persistence

Fischer stores every IV computation to enable historical analysis and faster re-pricing.

**Table: `iv_surface`**

| Column | Type | Description |
|--------|------|-------------|
| ticker | TEXT | Underlying ticker |
| expiry | DATE | Option expiry date |
| strike | REAL | Strike price |
| type | TEXT | 'P' or 'C' |
| iv_bid | REAL | IV computed from bid price |
| iv_ask | REAL | IV computed from ask price |
| iv_mid | REAL | IV computed from mid price |
| fit_iv | REAL | Polynomial-fitted smile IV at this strike |
| underlying_price | REAL | Spot price at time of computation |
| timestamp | TEXT | ISO 8601 timestamp |

Unique constraint on `(ticker, expiry, strike, type, DATE(timestamp))`.

This enables: (a) IV rank/percentile over trailing windows, (b) detection of IV regime changes, (c) faster re-pricing when only a few strikes have updated.

### 2.5 Time-to-Expiry Precision

Options expire at **4:00 PM Eastern Time**. For 0DTE and short-dated options, fractional day precision is critical.

```
T = (expiry_datetime − now_eastern) / timedelta(days=365.25)
```

Where `expiry_datetime` is set to 16:00 ET on the expiration date. Fischer uses `zoneinfo.ZoneInfo("America/New_York")` for all time calculations. When T < 1/365 (less than 1 day), Fischer switches to minute-level precision: `T = minutes_remaining / 525960`.

---

## 3. Expected Value Engine

### 3.1 The Core EV Formula

Fischer's primary optimization objective is Net Expected Value per position.

**For a cash-secured short put (no equity position yet):**

```
EV = Premium_Collected − P(Assignment) × E[K − S_T | S_T < K]
```

Where `P(Assignment) = N(−d2)` under BSM (smile-adjusted for 0DTE), and `E[K − S_T | S_T < K]` is the expected value of (K − S_T) conditional on S_T < K at expiry, integrated over the left tail of the risk-neutral lognormal distribution.

**For a covered put (already long the underlying):**

The relevant P&L is the combined equity + option position. If equity was purchased at entry price E:

```
EV = Premium_Collected − P(Assignment) × E[max(E − S_T, 0) | S_T < K] + P(Expire_Worthless) × 0
```

The legged-in framework (§3.4) is the primary EV model for covered puts. The entry price shifts the effective breakeven and changes which strikes produce positive EV. Fischer always asks for the equity entry price when evaluating covered puts and defaults to current spot if none is provided.

**For a short call (covered call seller), the same framework applies with the right tail.**

### 3.2 Full Output Fields

| Output Field | Formula | Description | Notes |
|-------------|---------|-------------|-------|
| Premium Collected | Bid price × 100 × contracts | Gross credit received | Uses bid — assume filled at bid for conservatism |
| P(Expire Worthless) | N(d2) for calls; N(−d2) for puts | BSM risk-neutral probability OTM at expiry | Smile-adjusted for 0DTE |
| Expected Assignment Loss | P(ITM) × E[Intrinsic \| ITM] | Prob-weighted loss if assigned | Integrates tail of lognormal distribution |
| Net EV per Contract | Premium − Exp. Assignment Loss | True edge per contract | Core ranking metric |
| Net EV per Position | Net EV × contracts | Scaled to position size | Bounded by max capital at risk input |
| Nenner Score | 0–100 composite | Signal alignment bonus/penalty | Multiplies EV confidence, not EV directly |
| Earnings Flag | CLEAN / STRADDLES / EARNINGS TODAY | Whether expiry crosses an earnings announcement | See §5.4 |
| Implied Move | ATM straddle / spot | Market-priced expected earnings move | Only shown when earnings flag is not CLEAN |

### 3.3 Strike Ranking Logic

Fischer evaluates all strikes within the **2.75% moneyness band** (matching the Options_RT ladder) and the 0–7 DTE window, computes Net EV per contract for each, and returns a ranked table sorted by Net EV descending. The top recommendation is highlighted.

Fischer also flags any strike where:
- Bid-ask spread exceeds 15% of mid price → **liquidity warning**
- Open interest is below 100 contracts → **thin market warning**
- Gamma risk is elevated per the 0DTE rule above → **gamma warning**

### 3.4 Legged-In Trade Mode

When you specify an equity entry price that differs from current market, Fischer computes your effective breakeven and adjusts the EV calculation accordingly. For a covered put where the equity was purchased at a price different from current spot, the put strike selection shifts to protect the actual cost basis rather than current market. Fischer will display both the market-neutral optimal strike and the cost-basis-adjusted optimal strike side by side.

---

## 4. Options_RT — The Live Option Chain

### 4.1 Design Philosophy

Options_RT is a new worksheet added to **`Nenner_DataCenter.xlsm`** — the same workbook that already hosts `Equities_RT` and `Futures_FX_RT`. It follows the identical Thomson One RTD pattern used throughout the DataCenter infrastructure.

The sheet presents a **pre-built strike ladder** centered on the at-the-money (or closest-to-ATM) strike, extending ±2.75% from spot. Only two inputs change between tickers: the **ticker** (cell B1) and the **strike increment** (cell B2). Everything else — RIC construction, expiry selection, RTD formulas — cascades automatically.

### 4.2 Sheet Layout

```
┌──────────────────────────────────────────────────────────────────────┐
│  A        B          C         D         E         F         G      │
├──────────────────────────────────────────────────────────────────────┤
│1 Ticker:  SPY                                                       │
│2 Incr:    1          [dropdown: 0.5, 1, 2.5, 5]                     │
│3 Spot:    =RTD("tf.rtdsvr",,"Q",$B$1,"BID")                        │
│4 r:       =RTD("tf.rtdsvr",,"Q","^IRX","LAST")/100                 │
│5 Div Yld: [manual — annual yield for BAW]                           │
│6 Updated: =NOW()                                                    │
│7                                                                    │
│  ── PUTS ─────────────────────────────────────────────────────────  │
│  Expiry 1: [date]  DTE: [days]                                      │
│  Strike | RIC | Bid | Ask | Last | OI | Volume                      │
│  [ATM-N*incr through ATM+N*incr — enough rows to cover ±2.75%]     │
│                                                                     │
│  Expiry 2: [date]  DTE: [days]                                      │
│  [same strike ladder]                                                │
│                                                                     │
│  Expiry 3: [date]  DTE: [days]                                      │
│  [same strike ladder]                                                │
│                                                                     │
│  ── CALLS ────────────────────────────────────────────────────────  │
│  [mirror of puts section with same strikes and expiries]            │
│                                                                     │
└──────────────────────────────────────────────────────────────────────┘
```

### 4.3 Strike Ladder Construction

The ladder is built from the ATM strike outward in both directions, using the strike increment in B2.

```
ATM_strike = ROUND(Spot / Increment, 0) * Increment
Lowest_strike = ATM − CEILING(ATM × 0.0275 / Increment, 1) × Increment
Highest_strike = ATM + CEILING(ATM × 0.0275 / Increment, 1) × Increment
```

**Example:** SPY at 597, increment = 1:
- 2.75% of 597 = 16.42 → 17 strikes each direction
- Ladder: 580, 581, 582, ... 597, ... 612, 613, 614
- Total: ~35 strikes per expiry

**Example:** AAPL at 245, increment = 2.5:
- 2.75% of 245 = 6.74 → 3 increments each direction
- Ladder: 237.5, 240, 242.5, 245, 247.5, 250, 252.5
- Total: ~7 strikes per expiry

The number of rows is fixed at a generous maximum (e.g., 40 per expiry block). Unused rows at the edges return blank when the strike falls outside the ±2.75% window. This avoids needing to resize the sheet when switching tickers.

### 4.4 Expiry Selection

Uses the same auto-rolling expiry system already proven in `Nenner_Positions.xlsm`:

```excel
B8  = =TODAY()+CHOOSE(WEEKDAY(TODAY(),2),0,1,0,1,0,2,1)     ' Next Mon/Wed/Fri
B9  = =B8+CHOOSE(WEEKDAY(B8,2),2,0,2,0,3,0,0)              ' Following expiry
B10 = =B9+CHOOSE(WEEKDAY(B9,2),2,0,2,0,3,0,0)              ' Third expiry
```

DTE computed as `= expiry_date − TODAY()`.

### 4.5 Option RIC Construction

Follows the existing OptionCode sheet pattern already in `Nenner_DataCenter.xlsm`:

```excel
=CONCATENATE($B$1, VLOOKUP(B8, OptionCode!$F$5:$H$527, 3, FALSE), strike_cell)
```

Where column 3 = put code, column 2 = call code. The OptionCode sheet (already present) maps every Friday expiry date to its Thomson One symbol code.

Example: SPY put, expiry Feb 27 2026, strike 590 → `SPY2627N590`

### 4.6 RTD Field Formulas Per Strike Row

Each strike row contains these RTD formulas referencing the constructed RIC:

| Column | Header | Formula |
|--------|--------|---------|
| A | Strike | `=ATM + (row_offset × $B$2)` |
| B | RIC | `=CONCATENATE($B$1, VLOOKUP(...), A{row})` |
| C | Bid | `=IFERROR(RTD("tf.rtdsvr",,"Q",$B{row},"BID"),"")` |
| D | Ask | `=IFERROR(RTD("tf.rtdsvr",,"Q",$B{row},"ASK"),"")` |
| E | Last | `=IFERROR(RTD("tf.rtdsvr",,"Q",$B{row},"LAST"),"")` |
| F | OI | `=IFERROR(RTD("tf.rtdsvr",,"Q",$B{row},"OPENINT"),"")` |
| G | Volume | `=IFERROR(RTD("tf.rtdsvr",,"Q",$B{row},"VOLUME"),"")` |

All RTD formulas are wrapped in `IFERROR` to handle stale or missing RICs gracefully.

### 4.7 Risk-Free Rate — Automated

Cell B4 uses the 13-week T-bill rate from Thomson One:

```excel
=RTD("tf.rtdsvr",,"Q","^IRX","LAST")/100
```

`^IRX` is the CBOE Interest Rate 13-Week T-Bill index. This eliminates the manual daily update required in v1.0. Fallback: if `^IRX` is unavailable, Fischer uses the last known value from the database.

### 4.8 Chain Size Budget

Thomson One RTD has practical limits on simultaneous cell subscriptions. The Options_RT sheet budget:

| Component | Count |
|-----------|-------|
| Strikes per expiry | 40 max |
| Expiries | 3 |
| Fields per strike | 5 (Bid, Ask, Last, OI, Volume) |
| Sections (Puts + Calls) | 2 |
| **Total RTD cells** | **40 × 3 × 5 × 2 = 1,200** |

Plus ~10 cells for spot, rate, and metadata. Total: ~1,210 RTD subscriptions. This is well within Thomson One's practical limit of ~3,000 per workbook, even accounting for the existing Equities_RT (~200) and Futures_FX_RT (~200) subscriptions.

### 4.9 Building the Sheet — `build_options_rt.py`

A new Python script in `E:\Workspace\DataCenter\` that constructs the Options_RT worksheet using the same `win32com` pattern as `rebuild_option_board.py`. The script:

1. Opens the live `Nenner_DataCenter.xlsm` via `win32com`
2. Creates or clears the `Options_RT` sheet
3. Writes the header cells (B1=ticker, B2=increment dropdown, B3=spot formula, B4=rate formula)
4. For each of 3 expiry blocks × 2 sections (puts, calls):
   - Writes expiry date reference and DTE formula
   - For each of 40 strike rows: writes strike formula, RIC CONCATENATE, and 5 RTD formulas
5. Applies formatting (alternating rows, header colors, number formats)
6. Adds data validation dropdown on B2 for strike increments

This is a one-time build script. Once the sheet exists, switching tickers only requires changing cell B1. The strike ladder recomputes automatically because all strike formulas reference the spot price in B3.

---

## 5. Fischer's Chain Reader — `chain_reader.py`

### 5.1 xlwings Read Pattern

Fischer reads the Options_RT sheet via xlwings using the identical pattern established by `prices.py` for `Equities_RT` and `Futures_FX_RT`:

```python
import xlwings as xw

def read_option_chain(ticker: str | None = None) -> pd.DataFrame:
    """Read the live Options_RT sheet from Nenner_DataCenter.xlsm.

    Returns a DataFrame with columns:
    expiry, strike, type, bid, ask, last, oi, volume, spot, rate, div_yield
    """
    wb = xw.Book("Nenner_DataCenter.xlsm")  # connects to already-open instance
    ws = wb.sheets["Options_RT"]

    # Read header cells
    sheet_ticker = ws.range("B1").value
    spot = ws.range("B3").value
    rate = ws.range("B4").value
    div_yield = ws.range("B5").value or 0.0
    updated = ws.range("B6").value

    # Validate
    if ticker and sheet_ticker != ticker:
        raise StaleChainError(f"Options_RT shows {sheet_ticker}, requested {ticker}")

    # ... iterate expiry blocks, read strike rows into DataFrame
```

The reader validates:
- Ticker matches the requested underlying (or warns)
- Spot price is non-null and positive
- At least one expiry block has non-empty bid/ask data
- Data freshness: warns if `Updated` timestamp is more than 5 minutes old

### 5.2 yFinance Fallback — Paper Mode

When Thomson One is unavailable (evenings, weekends, or when the workbook is closed), Fischer falls back to yFinance option chain data:

```python
import yfinance as yf

def read_yfinance_chain(ticker: str) -> pd.DataFrame:
    """Fallback: fetch option chain from yFinance for paper/backtest mode."""
    t = yf.Ticker(ticker)
    expirations = t.options  # list of expiry date strings
    # Filter to 0-7 DTE
    # For each expiry: t.option_chain(expiry) returns calls and puts DataFrames
    # Columns: strike, bid, ask, lastPrice, volume, openInterest, impliedVolatility
```

yFinance data is delayed (~15 min) and lacks the precision of live RTD, but it enables:
- End-to-end testing without Thomson One
- Weekend/after-hours analysis and backtesting
- Development and debugging of the pricing engine

Fischer clearly labels output as `[LIVE]` or `[DELAYED — yFinance]` so the trader always knows the data source.

### 5.3 Ex-Dividend Awareness

Fischer checks the ex-dividend date for the underlying **only when it falls before the option's expiry date**. This is the only case where early exercise risk from dividends is relevant.

```python
def check_ex_div_risk(ticker: str, expiry: date) -> dict | None:
    """Returns ex-div info only if ex-date falls before option expiry."""
    t = yf.Ticker(ticker)
    cal = t.calendar  # contains ex-dividend date
    if cal and cal.get("Ex-Dividend Date"):
        ex_date = cal["Ex-Dividend Date"]
        if ex_date < expiry:
            return {
                "ex_date": ex_date,
                "dividend_amount": cal.get("Dividend"),
                "days_before_expiry": (expiry - ex_date).days,
                "warning": f"Ex-div {ex_date} falls before {expiry} expiry — "
                           f"early assignment risk elevated for ITM puts"
            }
    return None
```

When triggered, Fischer displays the warning alongside the affected expiry block. No warning is shown for expiries that mature before the ex-div date.

### 5.4 Earnings Announcement Awareness

Earnings releases create a distinct IV environment. In the days leading up to an announcement, implied volatility expands as the market prices in the unknown move — then collapses immediately after the release ("IV crush"). For a short premium seller, this creates a dual-edged situation:

- **Selling before earnings** captures elevated premium but carries full gap risk through the announcement.
- **Selling after earnings** (post-crush) collects less premium but faces a normalized distribution again.

Fischer checks for upcoming earnings on every scan and adjusts its output accordingly.

**Data Sources (priority order):**

1. **Thomson One RTD (primary for equities in Options_RT):** FID `593285` (`FID_CUR_EST_EPS_DATE`) returns the projected earnings announcement date directly from the RTD feed. Available on the `RTD_Config` sheet. Fischer reads this from the Options_RT header area when the workbook is open.
2. **yFinance (fallback):** `Ticker.calendar` provides next confirmed or estimated earnings date. Used when Thomson One is unavailable or for tickers not loaded in the workbook.

```python
def check_earnings_proximity(ticker: str, expiries: list[date]) -> dict | None:
    """Check if an earnings announcement falls within the option window.

    Returns earnings info when the announcement date falls on or before
    the latest expiry being evaluated.
    """
    t = yf.Ticker(ticker)
    cal = t.calendar  # contains 'Earnings Date' (next confirmed or estimated)

    # yFinance returns up to 2 earnings dates (range estimate)
    # Use the earliest date as the conservative assumption
    earnings_dates = cal.get("Earnings Date") if cal else None
    if not earnings_dates:
        return None

    earnings_date = min(earnings_dates) if isinstance(earnings_dates, list) else earnings_dates
    latest_expiry = max(expiries)

    if earnings_date <= latest_expiry:
        days_to_earnings = (earnings_date - date.today()).days
        return {
            "earnings_date": earnings_date,
            "days_away": days_to_earnings,
            "confirmed": len(set(earnings_dates)) == 1,  # single date = confirmed
            "affects_expiries": [e for e in expiries if e >= earnings_date],
            "clean_expiries": [e for e in expiries if e < earnings_date],
        }
    return None
```

**How Fischer Uses Earnings Data:**

Fischer classifies every expiry into one of three categories:

| Category | Condition | Fischer Behavior |
|----------|-----------|-----------------|
| **CLEAN** | Expiry falls before earnings date | Normal EV ranking. No earnings flag. |
| **STRADDLES EARNINGS** | Earnings date falls on or before expiry | Prominent warning: `EARNINGS {date} — IV elevated, gap risk through announcement`. Fischer still ranks strikes but adds an `[EARNINGS]` flag to every affected row and computes an **earnings-adjusted adverse move** (see below). |
| **EARNINGS TODAY** | Earnings date is today and option expires today (0DTE) | `EXTREME CAUTION` banner. Fischer displays the recommendation but prepends: `Earnings release today — IV crush will occur post-announcement. Premium is elevated but binary risk is maximum.` |

**Earnings-Adjusted Adverse Move:**

For expiries that straddle an earnings announcement, the standard BSM-based probability estimates understate tail risk because earnings moves are not lognormally distributed — they are closer to a bimodal distribution (beat or miss). Fischer supplements the standard output with an **implied earnings move** derived from the ATM straddle price:

```
Implied_Move = (ATM_Call_Mid + ATM_Put_Mid) / Spot
```

This is the market's priced-in expected move for the earnings event. Fischer displays this alongside every affected expiry:

```
EARNINGS: AAPL reports Feb 27 (2 days) — Market implies ±3.2% move ($7.84)
          Expiries on or after Feb 27 carry full announcement risk.
          Pre-earnings expiries (Feb 26) are CLEAN.
```

**Interaction with the EV Engine:**

Fischer does **not** refuse to recommend trades that straddle earnings — elevated IV means elevated premium, which can produce high EV. But he ensures the trader sees:

1. Which expiries are clean vs. which straddle earnings
2. The market-implied move size
3. The dollar P&L at the implied move (what happens if the stock moves exactly the expected amount against the position)
4. A clear `[EARNINGS]` flag on every affected row in the ranked table

This gives the trader the information to make the risk/reward judgment — Fischer flags, he doesn't block.

---

## 6. Nenner Signal Integration

### 6.1 Direct Database Access — No Bridge Layer

Fischer queries the existing Nenner signal database directly using functions from `nenner_engine.db`. The `nenner_bridge.py` module from v1.0 is eliminated — it would duplicate infrastructure that already exists.

Fischer imports:
- `nenner_engine.db.get_connection()` — SQLite connection
- `nenner_engine.instruments.INSTRUMENT_MAP` — the 89-instrument universe
- Queries against `current_state`, `signals`, and `cycles` tables directly

### 6.2 Ticker Mapping — ETF to Futures

Fischer uses the **Futures-to-ETF Proxy Map** already defined in `CLAUDE.md` and reflected in `instruments.py`:

| ETF/Equity | Nenner Ticker | Notes |
|-----------|--------------|-------|
| SPY | ES | S&P 500 |
| QQQ | NQ | Nasdaq |
| DIA | YM | Dow |
| GLD | GC | Gold |
| SLV | SI | Silver |
| TLT | ZB | Treasuries (also ZN) |
| USO | CL | Crude |
| UNG | NG | Natural Gas |
| CORN | ZC | Corn |
| SOYB | ZS | Soybeans |
| WEAT | ZW | Wheat |
| FXE | EUR/USD | Euro |
| UUP | DXY | Dollar Index |
| GBTC | BTC | Bitcoin |
| ETHE | ETH | Ethereum |

For individual stocks (AAPL, TSLA, BAC, etc.) where Nenner covers the stock directly, the ticker maps to itself. For stocks not covered by Nenner, the conviction score defaults to 50 (neutral) and Fischer displays: `No Nenner signal available for [TICKER] — proceeding on options math only.`

### 6.3 Nenner Conviction Score

The conviction score is a 0–100 composite:

| Condition | Points | Effect |
|-----------|--------|--------|
| Signal direction aligns with trade | +30 | Trade confirmed |
| Signal direction opposes trade | −40 | Red flag; tighten delta |
| All 3 cycles aligned (D/W/M) | +25 | High conviction |
| Mixed cycles (1–2 aligned) | +10 | Normal parameters |
| No cycles aligned | −20 | Tighten to 0.15 delta max |
| Cancel level within 2% of spot | −15 | Imminent reversal risk |
| Cancel level >5% from spot | +15 | Signal stability bonus |

**Maximum score: 100.**

Score thresholds:
- **Below 30:** Fischer declines to recommend. Explains the Nenner conflict.
- **30–59:** Tightened parameters — max delta 0.20, 3 DTE max.
- **60–100:** Standard parameters — full delta range, full DTE range.

---

## 7. Built-In Risk Controls

| Control | Rule | Override |
|---------|------|---------|
| **Delta cap** | No short option with \|delta\| > 0.35 (standard) or > 0.20 (tightened Nenner) | None |
| **Liquidity filter** | Bid-ask > 15% of mid or OI < 100 → flagged with execution risk note | Strike still shown, not hidden |
| **Stale data warning** | Chain data > 5 minutes old → prominent warning before output | None |
| **0DTE gamma warning** | Strike within 0.5% of spot with DTE < 1 → mandatory adverse-move P&L display | None |
| **Margin estimation** | Conservative Reg-T: 20% of underlying notional for short puts | Labeled as estimate |
| **Ex-div proximity** | Warns only when ex-div date falls before option expiry (see §5.3) | None |
| **Earnings proximity** | Flags expiries that straddle an earnings announcement; shows implied move and dollar impact (see §5.4) | Flags, does not block |
| **No execution** | Fischer never connects to broker APIs | Permanent |

### 7.1 Position-Aware Checks

Before finalizing a recommendation, Fischer checks existing open positions (when position data is available via `Nenner_Positions.xlsm` or the positions module):

- **Same-underlying overlap:** If there's already a short put on SPY at a different strike, Fischer warns and shows the combined delta exposure.
- **Correlated cluster:** If the proposed trade would push a correlated group (e.g., equity indices: SPY + QQQ + DIA) beyond the 40% gross exposure cap defined in `CLAUDE.md`, Fischer flags it.
- **This check is best-effort** — it runs when position data is available and is skipped silently when it is not.

---

## 8. System Architecture

### 8.1 Component Map

| Component | Technology | Responsibility |
|-----------|-----------|----------------|
| `fischer_engine.py` | Python — scipy, numpy | BSM/BAW pricing, Greeks, IV solver, EV calculator |
| `fischer_mcp.py` | Python — FastMCP | MCP tool definitions, registered alongside `nenner_mcp_server.py` |
| `Options_RT` sheet | Excel — Thomson One RTD | Live option chain with pre-built ±2.75% strike ladder |
| `build_options_rt.py` | Python — win32com | One-time builder for the Options_RT worksheet |
| `chain_reader.py` | Python — xlwings | Read Options_RT sheet into pandas DataFrame |
| `nenner_engine.db` | SQLite (existing) | Signal direction, cycle phase, cancel level — reused directly |
| `nenner_engine.instruments` | Python (existing) | Ticker mapping — reused directly |

### 8.2 MCP Tool Definitions

Fischer exposes these tools via FastMCP, registered as a second MCP server or merged into the existing `nenner_mcp_server.py`:

| Tool | Parameters | Returns |
|------|-----------|---------|
| `fischer_scan` | ticker, intent (covered_put / covered_call), capital, entry_price? | Ranked recommendation table + plain-English summary |
| `fischer_price` | ticker, strike, expiry, type (P/C) | BSM/BAW price, all Greeks, IV |
| `fischer_ev` | ticker, strike, expiry, type, premium, entry_price? | Full EV breakdown for a specific option |
| `fischer_conviction` | ticker, intent | Nenner conviction score with component breakdown |
| `fischer_chain` | ticker | Raw option chain DataFrame as formatted table |
| `fischer_earnings` | ticker | Earnings date, days away, implied move, affected expiries |

**Conversational usage via Claude Code:**

> "What's the best covered put on SPY right now? I have $500K to deploy."

Claude Code invokes `fischer_scan(ticker="SPY", intent="covered_put", capital=500000)` and returns the ranked table with explanation.

> "Price the SPY 590 put expiring Friday."

Claude Code invokes `fischer_price(ticker="SPY", strike=590, expiry="2026-02-27", type="P")`.

### 8.3 Output Format

Fischer returns a ranked recommendation table followed by a plain-English summary. The table includes:

```
Strike | Expiry | DTE | Bid | Ask | IV | Delta | Gamma | Theta |
P(Expire Worthless) | Net EV/Contract | Net EV/Position | Nenner Score | Flags
```

The top-ranked trade is labeled **BEST** and the second is labeled **ALTERNATE**. Fischer then writes one paragraph explaining the recommendation in plain English, including the Nenner context, any risk flags, the ex-dividend status if relevant, and the earnings proximity status.

**When earnings are in the window**, the output is preceded by an earnings banner:

```
═══ EARNINGS ALERT ═══════════════════════════════════════════════════
AAPL reports Feb 27 (2 days) — Market implies ±3.2% move ($7.84)
Expiry 1 (Feb 26): CLEAN — settles before announcement
Expiry 2 (Feb 28): STRADDLES EARNINGS — full gap risk
Expiry 3 (Mar 2):  STRADDLES EARNINGS — full gap risk
══════════════════════════════════════════════════════════════════════
```

Rows in the ranked table that straddle earnings carry an `[EARNINGS]` flag alongside any other flags (liquidity, gamma, etc.).

---

## 9. Options Trade Log — Closing the Feedback Loop

### 9.1 Trade Logging Table

Every Fischer recommendation is logged to enable performance tracking over time.

**Table: `option_recommendations`**

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| timestamp | TEXT | When recommendation was generated |
| ticker | TEXT | Underlying ticker |
| intent | TEXT | 'covered_put' or 'covered_call' |
| strike | REAL | Recommended strike |
| expiry | DATE | Option expiry date |
| type | TEXT | 'P' or 'C' |
| bid_at_rec | REAL | Bid price at recommendation time |
| ask_at_rec | REAL | Ask price at recommendation time |
| spot_at_rec | REAL | Underlying spot at recommendation time |
| iv_at_rec | REAL | Implied volatility at recommendation time |
| delta_at_rec | REAL | Delta at recommendation time |
| ev_per_contract | REAL | Computed Net EV |
| nenner_score | INTEGER | Conviction score at recommendation time |
| capital | REAL | Capital input by trader |
| rank | INTEGER | Rank in the recommendation table (1 = BEST) |

### 9.2 Settlement Tracking

A companion table records actual outcomes:

**Table: `option_outcomes`**

| Column | Type | Description |
|--------|------|-------------|
| recommendation_id | INTEGER FK | Links to `option_recommendations.id` |
| action | TEXT | 'FILLED', 'PASSED', 'EXPIRED', 'ASSIGNED' |
| fill_price | REAL | Actual fill price (null if passed) |
| settlement_price | REAL | Underlying price at expiry |
| actual_pnl | REAL | Realized P&L per contract |
| notes | TEXT | Free-text |

### 9.3 Performance Analytics

Fischer can compute running statistics over its own recommendation history, analogous to `trade_stats.py`:

- Win rate (expired worthless / total filled)
- Average premium collected
- Average EV vs. actual P&L (model calibration check)
- Hit rate by Nenner score bucket
- Performance by ticker, by DTE bucket, by delta bucket

---

## 10. Build Plan — Phased Delivery

### Phase 1 — Core Math Engine

**Deliverable:** `fischer_engine.py`

- BSM pricer (European calls and puts)
- BAW pricer (American early exercise premium)
- Newton-Raphson IV solver
- Full Greeks calculator (Δ, Γ, Θ, ν, ρ)
- 0DTE smile interpolation module
- EV calculator with lognormal tail integration
- Strike ranking and filtering logic
- Unit tests: validate against known option prices and published IV tables
- Timezone-aware time-to-expiry calculation (§2.5)

### Phase 2 — yFinance Fallback Chain + MCP Tools

**Deliverable:** `chain_reader.py` (yFinance mode), `fischer_mcp.py`

- yFinance option chain reader with 0–7 DTE filtering
- MCP tool registration: `fischer_scan`, `fischer_price`, `fischer_ev`, `fischer_conviction`, `fischer_chain`
- Nenner conviction scoring using existing `db.py` functions
- Ticker mapping using existing `instruments.py`
- **Milestone: Fischer is usable end-to-end from Claude Code using delayed yFinance data**

### Phase 3 — Options_RT Worksheet

**Deliverable:** `build_options_rt.py`, Options_RT sheet in `Nenner_DataCenter.xlsm`

- Build script using win32com (follows `rebuild_option_board.py` pattern)
- ±2.75% strike ladder with configurable increment
- 3 auto-rolling expiry blocks
- RIC construction via existing OptionCode sheet
- RTD formulas for Bid, Ask, Last, OI, Volume
- Automated risk-free rate via `^IRX`
- Data validation dropdown for strike increment

### Phase 4 — Live Chain Reader + IV Surface

**Deliverable:** `chain_reader.py` (live mode), `iv_surface` table

- xlwings reader for Options_RT (follows `prices.py` pattern)
- Staleness detection and warning
- IV surface computation and database storage
- Automatic source selection: live Thomson One → yFinance fallback
- Ex-dividend date checking (§5.3)

### Phase 5 — Trade Log + Performance

**Deliverable:** `option_recommendations` and `option_outcomes` tables, analytics functions

- Recommendation logging on every `fischer_scan` invocation
- Settlement tracking (manual entry or automated via price history)
- Performance analytics: win rate, EV calibration, score-bucketed analysis
- Position-aware checks (§7.1) when position data is available

---

*Fischer — Named for Fischer Black (1938–1995), co-creator of the Black-Scholes model*
*Vartanian Capital Management, LLC | Confidential — Internal Use Only*
