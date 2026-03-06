"""
DataBento Price & Option Chain Feed
=====================================
Drop-in alternative to T1/xlwings (prices.py) and Excel chain reader
(_oc_reader.py / fischer_chain.py).  Additive — does not modify existing code.

Data contracts match the existing interfaces exactly:
  - fetch_option_chain()       -> (DataFrame, ChainMeta)    (same as read_chain)
  - fetch_all_option_chains()  -> (put_chains, call_chains)  (same as read_all_chains)

Uses the Historical API exclusively — no live streaming needed for scan-cadence
data (1-3x per day).  Simpler, no connection management, `.to_df()` auto-converts
fixed-precision prices to decimal.

Requires:
  pip install databento
  DATABENTO_API_KEY in environment, .env, or Azure Key Vault (secret: databento-api)

Datasets used:
  - OPRA.PILLAR  : US equity options — cbbo-1s (1-second NBBO snapshots)
"""

import logging
import math
import os
from datetime import date, datetime, timedelta, timezone

log = logging.getLogger("nenner")


# ---------------------------------------------------------------------------
# API Key
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    """Load DataBento API key from environment, .env, or Azure Key Vault."""
    # 1. Environment variable
    key = os.environ.get("DATABENTO_API_KEY")
    if key:
        return key

    # 2. .env file in project root
    from pathlib import Path
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("DATABENTO_API_KEY="):
                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                if val:
                    return val

    # 3. Azure Key Vault (check env and .env for vault URL)
    vault_url = os.environ.get("AZURE_KEYVAULT_URL")
    if not vault_url and env_path.exists():
        for line in env_path.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith("AZURE_KEYVAULT_URL="):
                vault_url = stripped.split("=", 1)[1].strip().strip('"').strip("'")
    if vault_url:
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vault_url, credential=credential)
            secret = client.get_secret("databento-api").value
            if secret:
                return secret
        except Exception as e:
            log.error("Azure Key Vault error (DataBento): %s", e)

    raise RuntimeError(
        "DATABENTO_API_KEY not found in environment, .env, or Azure Key Vault."
    )


# ---------------------------------------------------------------------------
# Fischer option chain tickers — must be on OPRA
# ---------------------------------------------------------------------------

FISCHER_TICKERS: list[str] = [
    "AAPL", "AMZN", "AVGO", "GOOGL", "IWM",
    "META", "MSFT", "NVDA", "QQQ", "TSLA",
    "GLD", "MSTR", "SLV", "UNG", "USO",
]


# ---------------------------------------------------------------------------
# Expiry Schedule — per-ticker based on what the exchanges actually list
# ---------------------------------------------------------------------------

# Daily expiries (Mon-Fri, 0DTE available): index ETFs
DAILY_EXPIRY_TICKERS: frozenset[str] = frozenset({"QQQ", "IWM"})

# Tri-weekly expiries (Mon/Wed/Fri): large-cap single stocks
TRIWEEKLY_EXPIRY_TICKERS: frozenset[str] = frozenset({
    "AAPL", "AMZN", "AVGO", "GOOGL", "META", "MSFT", "NVDA", "TSLA",
})

# Weekly expiries (Fri only): commodity/macro ETFs
WEEKLY_EXPIRY_TICKERS: frozenset[str] = frozenset({
    "GLD", "MSTR", "SLV", "UNG", "USO",
})

# DTE caps per tier
DAILY_MAX_DTE = 2       # 0, 1, 2 DTE only
TRIWEEKLY_MAX_DTE = 7   # next 4 tri-weekly expiries (approx 7 calendar days)
WEEKLY_MAX_DTE = 14      # next 2 Fridays


def _expiries_for_ticker(ticker: str) -> list[date]:
    """Return the valid expiry dates for a ticker based on its expiry tier."""
    today = date.today()

    if ticker in DAILY_EXPIRY_TICKERS:
        expiries = []
        d = today
        end = today + timedelta(days=DAILY_MAX_DTE + 1)
        while d < end:
            if d.weekday() < 5:
                expiries.append(d)
            d += timedelta(days=1)
        return expiries

    if ticker in TRIWEEKLY_EXPIRY_TICKERS:
        expiries = []
        d = today
        while len(expiries) < 4:
            if d.weekday() in (0, 2, 4):
                expiries.append(d)
            d += timedelta(days=1)
        return expiries

    # Weekly (Fri only)
    expiries = []
    d = today
    end = today + timedelta(days=WEEKLY_MAX_DTE + 1)
    while d < end:
        if d.weekday() == 4:
            expiries.append(d)
        d += timedelta(days=1)
    return expiries


# ---------------------------------------------------------------------------
# Strike computation
# ---------------------------------------------------------------------------

def _compute_strikes(
    spot: float,
    ticker: str,
    n_strikes: int = 7,
    opt_type: str = "P",
) -> list[float]:
    """Compute the N target strikes around spot, OTM-biased."""
    increment = get_increment(ticker)
    atm_strike = round(spot / increment) * increment

    if opt_type == "P":
        top = atm_strike + increment
        strikes = [top - i * increment for i in range(n_strikes)]
        strikes.reverse()
    else:
        bottom = atm_strike - increment
        strikes = [bottom + i * increment for i in range(n_strikes)]

    return strikes


def _parse_occ_symbol(raw: str) -> tuple[str, date, str, float] | None:
    """Parse an OCC symbol into (ticker, expiry, opt_type, strike).

    OCC format: TICKER(6) YYMMDD P/C PRICE(8)
    Returns None if the symbol can't be parsed.
    """
    if not raw or len(raw) < 21:
        return None
    try:
        ticker = raw[:6].strip()
        exp = date(2000 + int(raw[6:8]), int(raw[8:10]), int(raw[10:12]))
        opt_type = raw[12]
        strike = int(raw[13:21]) / 1000.0
        return (ticker, exp, opt_type, strike)
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Historical API client (cached per session)
# ---------------------------------------------------------------------------

_hist_client = None


def _get_client():
    """Return a cached Historical client."""
    global _hist_client
    if _hist_client is None:
        import databento as db
        _hist_client = db.Historical(key=_get_api_key())
    return _hist_client


# ---------------------------------------------------------------------------
# Market hours time window
# ---------------------------------------------------------------------------

def _market_time_window(lookback_seconds: int = 10) -> tuple[str, str]:
    """Return (start, end) ISO timestamps for a DataBento query.

    Caps `end` at the dataset's actual available end (OPRA.PILLAR has ~30 min
    ingest lag during market hours).  After market close, caps at 4:15 PM ET.
    """
    now_utc = datetime.now(timezone.utc)
    today_str = date.today().isoformat()
    market_close_utc = datetime.fromisoformat(f"{today_str}T20:15:00+00:00")

    if now_utc <= market_close_utc:
        end = now_utc
    else:
        end = market_close_utc

    # DataBento OPRA.PILLAR has ~30 min ingest lag — cap at available end
    try:
        client = _get_client()
        r = client.metadata.get_dataset_range(dataset="OPRA.PILLAR")
        avail_end = datetime.fromisoformat(str(r["end"]).replace("Z", "+00:00"))
        if end > avail_end:
            log.info("DataBento: capping end to available %s (was %s)",
                     avail_end.isoformat(), end.isoformat())
            end = avail_end
    except Exception as e:
        log.warning("DataBento: could not fetch dataset range, using raw end: %s", e)

    start = end - timedelta(seconds=lookback_seconds)
    return start.isoformat(), end.isoformat()


# ---------------------------------------------------------------------------
# Core: Bulk NBBO fetch via parent symbology + client-side filtering
# ---------------------------------------------------------------------------

def _fetch_bulk_nbbo(
    tickers: list[str],
    target_expiries: dict[str, set[date]],
    target_strikes: dict[str, set[float]],
) -> dict[str, dict[str, tuple[float, float]]]:
    """Fetch NBBO for all target contracts using parent symbology.

    Uses cbbo-1s (1-second consolidated NBBO snapshots) with parent symbols
    (TICKER.OPT) and filters client-side to our target expiries/strikes.
    This is DataBento's recommended approach for large symbol universes.

    Args:
        tickers: Underlying tickers to query.
        target_expiries: {ticker: set of target expiry dates}
        target_strikes: {ticker: set of target strikes}

    Returns:
        {ticker: {occ_symbol: (bid, ask)}}
    """
    client = _get_client()
    parent_symbols = [f"{t}.OPT" for t in tickers]

    # Use a narrow 10-second window — cbbo-1s gives one record per second,
    # so we get ~10 snapshots per contract. Last one = freshest quote.
    start, end = _market_time_window(lookback_seconds=10)

    log.info("DataBento: cbbo-1s query for %d parent symbols, window %s → %s",
             len(parent_symbols), start, end)

    try:
        data = client.timeseries.get_range(
            dataset="OPRA.PILLAR",
            schema="cbbo-1s",
            stype_in="parent",
            symbols=parent_symbols,
            start=start,
            end=end,
        )
        df = data.to_df()
    except Exception as e:
        log.error("DataBento: cbbo-1s query failed: %s", e)
        return {}

    if df.empty:
        log.warning("DataBento: cbbo-1s returned no data")
        return {}

    log.info("DataBento: cbbo-1s returned %d rows", len(df))

    # Client-side filtering: parse OCC symbols, keep only target expiry/strike puts
    result: dict[str, dict[str, tuple[float, float]]] = {t: {} for t in tickers}

    # Group by symbol and take last record per symbol (freshest quote)
    for symbol, group in df.groupby("symbol"):
        parsed = _parse_occ_symbol(symbol)
        if parsed is None:
            continue

        ticker, exp, opt_type, strike = parsed

        # Only puts for Fischer
        if opt_type != "P":
            continue

        # Filter to our target tickers
        if ticker not in target_expiries:
            continue

        # Filter to target expiries
        if exp not in target_expiries[ticker]:
            continue

        # Filter to target strikes
        if strike not in target_strikes[ticker]:
            continue

        # Take the last (most recent) record
        last = group.iloc[-1]
        bid = float(last["bid_px_00"])
        ask = float(last["ask_px_00"])

        # Skip UNDEF_PRICE (NaN in .to_df() decimal format)
        if math.isnan(bid):
            bid = 0.0
        if math.isnan(ask):
            ask = 0.0

        if bid <= 0 and ask <= 0:
            continue

        result[ticker][symbol] = (bid, ask)

    for t in tickers:
        log.info("DataBento: %s — %d contracts matched filters", t, len(result[t]))

    return result


# ---------------------------------------------------------------------------
# Public API: fetch_option_chain (single ticker)
# ---------------------------------------------------------------------------

def fetch_option_chain(
    ticker: str,
    spot: float | None = None,
    expiries: list[date] | None = None,
    n_strikes: int = 7,
    rate: float = 0.045,
) -> tuple:
    """Fetch a single ticker's put chain from DataBento OPRA.

    Uses cbbo-1s with parent symbology and client-side filtering.

    Returns:
        (pd.DataFrame, ChainMeta) — same contract as fischer_chain.read_chain()
    """
    import pandas as pd
    from .fischer_chain import ChainMeta, CHAIN_COLUMNS

    if expiries is None:
        expiries = _expiries_for_ticker(ticker)

    if spot is None:
        log.warning("DataBento: spot price required for %s", ticker)
        return pd.DataFrame(columns=CHAIN_COLUMNS), ChainMeta(
            ticker=ticker, spot=0, rate=rate, div_yield=0.0,
            source="DATABENTO", timestamp=datetime.now(), expiries=[],
        )

    strikes = _compute_strikes(spot, ticker, n_strikes, opt_type="P")

    quotes_by_ticker = _fetch_bulk_nbbo(
        [ticker],
        {ticker: set(expiries)},
        {ticker: set(strikes)},
    )

    quotes = quotes_by_ticker.get(ticker, {})

    rows = []
    for occ, (bid, ask) in quotes.items():
        parsed = _parse_occ_symbol(occ)
        if parsed is None:
            continue
        _, exp, opt_type, strike = parsed
        rows.append({
            "expiry": exp,
            "strike": strike,
            "type": opt_type,
            "bid": bid,
            "ask": ask,
            "last": 0.0,
            "oi": 0,
            "volume": 0,
        })

    chain_df = pd.DataFrame(rows, columns=CHAIN_COLUMNS)
    if not chain_df.empty:
        chain_df = chain_df.sort_values(
            ["type", "expiry", "strike"]
        ).reset_index(drop=True)

    meta = ChainMeta(
        ticker=ticker,
        spot=spot,
        rate=rate,
        div_yield=0.0,
        source="DATABENTO",
        timestamp=datetime.now(),
        expiries=expiries,
    )

    log.info("DataBento: got %d quotes for %s", len(rows), ticker)
    return chain_df, meta


# ---------------------------------------------------------------------------
# Public API: fetch_all_option_chains (all Fischer tickers)
# ---------------------------------------------------------------------------

def fetch_all_option_chains(
    tickers: list[str] | None = None,
    rate: float = 0.045,
    spots: dict[str, float] | None = None,
) -> tuple:
    """Fetch put chains for all Fischer tickers from DataBento OPRA.

    Single bulk request using cbbo-1s with parent symbology (TICKER.OPT)
    for all tickers at once. Client-side filtering to target expiries/strikes.
    This minimizes API requests and data volume per DataBento's recommendation.

    Args:
        tickers: List of tickers to scan. Defaults to FISCHER_TICKERS.
        rate: Risk-free rate for ChainMeta.
        spots: Pre-fetched spot prices {ticker: price}. Pass T1 prices here
               to avoid needing a DataBento equities subscription.

    Returns:
        (put_chains, call_chains) — same contract as read_all_chains()
    """
    import pandas as pd
    from .fischer_chain import ChainMeta, CHAIN_COLUMNS

    if tickers is None:
        tickers = FISCHER_TICKERS

    if spots is None:
        log.warning("DataBento: spots dict required for fetch_all_option_chains")
        return {}, {}

    # Build per-ticker expiries and strikes
    target_expiries: dict[str, set[date]] = {}
    target_strikes: dict[str, set[float]] = {}
    failed = []

    for ticker in tickers:
        spot = spots.get(ticker)
        if not spot:
            failed.append(ticker)
            continue
        target_expiries[ticker] = set(_expiries_for_ticker(ticker))
        target_strikes[ticker] = set(
            _compute_strikes(spot, ticker, n_strikes=7)
        )

    active_tickers = [t for t in tickers if t not in failed]
    if not active_tickers:
        return {}, {}

    # Single bulk NBBO request for all tickers
    quotes_by_ticker = _fetch_bulk_nbbo(
        active_tickers, target_expiries, target_strikes,
    )

    # Assemble per-ticker DataFrames
    put_chains = {}
    for ticker in active_tickers:
        quotes = quotes_by_ticker.get(ticker, {})
        if not quotes:
            failed.append(ticker)
            continue

        rows = []
        for occ, (bid, ask) in quotes.items():
            parsed = _parse_occ_symbol(occ)
            if parsed is None:
                continue
            _, exp, opt_type, strike = parsed
            rows.append({
                "expiry": exp,
                "strike": strike,
                "type": opt_type,
                "bid": bid,
                "ask": ask,
                "last": 0.0,
                "oi": 0,
                "volume": 0,
            })

        chain_df = pd.DataFrame(rows, columns=CHAIN_COLUMNS)
        chain_df = chain_df.sort_values(
            ["type", "expiry", "strike"]
        ).reset_index(drop=True)

        meta = ChainMeta(
            ticker=ticker,
            spot=spots[ticker],
            rate=rate,
            div_yield=0.0,
            source="DATABENTO",
            timestamp=datetime.now(),
            expiries=sorted(target_expiries.get(ticker, set())),
        )
        put_chains[ticker] = (chain_df, meta)

    if failed:
        log.info("DataBento: no data for %s", ", ".join(set(failed)))

    log.info("DataBento: loaded %d/%d put chains", len(put_chains), len(tickers))
    return put_chains, {}


# ---------------------------------------------------------------------------
# Strike increment helper (reads from existing JSON)
# ---------------------------------------------------------------------------

class _StrikeIncrements:
    """Lazy loader for strike_increments.json."""
    _data: dict | None = None

    @classmethod
    def get(cls, ticker: str) -> float:
        if cls._data is None:
            import json
            from pathlib import Path
            path = Path(__file__).parent / "strike_increments.json"
            cls._data = json.loads(path.read_text())
        return cls._data.get(ticker, 1.0)


def get_increment(ticker: str) -> float:
    return _StrikeIncrements.get(ticker)


# ---------------------------------------------------------------------------
# Availability check
# ---------------------------------------------------------------------------

def is_available() -> bool:
    """Check if DataBento is configured and importable."""
    try:
        import databento  # noqa: F401
        _get_api_key()
        return True
    except (ImportError, RuntimeError):
        return False


# ---------------------------------------------------------------------------
# Cost estimation — preview cost before making a data request
# ---------------------------------------------------------------------------

def estimate_cost(
    tickers: list[str] | None = None,
    lookback_seconds: int = 10,
) -> float:
    """Estimate the USD cost of a cbbo-1s query for the given tickers.

    Uses metadata.get_cost() which respects flat-rate plan discounts.
    Returns the estimated cost in dollars.
    """
    client = _get_client()
    if tickers is None:
        tickers = FISCHER_TICKERS
    parent_symbols = [f"{t}.OPT" for t in tickers]
    start, end = _market_time_window(lookback_seconds)
    return client.metadata.get_cost(
        dataset="OPRA.PILLAR",
        schema="cbbo-1s",
        stype_in="parent",
        symbols=parent_symbols,
        start=start,
        end=end,
    )


def get_dataset_range() -> dict:
    """Return the available date range for OPRA.PILLAR given our entitlements."""
    client = _get_client()
    return client.metadata.get_dataset_range(dataset="OPRA.PILLAR")


def list_schemas() -> list[str]:
    """List available schemas for OPRA.PILLAR."""
    client = _get_client()
    return client.metadata.list_schemas(dataset="OPRA.PILLAR")


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not is_available():
        print("DataBento not configured. Set DATABENTO_API_KEY in .env")
        sys.exit(1)

    # Diagnostic mode: show entitlements and cost estimate
    if "--info" in sys.argv:
        print("=== DataBento OPRA.PILLAR Info ===")
        try:
            r = get_dataset_range()
            print(f"  Available range: {r}")
        except Exception as e:
            print(f"  Range check failed: {e}")
        try:
            schemas = list_schemas()
            print(f"  Available schemas: {schemas}")
        except Exception as e:
            print(f"  Schema list failed: {e}")
        try:
            cost = estimate_cost()
            print(f"  Estimated cost (15 tickers, 10s window): ${cost:.4f}")
        except Exception as e:
            print(f"  Cost estimate failed: {e}")
        sys.exit(0)

    # Test with a single ticker — requires spot price as argument
    ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    spot = float(sys.argv[2]) if len(sys.argv) > 2 else 235.0

    print(f"=== Option Chain Test: {ticker} @ ${spot:.2f} ===")
    chain_df, meta = fetch_option_chain(ticker, spot=spot)
    print(f"  Source: {meta.source}")
    print(f"  Expiries: {meta.expiries}")
    print(f"  Rows: {len(chain_df)}")
    if not chain_df.empty:
        print()
        print(chain_df.to_string())
    else:
        print("  No data returned")
