"""
Instrument Mapping & Attribution
=================================
Maps instrument names as they appear in Nenner emails to canonical tickers
and asset classes. Provides section-header-based instrument attribution.
"""

import re


# Maps instrument names (as they appear in Nenner emails) to canonical tickers
# and asset classes. The parser uses these to tag signals.
INSTRUMENT_MAP = {
    # Equity Indices
    "S&P": {"ticker": "ES", "asset_class": "Equity Index", "aliases": ["S&P 500", "S&P (March", "S&P (June", "S&P (Sep", "S&P (Dec"]},
    "Nasdaq": {"ticker": "NQ", "asset_class": "Equity Index", "aliases": ["Nasdaq (March", "Nasdaq (June"]},
    "Dow Jones": {"ticker": "YM", "asset_class": "Equity Index", "aliases": ["Dow"]},
    "FANG Index": {"ticker": "NYFANG", "asset_class": "Equity Index", "aliases": ["NYFANG"]},
    "VIX": {"ticker": "VIX", "asset_class": "Volatility", "aliases": ["CBOE Market Volatility Index", "CBOE Market Volatility"]},
    "TSX": {"ticker": "TSX", "asset_class": "Equity Index", "aliases": ["TSX (Canada)"]},
    "DAX": {"ticker": "DAX", "asset_class": "Equity Index (Europe)", "aliases": []},
    "FTSE": {"ticker": "FTSE", "asset_class": "Equity Index (Europe)", "aliases": []},
    "AEX": {"ticker": "AEX", "asset_class": "Equity Index (Europe)", "aliases": []},
    "NYSE Composite": {"ticker": "NYA", "asset_class": "Equity Index", "aliases": []},
    "Swiss Market Index": {"ticker": "SMI", "asset_class": "Equity Index (Europe)", "aliases": []},
    "Biotechnology Index": {"ticker": "BTK", "asset_class": "Equity Index", "aliases": []},

    # Precious Metals
    "Gold": {"ticker": "GC", "asset_class": "Precious Metals", "aliases": ["Gold (April", "Gold (June", "Gold (Feb", "Gold (Aug", "Gold (Dec"]},
    "GLD": {"ticker": "GLD", "asset_class": "Precious Metals ETF", "aliases": []},
    "GDXJ": {"ticker": "GDXJ", "asset_class": "Precious Metals ETF", "aliases": []},
    "NEM": {"ticker": "NEM", "asset_class": "Precious Metals Stock", "aliases": []},
    "Silver": {"ticker": "SI", "asset_class": "Precious Metals", "aliases": ["Silver (March", "Silver (May"]},
    "SLV": {"ticker": "SLV", "asset_class": "Precious Metals ETF", "aliases": []},
    "Copper": {"ticker": "HG", "asset_class": "Base Metals", "aliases": []},

    # Energy
    "Crude": {"ticker": "CL", "asset_class": "Energy", "aliases": ["Crude (", "Crude Oil"]},
    "USO": {"ticker": "USO", "asset_class": "Energy ETF", "aliases": []},
    "Nat Gas": {"ticker": "NG", "asset_class": "Energy", "aliases": ["Natural Gas"]},
    "UNG": {"ticker": "UNG", "asset_class": "Energy ETF", "aliases": []},

    # Agriculture
    "Corn": {"ticker": "ZC", "asset_class": "Agriculture", "aliases": ["Corn ("]},
    "CORN": {"ticker": "CORN", "asset_class": "Agriculture ETF", "aliases": []},
    "Soybean": {"ticker": "ZS", "asset_class": "Agriculture", "aliases": ["Soybean ("]},
    "SOYB": {"ticker": "SOYB", "asset_class": "Agriculture ETF", "aliases": []},
    "Wheat": {"ticker": "ZW", "asset_class": "Agriculture", "aliases": ["Wheat ("]},
    "WEAT": {"ticker": "WEAT", "asset_class": "Agriculture ETF", "aliases": []},
    "Lumber": {"ticker": "LBS", "asset_class": "Agriculture", "aliases": ["Lumber ("]},

    # Bonds
    "30 Year": {"ticker": "ZB", "asset_class": "Fixed Income", "aliases": ["US Bonds", "US 30-Year Bonds", "30-Year"]},
    "10 Year": {"ticker": "ZN", "asset_class": "Fixed Income", "aliases": []},
    "TLT": {"ticker": "TLT", "asset_class": "Fixed Income ETF", "aliases": []},
    "Bunds": {"ticker": "FGBL", "asset_class": "Fixed Income (Europe)", "aliases": []},

    # Currencies
    "Dollar": {"ticker": "DXY", "asset_class": "Currency", "aliases": ["Dollar Index"]},
    "Euro": {"ticker": "EUR/USD", "asset_class": "Currency", "aliases": ["Euro (EUR/USD)"]},
    "FXE": {"ticker": "FXE", "asset_class": "Currency ETF", "aliases": []},
    "Australian Dollar": {"ticker": "AUD/USD", "asset_class": "Currency", "aliases": ["Aussie"]},
    "Canadian Dollar": {"ticker": "USD/CAD", "asset_class": "Currency", "aliases": []},
    "Yen": {"ticker": "USD/JPY", "asset_class": "Currency", "aliases": ["Japanese Yen"]},
    "Swiss Franc": {"ticker": "USD/CHF", "asset_class": "Currency", "aliases": []},
    "British Pound": {"ticker": "GBP/USD", "asset_class": "Currency", "aliases": []},
    "Brazil Real": {"ticker": "USD/BRL", "asset_class": "Currency", "aliases": []},
    "Israel Shekel": {"ticker": "USD/ILS", "asset_class": "Currency", "aliases": []},

    # Crypto
    "Bitcoin": {"ticker": "BTC", "asset_class": "Crypto", "aliases": ["Bitcoin & GBTC"]},
    "GBTC": {"ticker": "GBTC", "asset_class": "Crypto ETF", "aliases": []},
    "Ethereum": {"ticker": "ETH", "asset_class": "Crypto", "aliases": ["Ethereum & ETHE"]},
    "ETHE": {"ticker": "ETHE", "asset_class": "Crypto ETF", "aliases": []},
    "BITO": {"ticker": "BITO", "asset_class": "Crypto ETF", "aliases": ["ETF BITO"]},

    # Single Stocks
    "Apple": {"ticker": "AAPL", "asset_class": "Single Stock", "aliases": ["AAPL", "Apple (AAPL)"]},
    "Alphabet": {"ticker": "GOOG", "asset_class": "Single Stock", "aliases": ["GOOG", "Alphabet (GOOG)"]},
    "Bank of America": {"ticker": "BAC", "asset_class": "Single Stock", "aliases": ["BAC", "Bank of America (BAC)"]},
    "Microsoft": {"ticker": "MSFT", "asset_class": "Single Stock", "aliases": ["MSFT", "Microsoft (MSFT)"]},
    "Nvidia": {"ticker": "NVDA", "asset_class": "Single Stock", "aliases": ["NVDA", "Nvidia (NVDA)"]},
    "Tesla": {"ticker": "TSLA", "asset_class": "Single Stock", "aliases": ["TSLA", "Tesla (TSLA)"]},
    "Amazon": {"ticker": "AMZN", "asset_class": "Single Stock", "aliases": ["AMZN", "Amazon (AMZN)"]},
    "3M Company": {"ticker": "MMM", "asset_class": "Single Stock", "aliases": ["3M"]},
    "American Express": {"ticker": "AXP", "asset_class": "Single Stock", "aliases": []},
    "Citibank": {"ticker": "C", "asset_class": "Single Stock", "aliases": ["Citi"]},
    "Goldman Sachs": {"ticker": "GS", "asset_class": "Single Stock", "aliases": []},
}

# Build reverse lookup: text fragment -> (instrument_name, ticker, asset_class)
_INSTRUMENT_LOOKUP: list[tuple[str, str, str, str]] = []
for _name, _info in INSTRUMENT_MAP.items():
    _INSTRUMENT_LOOKUP.append((_name, _name, _info["ticker"], _info["asset_class"]))
    for _alias in _info["aliases"]:
        _INSTRUMENT_LOOKUP.append((_alias, _name, _info["ticker"], _info["asset_class"]))

# Sort by length descending so longer matches take priority
_INSTRUMENT_LOOKUP.sort(key=lambda x: len(x[0]), reverse=True)

# Section header patterns for instrument attribution.
# Used by get_section_instrument() to find the nearest instrument header
# preceding a signal sentence in the email body.
SECTION_HEADERS = [
    # Equities
    (r'S&P\s*\(', "S&P", "ES", "Equity Index"),
    (r'S&P /', "S&P", "ES", "Equity Index"),
    (r'Nasdaq\s*\(', "Nasdaq", "NQ", "Equity Index"),
    (r'Dow Jones', "Dow Jones", "YM", "Equity Index"),
    (r'FANG Index', "FANG Index", "NYFANG", "Equity Index"),
    (r'CBOE Market Volatility|VIX\)', "VIX", "VIX", "Volatility"),
    (r'TSX\s*\(Canada\)', "TSX", "TSX", "Equity Index"),
    (r'DAX\s*/\s*FTSE|DAX continues|DAX cancelled', "DAX", "DAX", "Equity Index (Europe)"),
    (r'FTSE continues|FTSE cancelled', "FTSE", "FTSE", "Equity Index (Europe)"),
    (r'AEX continues|AEX cancelled', "AEX", "AEX", "Equity Index (Europe)"),
    (r'NYSE Composite', "NYSE Composite", "NYA", "Equity Index"),
    (r'Swiss Market Index', "Swiss Market Index", "SMI", "Equity Index (Europe)"),
    (r'Biotechnology Index', "Biotechnology Index", "BTK", "Equity Index"),
    # Precious Metals
    (r'Gold\s*\([A-Z]', "Gold", "GC", "Precious Metals"),
    (r'\bGLD\b', "GLD", "GLD", "Precious Metals ETF"),
    (r'\bGDXJ\b', "GDXJ", "GDXJ", "Precious Metals ETF"),
    (r'\bNEM\b', "NEM", "NEM", "Precious Metals Stock"),
    (r'Silver\s*\([A-Z]', "Silver", "SI", "Precious Metals"),
    (r'\bSLV\b', "SLV", "SLV", "Precious Metals ETF"),
    (r'Copper', "Copper", "HG", "Base Metals"),
    # Energy
    (r'Crude\s*\(', "Crude", "CL", "Energy"),
    (r'\bUSO\b', "USO", "USO", "Energy ETF"),
    (r'Nat Gas\s*\(|Natural Gas', "Nat Gas", "NG", "Energy"),
    (r'\bUNG\b', "UNG", "UNG", "Energy ETF"),
    # Agriculture
    (r'Corn\s*\(', "Corn", "ZC", "Agriculture"),
    (r'\bCORN\b', "CORN", "CORN", "Agriculture ETF"),
    (r'Soybean\s*\(', "Soybean", "ZS", "Agriculture"),
    (r'\bSOYB\b', "SOYB", "SOYB", "Agriculture ETF"),
    (r'Wheat\s*\(', "Wheat", "ZW", "Agriculture"),
    (r'\bWEAT\b', "WEAT", "WEAT", "Agriculture ETF"),
    (r'Lumber\s*\(', "Lumber", "LBS", "Agriculture"),
    # Bonds
    (r'US Bonds|30 Year continues|30\s*-?\s*Year', "30 Year", "ZB", "Fixed Income"),
    (r'10 Year', "10 Year", "ZN", "Fixed Income"),
    (r'\bTLT\b', "TLT", "TLT", "Fixed Income ETF"),
    (r'Bunds', "Bunds", "FGBL", "Fixed Income (Europe)"),
    # Currencies
    (r'\bDollar\b(?!\s*\()', "Dollar", "DXY", "Currency"),
    (r'Euro\s*\(EUR', "Euro", "EUR/USD", "Currency"),
    (r'\bFXE\b', "FXE", "FXE", "Currency ETF"),
    (r'Australian Dollar', "Australian Dollar", "AUD/USD", "Currency"),
    (r'Canadian Dollar', "Canadian Dollar", "USD/CAD", "Currency"),
    (r'(?:Japanese\s+)?Yen\s*\(USD', "Yen", "USD/JPY", "Currency"),
    (r'Swiss Franc', "Swiss Franc", "USD/CHF", "Currency"),
    (r'British Pound', "British Pound", "GBP/USD", "Currency"),
    (r'Brazil Real', "Brazil Real", "USD/BRL", "Currency"),
    (r'Israel Shekel', "Israel Shekel", "USD/ILS", "Currency"),
    # Crypto - parent instruments MUST come before ETF derivatives
    (r'Bitcoin\s*&?\s*GBTC|Bitcoin', "Bitcoin", "BTC", "Crypto"),
    (r'GBTC\s*-|GBTC\b', "GBTC", "GBTC", "Crypto ETF"),
    (r'Ethereum\s*&?\s*ETHE|Ethereum', "Ethereum", "ETH", "Crypto"),
    (r'ETHE\s*-|ETHE\b', "ETHE", "ETHE", "Crypto ETF"),
    (r'ETF BITO|\bBITO\b', "BITO", "BITO", "Crypto ETF"),
    # Single Stocks
    (r'Apple\s*\(AAPL\)|AAPL\s*(?:Daily|Weekly|Monthly)', "Apple", "AAPL", "Single Stock"),
    (r'Alphabet\s*\(GOOG\)|GOOG\s*(?:Daily|Weekly|Monthly)', "Alphabet", "GOOG", "Single Stock"),
    (r'Bank of America\s*\(BAC\)|BAC\s*(?:Daily|Weekly|Monthly)', "Bank of America", "BAC", "Single Stock"),
    (r'Microsoft\s*\(MSFT\)|MSFT\s*(?:Daily|Weekly|Monthly)', "Microsoft", "MSFT", "Single Stock"),
    (r'Nvidia\s*\(NVDA\)|NVDA\s*(?:Daily|Weekly|Monthly)', "Nvidia", "NVDA", "Single Stock"),
    (r'Tesla\s*\(TSLA\)|TSLA\s*(?:Daily|Weekly|Monthly)', "Tesla", "TSLA", "Single Stock"),
    (r'Amazon\s*\(AMZN\)', "Amazon", "AMZN", "Single Stock"),
    (r'3M Company', "3M Company", "MMM", "Single Stock"),
    (r'American Express', "American Express", "AXP", "Single Stock"),
    (r'Citibank', "Citibank", "C", "Single Stock"),
    (r'Goldman Sachs(?!\s+Commodity)', "Goldman Sachs", "GS", "Single Stock"),
]


def identify_instrument(text: str, context_instrument: str = None) -> tuple[str, str, str]:
    """
    Given a text fragment (typically the sentence or paragraph containing a signal),
    identify which instrument it refers to.
    Returns (instrument_name, ticker, asset_class).
    Falls back to context_instrument if no match found.
    """
    for fragment, name, ticker, asset_class in _INSTRUMENT_LOOKUP:
        if fragment in text:
            return name, ticker, asset_class
    if context_instrument and context_instrument in INSTRUMENT_MAP:
        info = INSTRUMENT_MAP[context_instrument]
        return context_instrument, info["ticker"], info["asset_class"]
    return "Unknown", "UNK", "Unknown"


def get_section_instrument(text_before: str) -> tuple[str, str, str]:
    """
    Determine the current instrument context based on the section header
    that precedes a signal sentence in the email.

    Strategy: search backward through the text for known instrument markers.
    We look at the NEAREST instrument header to avoid misattribution when
    instruments appear sequentially (e.g., VIX then TSX then DAX).
    """
    best_pos = -1
    best_result = ("Unknown", "UNK", "Unknown")

    for pattern, name, ticker, asset_class in SECTION_HEADERS:
        for m in re.finditer(pattern, text_before):
            if m.start() > best_pos:
                best_pos = m.start()
                best_result = (name, ticker, asset_class)

    return best_result
