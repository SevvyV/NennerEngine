"""
Nenner Signal Engine - IMAP Connector & Parser
================================================
Vartanian Capital Management, LLC

Connects to Gmail via IMAP, pulls all Charles Nenner Research emails,
parses buy/sell signals, price targets, cycle directions, and trigger
levels into a local SQLite database.

Usage:
    # First run - backfill all historical emails:
    python nenner_engine.py --backfill

    # Ongoing monitoring (run via Task Scheduler every 5 min):
    python nenner_engine.py

    # Parse local .eml files (testing / one-time import):
    python nenner_engine.py --import-folder "C:\\path\\to\\eml\\files"

    # Show current signal state:
    python nenner_engine.py --status

    # Show signal history for an instrument:
    python nenner_engine.py --history "Gold"

    # Export database to CSV:
    python nenner_engine.py --export

Configuration:
    Set environment variables or use a .env file:
        GMAIL_ADDRESS=your_email@gmail.com
        GMAIL_APP_PASSWORD=your_app_password
    
    Or use Azure Key Vault:
        AZURE_KEYVAULT_URL=https://your-vault.vault.azure.net/
        GMAIL_ADDRESS_SECRET=gmail-address
        GMAIL_PASSWORD_SECRET=gmail-app-password

Dependencies:
    pip install python-dotenv
    pip install azure-identity azure-keyvault-secrets  (optional, for Key Vault)
"""

import imaplib
import email as email_lib
from email import policy
from email.parser import BytesParser
import sqlite3
import re
import html
import os
import sys
import json
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nenner_signals.db")
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nenner_engine.log")
NENNER_SENDER = "newsletter@charlesnenner.com"
IMAP_SERVER = "imap.gmail.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("nenner")


# ---------------------------------------------------------------------------
# Credential Management
# ---------------------------------------------------------------------------

def get_credentials() -> tuple[str, str]:
    """Get Gmail credentials from env vars, .env file, or Azure Key Vault."""
    
    # Try .env file first
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())

    gmail_addr = os.environ.get("GMAIL_ADDRESS")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD")

    if gmail_addr and gmail_pass:
        return gmail_addr, gmail_pass

    # Try Azure Key Vault
    vault_url = os.environ.get("AZURE_KEYVAULT_URL")
    if vault_url:
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vault_url, credential=credential)
            addr_secret = os.environ.get("GMAIL_ADDRESS_SECRET", "gmail-address")
            pass_secret = os.environ.get("GMAIL_PASSWORD_SECRET", "gmail-app-password")
            gmail_addr = client.get_secret(addr_secret).value
            gmail_pass = client.get_secret(pass_secret).value
            return gmail_addr, gmail_pass
        except Exception as e:
            log.error(f"Azure Key Vault error: {e}")

    raise ValueError(
        "No credentials found. Set GMAIL_ADDRESS and GMAIL_APP_PASSWORD "
        "in environment variables or a .env file, or configure Azure Key Vault."
    )


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Initialize SQLite database with schema."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT UNIQUE,
            subject TEXT,
            date_sent TEXT,
            date_parsed TEXT,
            email_type TEXT,
            raw_text TEXT,
            signal_count INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id INTEGER REFERENCES emails(id),
            date TEXT,
            instrument TEXT,
            ticker TEXT,
            asset_class TEXT,
            signal_type TEXT,
            signal_status TEXT,
            origin_price REAL,
            cancel_direction TEXT,
            cancel_level REAL,
            trigger_direction TEXT,
            trigger_level REAL,
            price_target REAL,
            target_direction TEXT,
            note_the_change INTEGER DEFAULT 0,
            uses_hourly_close INTEGER DEFAULT 0,
            raw_text TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id INTEGER REFERENCES emails(id),
            date TEXT,
            instrument TEXT,
            ticker TEXT,
            timeframe TEXT,
            direction TEXT,
            until_description TEXT,
            raw_text TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS price_targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id INTEGER REFERENCES emails(id),
            date TEXT,
            instrument TEXT,
            ticker TEXT,
            target_price REAL,
            direction TEXT,
            condition TEXT,
            reached INTEGER DEFAULT 0,
            reached_date TEXT,
            raw_text TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(date);
        CREATE INDEX IF NOT EXISTS idx_signals_instrument ON signals(instrument);
        CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker);
        CREATE INDEX IF NOT EXISTS idx_cycles_date ON cycles(date);
        CREATE INDEX IF NOT EXISTS idx_cycles_instrument ON cycles(instrument);
        CREATE INDEX IF NOT EXISTS idx_emails_message_id ON emails(message_id);
        CREATE INDEX IF NOT EXISTS idx_emails_date ON emails(date_sent);
    """)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Instrument Mapping
# ---------------------------------------------------------------------------

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
_INSTRUMENT_LOOKUP = []
for name, info in INSTRUMENT_MAP.items():
    _INSTRUMENT_LOOKUP.append((name, name, info["ticker"], info["asset_class"]))
    for alias in info["aliases"]:
        _INSTRUMENT_LOOKUP.append((alias, name, info["ticker"], info["asset_class"]))

# Sort by length descending so longer matches take priority
_INSTRUMENT_LOOKUP.sort(key=lambda x: len(x[0]), reverse=True)


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


# ---------------------------------------------------------------------------
# Email Classification
# ---------------------------------------------------------------------------

def classify_email(subject: str) -> str:
    """Classify email type from subject line."""
    subject_lower = subject.lower()
    if "morning update" in subject_lower:
        return "morning_update"
    elif "intraday" in subject_lower:
        return "intraday_update"
    elif "stocks update" in subject_lower or "stocks cycle" in subject_lower:
        return "stocks_update"
    elif "sunday cycle" in subject_lower and "stock" not in subject_lower:
        return "sunday_cycles"
    elif "special report" in subject_lower or "special update" in subject_lower:
        return "special_report"
    elif "weekly overview" in subject_lower:
        return "weekly_overview"
    else:
        return "other"


# ---------------------------------------------------------------------------
# Text Extraction
# ---------------------------------------------------------------------------

def extract_text_from_email(msg) -> str:
    """Extract clean text from email message object."""
    body = ""
    
    if msg.is_multipart():
        # Prefer plain text
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                try:
                    body = part.get_content()
                except Exception:
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode("utf-8", errors="replace")
                break
        
        # Fall back to HTML
        if not body:
            for part in msg.walk():
                ct = part.get_content_type()
                if ct == "text/html":
                    try:
                        html_content = part.get_content()
                    except Exception:
                        payload = part.get_payload(decode=True)
                        html_content = payload.decode("utf-8", errors="replace") if payload else ""
                    body = re.sub(r"<[^>]+>", " ", html_content)
                    body = re.sub(r"\s+", " ", body)
                    body = html.unescape(body)
                    break
    else:
        try:
            body = msg.get_content()
        except Exception:
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode("utf-8", errors="replace")

    return body


# ---------------------------------------------------------------------------
# Signal Parser
# ---------------------------------------------------------------------------

# Compiled regex patterns
RE_ACTIVE = re.compile(
    r'[Cc]ontinues?\s+(?:on\s+a\s+|the\s+)?(buy|sell|move)\s+(?:signal\s+)?'
    r'from\s+([\d,\.]+)\s+as\s+long\s+as\s+there\s+is\s+no\s+'
    r'(?:good\s+)?(?:(hourly)\s+)?close\s+(above|below)\s+'
    r'(?:the\s+trend\s+line,?\s*(?:around\s+)?)?([\d,\.]+)'
    r'(\s*\(note\s+the\s+change\))?',
    re.IGNORECASE
)

RE_CANCELLED = re.compile(
    r'[Cc]ancelled\s+the\s+(buy|sell|move)\s+(?:signal\s+)?'
    r'from\s+([\d,\.]+)\s+(?:again\s+)?with\s+the\s+(?:theo?\s+)?'
    r'(?:(hourly)\s+)?close\s+(above|below)\s+([\d,\.]+)',
    re.IGNORECASE
)

RE_TRIGGER = re.compile(
    r'[Aa]\s+close\s+(?:now\s+)?(above|below)\s+([\d,\.]+)\s+'
    r'will\s+(?:give|resume)\s+(?:a\s+)?(?:new\s+)?(buy|sell)',
    re.IGNORECASE
)

RE_TARGET = re.compile(
    r'(?:[Tt]here\s+is\s+(?:still\s+)?(?:a\s+|an\s+)?(?:new\s+)?)'
    r'(upside|downside)\s+price\s+target\s+(?:at|of)\s+([\d,\.]+)',
    re.IGNORECASE
)

RE_CYCLE = re.compile(
    r'[Tt]he\s+(daily|weekly|monthly|dominant|hourly|dominant\s+daily|'
    r'dominant\s+weekly|longer\s+term)\s+cycle[s]?\s+'
    r'(?:is\s+|continues?\s+|projects?\s+(?:a\s+)?|has\s+|turned?\s+|support\s+)'
    r'(up|down|(?:a\s+)?(?:top|bottom|low|high|bottomed|topped|an\s+up\s+move))'
    r'(?:\s+(?:until|into|for|again|next|by|this|another)\s+([^\.]+))?',
    re.IGNORECASE
)

RE_NOTE_CHANGE = re.compile(r'\(note\s+the\s+change\)', re.IGNORECASE)


def parse_price(s: str) -> Optional[float]:
    """Parse a price string like '6,950' or '1.1880' into a float."""
    if not s:
        return None
    try:
        cleaned = s.replace(",", "").rstrip(".")
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def get_section_instrument(text_before: str) -> tuple[str, str, str]:
    """
    Determine the current instrument context based on the section header
    that precedes a signal sentence in the email.
    
    Strategy: search backward through the text for known instrument markers.
    We look at the NEAREST instrument header to avoid misattribution when
    instruments appear sequentially (e.g., VIX then TSX then DAX).
    """
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
    
    best_pos = -1
    best_result = ("Unknown", "UNK", "Unknown")
    
    for pattern, name, ticker, asset_class in SECTION_HEADERS:
        for m in re.finditer(pattern, text_before):
            if m.start() > best_pos:
                best_pos = m.start()
                best_result = (name, ticker, asset_class)
    
    return best_result


def parse_email_signals(body: str, email_date: str, email_id: int) -> dict:
    """
    Parse all signals, cycles, and price targets from an email body.
    Returns dict with lists of signals, cycles, and price_targets.
    """
    results = {"signals": [], "cycles": [], "price_targets": []}
    
    # ----- Parse Active Signals -----
    for m in RE_ACTIVE.finditer(body):
        text_before = body[:m.start()]
        
        # Identify instrument from section context
        inst, ticker, asset_class = get_section_instrument(text_before)
        
        signal_type = m.group(1).upper()
        if signal_type == "MOVE":
            signal_type = "DIRECTIONAL"
        
        origin = parse_price(m.group(2))
        hourly = m.group(3) is not None
        cancel_dir = m.group(4).upper()
        cancel_lvl = parse_price(m.group(5))
        ntc = 1 if m.group(6) else 0
        
        # Check for note the change right after match
        after_text = body[m.end():m.end()+30]
        if RE_NOTE_CHANGE.search(after_text):
            ntc = 1
        
        results["signals"].append({
            "email_id": email_id,
            "date": email_date,
            "instrument": inst,
            "ticker": ticker,
            "asset_class": asset_class,
            "signal_type": signal_type,
            "signal_status": "ACTIVE",
            "origin_price": origin,
            "cancel_direction": cancel_dir,
            "cancel_level": cancel_lvl,
            "trigger_direction": None,
            "trigger_level": None,
            "price_target": None,
            "target_direction": None,
            "note_the_change": ntc,
            "uses_hourly_close": 1 if hourly else 0,
            "raw_text": m.group(0).strip()[:500],
        })

    # ----- Parse Cancelled Signals -----
    for m in RE_CANCELLED.finditer(body):
        text_before = body[:m.start()]
        
        inst, ticker, asset_class = get_section_instrument(text_before)
        
        signal_type = m.group(1).upper()
        if signal_type == "MOVE":
            signal_type = "DIRECTIONAL"
        
        origin = parse_price(m.group(2))
        hourly = m.group(3) is not None
        cancel_dir = m.group(4).upper()
        cancel_lvl = parse_price(m.group(5))
        
        # Look for the trigger sentence that usually follows
        after_text = body[m.end():m.end()+200]
        trigger_match = RE_TRIGGER.search(after_text)
        trigger_dir = None
        trigger_lvl = None
        if trigger_match:
            trigger_dir = trigger_match.group(1).upper()
            trigger_lvl = parse_price(trigger_match.group(2))
        
        results["signals"].append({
            "email_id": email_id,
            "date": email_date,
            "instrument": inst,
            "ticker": ticker,
            "asset_class": asset_class,
            "signal_type": signal_type,
            "signal_status": "CANCELLED",
            "origin_price": origin,
            "cancel_direction": cancel_dir,
            "cancel_level": cancel_lvl,
            "trigger_direction": trigger_dir,
            "trigger_level": trigger_lvl,
            "price_target": None,
            "target_direction": None,
            "note_the_change": 0,
            "uses_hourly_close": 1 if hourly else 0,
            "raw_text": m.group(0).strip()[:500],
        })

    # ----- Parse Price Targets -----
    for m in RE_TARGET.finditer(body):
        text_before = body[:m.start()]
        inst, ticker, asset_class = get_section_instrument(text_before)
        
        direction = m.group(1).upper()
        target = parse_price(m.group(2))
        
        # Check for condition (e.g., "as long as it stays on a sell signal")
        after = body[m.end():m.end()+100]
        condition = ""
        cond_match = re.search(r'as\s+long\s+as\s+it\s+stays\s+on\s+a\s+(buy|sell)\s+signal', after, re.IGNORECASE)
        if cond_match:
            condition = f"stays on {cond_match.group(1).lower()} signal"
        
        results["price_targets"].append({
            "email_id": email_id,
            "date": email_date,
            "instrument": inst,
            "ticker": ticker,
            "target_price": target,
            "direction": direction,
            "condition": condition,
            "raw_text": m.group(0).strip()[:500],
        })

    # ----- Parse Cycle Directions -----
    for m in RE_CYCLE.finditer(body):
        text_before = body[:m.start()]
        inst, ticker, asset_class = get_section_instrument(text_before)
        
        timeframe = m.group(1).strip().lower()
        direction_raw = m.group(2).strip().lower()
        until = m.group(3).strip() if m.group(3) else ""
        
        # Normalize direction
        if any(w in direction_raw for w in ["up", "bottom", "bottomed", "up move"]):
            direction = "UP"
        elif any(w in direction_raw for w in ["down", "top", "topped"]):
            direction = "DOWN"
        else:
            direction = direction_raw.upper()
        
        results["cycles"].append({
            "email_id": email_id,
            "date": email_date,
            "instrument": inst,
            "ticker": ticker,
            "timeframe": timeframe,
            "direction": direction,
            "until_description": until[:200],
            "raw_text": m.group(0).strip()[:500],
        })

    # ----- Post-process: Fix crypto attribution by price magnitude -----
    # Bitcoin signals have prices > 10,000; GBTC prices are < 200
    # Ethereum signals have prices > 500; ETHE prices are < 100
    for sig in results["signals"]:
        if sig["ticker"] == "GBTC" and sig["origin_price"] and sig["origin_price"] > 1000:
            sig["instrument"] = "Bitcoin"
            sig["ticker"] = "BTC"
            sig["asset_class"] = "Crypto"
        elif sig["ticker"] == "ETHE" and sig["origin_price"] and sig["origin_price"] > 100:
            sig["instrument"] = "Ethereum"
            sig["ticker"] = "ETH"
            sig["asset_class"] = "Crypto"

    return results


# ---------------------------------------------------------------------------
# Database Insert
# ---------------------------------------------------------------------------

def store_email(conn: sqlite3.Connection, message_id: str, subject: str,
                date_sent: str, email_type: str, raw_text: str) -> Optional[int]:
    """Store email metadata. Returns email_id or None if duplicate."""
    try:
        cur = conn.execute(
            "INSERT INTO emails (message_id, subject, date_sent, date_parsed, email_type, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (message_id, subject, date_sent, datetime.now().isoformat(), email_type, raw_text)
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        # Duplicate message_id
        return None


def store_parsed_results(conn: sqlite3.Connection, results: dict, email_id: int):
    """Store parsed signals, cycles, and price targets."""
    for sig in results["signals"]:
        conn.execute(
            "INSERT INTO signals (email_id, date, instrument, ticker, asset_class, "
            "signal_type, signal_status, origin_price, cancel_direction, cancel_level, "
            "trigger_direction, trigger_level, price_target, target_direction, "
            "note_the_change, uses_hourly_close, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (sig["email_id"], sig["date"], sig["instrument"], sig["ticker"],
             sig["asset_class"], sig["signal_type"], sig["signal_status"],
             sig["origin_price"], sig["cancel_direction"], sig["cancel_level"],
             sig["trigger_direction"], sig["trigger_level"],
             sig["price_target"], sig["target_direction"],
             sig["note_the_change"], sig["uses_hourly_close"], sig["raw_text"])
        )

    for cyc in results["cycles"]:
        conn.execute(
            "INSERT INTO cycles (email_id, date, instrument, ticker, timeframe, "
            "direction, until_description, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (cyc["email_id"], cyc["date"], cyc["instrument"], cyc["ticker"],
             cyc["timeframe"], cyc["direction"], cyc["until_description"], cyc["raw_text"])
        )

    for tgt in results["price_targets"]:
        conn.execute(
            "INSERT INTO price_targets (email_id, date, instrument, ticker, "
            "target_price, direction, condition, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (tgt["email_id"], tgt["date"], tgt["instrument"], tgt["ticker"],
             tgt["target_price"], tgt["direction"], tgt["condition"], tgt["raw_text"])
        )

    # Update signal count
    total = len(results["signals"]) + len(results["cycles"]) + len(results["price_targets"])
    conn.execute("UPDATE emails SET signal_count = ? WHERE id = ?", (total, email_id))
    conn.commit()


# ---------------------------------------------------------------------------
# IMAP Connector
# ---------------------------------------------------------------------------

def connect_imap(gmail_addr: str, gmail_pass: str) -> imaplib.IMAP4_SSL:
    """Connect to Gmail IMAP."""
    log.info(f"Connecting to {IMAP_SERVER}...")
    imap = imaplib.IMAP4_SSL(IMAP_SERVER)
    imap.login(gmail_addr, gmail_pass)
    log.info(f"Authenticated as {gmail_addr}")
    return imap


def fetch_nenner_emails(imap: imaplib.IMAP4_SSL, since_date: str = None,
                        limit: int = None) -> list:
    """
    Fetch all emails from Nenner.
    since_date: IMAP date string like '01-Jan-2024'
    Returns list of (uid, message) tuples.
    """
    imap.select('"[Gmail]/All Mail"')
    
    search_criteria = f'(FROM "{NENNER_SENDER}")'
    if since_date:
        search_criteria = f'(FROM "{NENNER_SENDER}" SINCE {since_date})'
    
    log.info(f"Searching: {search_criteria}")
    status, data = imap.search(None, search_criteria)
    
    if status != "OK":
        log.error(f"IMAP search failed: {status}")
        return []
    
    uids = data[0].split()
    log.info(f"Found {len(uids)} Nenner emails")
    
    if limit:
        uids = uids[-limit:]
        log.info(f"Processing last {limit} emails")
    
    messages = []
    for i, uid in enumerate(uids):
        if i % 100 == 0 and i > 0:
            log.info(f"  Fetching {i}/{len(uids)}...")
        
        status, msg_data = imap.fetch(uid, "(RFC822)")
        if status == "OK" and msg_data[0] is not None:
            raw = msg_data[0][1]
            msg = BytesParser(policy=policy.default).parsebytes(raw)
            messages.append((uid.decode(), msg))
    
    log.info(f"Fetched {len(messages)} emails")
    return messages


# ---------------------------------------------------------------------------
# Processing Pipeline
# ---------------------------------------------------------------------------

def process_email(conn: sqlite3.Connection, msg, source_id: str = None) -> bool:
    """
    Process a single email message: extract text, parse signals, store in DB.
    Returns True if new email was processed, False if duplicate.
    """
    subject = msg.get("subject", "No Subject")
    date_str = msg.get("date", "")
    message_id = msg.get("message-id", source_id or f"local-{hash(subject + date_str)}")
    
    # Parse date
    try:
        from email.utils import parsedate_to_datetime
        date_obj = parsedate_to_datetime(date_str)
        email_date = date_obj.strftime("%Y-%m-%d")
    except Exception:
        email_date = date_str[:10] if date_str else "unknown"
    
    # Classify
    email_type = classify_email(subject)
    
    # Extract text
    body = extract_text_from_email(msg)
    if not body or len(body) < 50:
        log.warning(f"Skipping empty email: {subject}")
        return False
    
    # Store email
    email_id = store_email(conn, message_id, subject, email_date, email_type, body)
    if email_id is None:
        return False  # Duplicate
    
    # Parse signals
    results = parse_email_signals(body, email_date, email_id)
    
    # Store results
    store_parsed_results(conn, results, email_id)
    
    sig_count = len(results["signals"])
    cyc_count = len(results["cycles"])
    tgt_count = len(results["price_targets"])
    
    log.info(
        f"Parsed: {subject[:60]}  |  "
        f"Signals: {sig_count}  Cycles: {cyc_count}  Targets: {tgt_count}"
    )
    return True


def backfill_imap(conn: sqlite3.Connection):
    """Pull all historical Nenner emails via IMAP and parse them."""
    gmail_addr, gmail_pass = get_credentials()
    imap = connect_imap(gmail_addr, gmail_pass)
    
    try:
        messages = fetch_nenner_emails(imap)
        new_count = 0
        skip_count = 0
        
        for uid, msg in messages:
            if process_email(conn, msg, source_id=f"imap-{uid}"):
                new_count += 1
            else:
                skip_count += 1
        
        log.info(f"Backfill complete: {new_count} new, {skip_count} skipped (duplicates)")
    finally:
        imap.logout()


def check_new_emails(conn: sqlite3.Connection):
    """Check for new emails since last run (incremental mode)."""
    # Get the most recent email date in our database
    row = conn.execute("SELECT MAX(date_sent) FROM emails").fetchone()
    last_date = row[0] if row[0] else "2020-01-01"
    
    # Go back 2 days to catch any we might have missed
    try:
        dt = datetime.strptime(last_date, "%Y-%m-%d") - timedelta(days=2)
        since_str = dt.strftime("%d-%b-%Y")
    except ValueError:
        since_str = "01-Jan-2020"
    
    gmail_addr, gmail_pass = get_credentials()
    imap = connect_imap(gmail_addr, gmail_pass)
    
    try:
        messages = fetch_nenner_emails(imap, since_date=since_str)
        new_count = 0
        
        for uid, msg in messages:
            if process_email(conn, msg, source_id=f"imap-{uid}"):
                new_count += 1
        
        if new_count > 0:
            log.info(f"Found {new_count} new emails")
        else:
            log.info("No new emails")
    finally:
        imap.logout()


def import_eml_folder(conn: sqlite3.Connection, folder_path: str):
    """Import .eml files from a local folder."""
    folder = Path(folder_path)
    if not folder.exists():
        log.error(f"Folder not found: {folder_path}")
        return
    
    eml_files = sorted(folder.glob("*.eml"))
    if not eml_files:
        log.warning(f"No .eml files found in {folder_path}")
        return
    
    log.info(f"Found {len(eml_files)} .eml files in {folder_path}")
    new_count = 0
    
    for eml_file in eml_files:
        try:
            with open(eml_file, "rb") as f:
                msg = BytesParser(policy=policy.default).parse(f)
            if process_email(conn, msg, source_id=f"file-{eml_file.name}"):
                new_count += 1
        except Exception as e:
            log.error(f"Error parsing {eml_file.name}: {e}")
    
    log.info(f"Import complete: {new_count} new emails from {len(eml_files)} files")


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def show_status(conn: sqlite3.Connection):
    """Print current signal state for all instruments."""
    print("\n" + "=" * 90)
    print("  NENNER SIGNAL STATUS  |  Latest signals per instrument")
    print("=" * 90)
    
    # Get latest signal per instrument
    rows = conn.execute("""
        SELECT s.instrument, s.ticker, s.asset_class, s.signal_type, s.signal_status,
               s.origin_price, s.cancel_direction, s.cancel_level,
               s.trigger_direction, s.trigger_level, s.note_the_change,
               s.date, s.uses_hourly_close
        FROM signals s
        INNER JOIN (
            SELECT instrument, ticker, MAX(date) as max_date
            FROM signals
            GROUP BY instrument, ticker
        ) latest ON s.instrument = latest.instrument 
               AND s.ticker = latest.ticker 
               AND s.date = latest.max_date
        ORDER BY s.asset_class, s.instrument, s.ticker
    """).fetchall()
    
    current_class = ""
    for row in rows:
        if row["asset_class"] != current_class:
            current_class = row["asset_class"]
            print(f"\n  [{current_class}]")
            print(f"  {'Instrument':<25} {'Ticker':<8} {'Signal':<6} {'Status':<10} "
                  f"{'From':>10} {'Cancel':>10} {'Trigger':>10} {'NTC':<4} {'Date':<12}")
            print("  " + "-" * 105)
        
        signal = row["signal_type"][:4] if row["signal_type"] else "----"
        status = row["signal_status"][:6] if row["signal_status"] else "------"
        ntc = " *" if row["note_the_change"] else "  "
        hourly = "(H)" if row["uses_hourly_close"] else "   "
        
        origin = f"{row['origin_price']:>10,.2f}" if row["origin_price"] else "      ----"
        cancel = f"{row['cancel_level']:>10,.2f}" if row["cancel_level"] else "      ----"
        trigger = f"{row['trigger_level']:>10,.2f}" if row["trigger_level"] else "      ----"
        
        print(f"  {row['instrument']:<25} {row['ticker']:<8} {signal:<6} {status:<10} "
              f"{origin} {cancel} {trigger} {ntc}{hourly} {row['date']:<12}")
    
    # Price targets
    print(f"\n{'=' * 90}")
    print("  ACTIVE PRICE TARGETS")
    print("=" * 90)
    
    targets = conn.execute("""
        SELECT instrument, ticker, target_price, direction, condition, date
        FROM price_targets
        WHERE reached = 0
        AND date = (SELECT MAX(date) FROM price_targets)
        ORDER BY instrument
    """).fetchall()
    
    print(f"  {'Instrument':<25} {'Ticker':<8} {'Target':>10} {'Direction':<10} {'Condition':<30} {'Date':<12}")
    print("  " + "-" * 95)
    for row in targets:
        target = f"{row['target_price']:>10,.2f}" if row["target_price"] else "      ----"
        print(f"  {row['instrument']:<25} {row['ticker']:<8} {target} {row['direction']:<10} "
              f"{row['condition']:<30} {row['date']:<12}")
    
    # Cycle summary
    print(f"\n{'=' * 90}")
    print("  CYCLE DIRECTIONS (Latest)")
    print("=" * 90)
    
    cycles = conn.execute("""
        SELECT instrument, ticker, timeframe, direction, until_description, date
        FROM cycles
        WHERE date = (SELECT MAX(date) FROM cycles)
        ORDER BY instrument, timeframe
    """).fetchall()
    
    print(f"  {'Instrument':<25} {'Ticker':<8} {'Timeframe':<15} {'Direction':<6} {'Until':<30} {'Date':<12}")
    print("  " + "-" * 95)
    for row in cycles:
        print(f"  {row['instrument']:<25} {row['ticker']:<8} {row['timeframe']:<15} "
              f"{row['direction']:<6} {row['until_description']:<30} {row['date']:<12}")
    
    # Database stats
    print(f"\n{'=' * 90}")
    print("  DATABASE STATS")
    print("=" * 90)
    email_count = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
    signal_count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    cycle_count = conn.execute("SELECT COUNT(*) FROM cycles").fetchone()[0]
    target_count = conn.execute("SELECT COUNT(*) FROM price_targets").fetchone()[0]
    min_date = conn.execute("SELECT MIN(date_sent) FROM emails").fetchone()[0]
    max_date = conn.execute("SELECT MAX(date_sent) FROM emails").fetchone()[0]
    print(f"  Emails: {email_count}  |  Signals: {signal_count}  |  Cycles: {cycle_count}  |  Targets: {target_count}")
    print(f"  Date range: {min_date} to {max_date}")
    print()


def show_history(conn: sqlite3.Connection, instrument: str):
    """Show signal history for an instrument."""
    print(f"\n  Signal History: {instrument}")
    print("=" * 100)
    
    rows = conn.execute("""
        SELECT date, signal_type, signal_status, origin_price, cancel_direction,
               cancel_level, trigger_level, note_the_change
        FROM signals
        WHERE instrument LIKE ? OR ticker LIKE ?
        ORDER BY date DESC, id DESC
        LIMIT 50
    """, (f"%{instrument}%", f"%{instrument}%")).fetchall()
    
    print(f"  {'Date':<12} {'Signal':<6} {'Status':<10} {'From':>10} {'Cancel Dir':<10} "
          f"{'Cancel Lvl':>10} {'Trigger':>10} {'NTC':<4}")
    print("  " + "-" * 80)
    
    for row in rows:
        origin = f"{row['origin_price']:>10,.2f}" if row["origin_price"] else "      ----"
        cancel = f"{row['cancel_level']:>10,.2f}" if row["cancel_level"] else "      ----"
        trigger = f"{row['trigger_level']:>10,.2f}" if row["trigger_level"] else "      ----"
        ntc = " *" if row["note_the_change"] else "  "
        print(f"  {row['date']:<12} {row['signal_type']:<6} {row['signal_status']:<10} "
              f"{origin} {row['cancel_direction'] or '':>10} {cancel} {trigger} {ntc}")
    print()


def export_csv(conn: sqlite3.Connection):
    """Export all tables to CSV files."""
    import csv
    
    tables = ["emails", "signals", "cycles", "price_targets"]
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    for table in tables:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        if not rows:
            continue
        
        csv_path = os.path.join(base_dir, f"nenner_{table}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(rows[0].keys())
            for row in rows:
                writer.writerow(tuple(row))
        
        log.info(f"Exported {len(rows)} rows to {csv_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Nenner Signal Engine - Parse and track Charles Nenner cycle signals",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python nenner_engine.py --backfill              Pull all historical emails from Gmail
  python nenner_engine.py                         Check for new emails (incremental)
  python nenner_engine.py --import-folder ./emls  Parse local .eml files
  python nenner_engine.py --status                Show current signal state
  python nenner_engine.py --history Gold           Show Gold signal history
  python nenner_engine.py --export                Export database to CSV
        """,
    )
    parser.add_argument("--backfill", action="store_true",
                        help="Pull all historical emails from Gmail via IMAP")
    parser.add_argument("--import-folder", type=str,
                        help="Import .eml files from a local folder")
    parser.add_argument("--status", action="store_true",
                        help="Show current signal state")
    parser.add_argument("--history", type=str,
                        help="Show signal history for an instrument (e.g., 'Gold', 'TSLA')")
    parser.add_argument("--export", action="store_true",
                        help="Export database tables to CSV files")
    parser.add_argument("--db", type=str, default=DB_PATH,
                        help=f"Database path (default: {DB_PATH})")
    
    args = parser.parse_args()
    
    # Initialize database
    conn = init_db(args.db)
    
    if args.status:
        show_status(conn)
    elif args.history:
        show_history(conn, args.history)
    elif args.export:
        export_csv(conn)
    elif args.import_folder:
        import_eml_folder(conn, args.import_folder)
    elif args.backfill:
        backfill_imap(conn)
    else:
        # Default: incremental check
        check_new_emails(conn)
    
    conn.close()


if __name__ == "__main__":
    main()
