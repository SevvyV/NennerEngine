"""
Signal Parser
==============
Regex-based extraction of Nenner signal grammar from email bodies.
Handles active signals, cancelled signals, price targets, cycle
directions, and email classification.
"""

import re
import html as html_lib
from typing import Optional

from .instruments import get_section_instrument


# ---------------------------------------------------------------------------
# Compiled Regex Patterns
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_price(s: str) -> Optional[float]:
    """Parse a price string like '6,950' or '1.1880' into a float."""
    if not s:
        return None
    try:
        cleaned = s.replace(",", "").rstrip(".")
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


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
                    body = html_lib.unescape(body)
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
