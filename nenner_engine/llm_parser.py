"""
LLM-Based Signal Parser
========================
Replaces the regex-based parser with Claude Haiku for natural language
understanding of Nenner email signals. Uses the same output schema as
the legacy parser so downstream code (db.py, imap_client.py) is unchanged.
"""

import json
import logging
import os
import time
from typing import Optional

from .instruments import INSTRUMENT_MAP, get_instrument_map_json

log = logging.getLogger("nenner")

# ---------------------------------------------------------------------------
# Default model
# ---------------------------------------------------------------------------

DEFAULT_MODEL = "claude-haiku-4-5-20251001"

# ---------------------------------------------------------------------------
# System Prompt — The Signal Interpretation Rulebook
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_TEMPLATE = """\
You are a structured data extraction engine for Charles Nenner's cycle research trading signals. You parse email bodies and extract trading signals, cycle directions, and price targets.

## Signal Interpretation Rules

1. There are only two signal types: BUY or SELL. There is never a neutral signal. "Move" signals should be classified as the contextually appropriate BUY or SELL based on direction language.
2. A signal consists of: signal_type (BUY/SELL) + origin_price + cancel_direction (ABOVE/BELOW) + cancel_level.
3. When a signal is "cancelled" (price closed beyond the cancel level), it implies an automatic reversal to the opposite direction. The cancel_level becomes the new origin_price for the implied reversal.
4. Cancel levels can change between emails. When Nenner writes "(note the change)" it means the cancel level has been updated from a prior email.
5. Signals are evaluated on the daily close only (4:15 PM ET equities close). The exception is when Nenner specifies an "hourly close" — set uses_hourly_close to 1 in that case.
6. "Good close" exception: Nenner may say to wait for a "good close" before acting on a cancellation. He says this explicitly when it applies. If he uses "good close" language, the signal remains ACTIVE (not cancelled).
7. Trigger levels indicate where the NEXT signal in the opposite direction would be initiated after a cancellation. They do NOT change the current signal. Extract them as trigger_direction and trigger_level.
8. Price targets (Fibonacci levels) are informational. They do NOT affect signal direction. Extract them separately.
9. Cycle directions (daily, weekly, monthly, dominant, hourly, longer term) provide timing context. They do NOT change signals. Extract them separately.
10. When the same instrument appears in multiple sections of the same email, extract each signal occurrence.
11. These rules are consistent across ALL asset classes (equities, commodities, currencies, crypto, bonds).
12. A BUY signal has cancel_direction ABOVE (cancelled if price closes above the cancel level). Wait — that's wrong. A BUY signal means you are long. It would be cancelled with a close BELOW the cancel level. A SELL signal means you are short. It would be cancelled with a close ABOVE the cancel level. Extract the cancel_direction exactly as stated in the text.

## Instrument Attribution

Nenner's emails are organized by sections with instrument headers. Each signal sentence belongs to the instrument whose header most recently appeared above it.

### Crypto Price Magnitude Rules
- "Bitcoin & GBTC" section: If origin_price > 1000, it's Bitcoin (BTC). If origin_price < 200, it's GBTC.
- "Ethereum & ETHE" section: If origin_price > 500, it's Ethereum (ETH). If origin_price < 100, it's ETHE.

## Known Instruments

{instrument_map}

## Signal Patterns to Recognize

ACTIVE signals — the signal is currently in effect:
- "Continues on a buy/sell signal from [ORIGIN] as long as there is no close above/below [CANCEL]"
- "Continues the buy/sell signal from [ORIGIN] as long as there is no close above/below [CANCEL]"
- "Continue the buy/sell signal from [ORIGIN]..."
- "The buy/sell signal from [ORIGIN] continues as long as there is no close above/below [CANCEL]"
- Variations with "good close", "hourly close", "trend line, around [CANCEL]", "(note the change)"
- Any phrasing that means "the signal is still active" with an origin price and cancel level

CANCELLED signals — the signal was just cancelled:
- "Cancelled the buy/sell signal from [ORIGIN] with the close above/below [CANCEL]"
- "Cancelled the buy/sell signal from [ORIGIN] again with the close above/below [CANCEL]"
- Often followed by a trigger: "A close above/below [LEVEL] will give/resume a new buy/sell"

Price targets:
- "There is a/an/still/new upside/downside price target at/of [PRICE]"
- "reached our downside price target of [PRICE]" — extract with condition "reached"
- Any mention of upside or downside price targets with a specific price

Cycle directions:
- "The daily/weekly/monthly/dominant/hourly/longer term cycle is/continues/projects/has/turned up/down/top/bottom/low/high until/into/for/by [TIMEFRAME]"
- Normalize direction: up/bottom/bottomed/low → "UP"; down/top/topped/high → "DOWN"
- Extract the timeframe description (e.g., "next week", "the end of the month")

## Output Format

Return ONLY valid JSON with this exact structure. No markdown, no code fences, no explanation:
{{
  "signals": [
    {{
      "instrument": "string - full instrument name as it appears in INSTRUMENT_MAP keys",
      "ticker": "string - canonical ticker from INSTRUMENT_MAP",
      "asset_class": "string - asset class from INSTRUMENT_MAP",
      "signal_type": "BUY or SELL",
      "signal_status": "ACTIVE or CANCELLED",
      "origin_price": number or null,
      "cancel_direction": "ABOVE or BELOW",
      "cancel_level": number or null,
      "trigger_direction": "ABOVE or BELOW or null",
      "trigger_level": number or null,
      "price_target": null,
      "target_direction": null,
      "note_the_change": 0 or 1,
      "uses_hourly_close": 0 or 1,
      "raw_text": "the exact sentence(s) from the email that produced this signal"
    }}
  ],
  "cycles": [
    {{
      "instrument": "string - instrument name",
      "ticker": "string - canonical ticker",
      "timeframe": "daily or weekly or monthly or dominant or dominant daily or dominant weekly or hourly or longer term",
      "direction": "UP or DOWN",
      "until_description": "string - when the cycle turns, or empty string",
      "raw_text": "the exact sentence from the email"
    }}
  ],
  "price_targets": [
    {{
      "instrument": "string - instrument name",
      "ticker": "string - canonical ticker",
      "target_price": number,
      "direction": "UPSIDE or DOWNSIDE",
      "condition": "string - e.g. 'stays on sell signal' or empty string",
      "raw_text": "the exact sentence from the email"
    }}
  ]
}}

Return ONLY signals, cycles, and targets that are EXPLICITLY stated in the email text.
Do NOT infer or predict signals. Do NOT fabricate data.
If no signals/cycles/targets are found for a category, return an empty array.
"""


# ---------------------------------------------------------------------------
# Credential Retrieval
# ---------------------------------------------------------------------------

def _load_env():
    """Load .env file into os.environ."""
    for search_dir in [os.getcwd(),
                       os.path.dirname(os.path.dirname(os.path.abspath(__file__)))]:
        env_path = os.path.join(search_dir, ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        key, val = line.split("=", 1)
                        os.environ.setdefault(key.strip(), val.strip())
            break


def get_anthropic_api_key() -> str:
    """Get Anthropic API key from env vars, .env file, or Azure Key Vault.

    Lookup order:
      1. Environment variable ANTHROPIC_API_KEY (or .env file)
      2. Azure Key Vault secret 'anthropic-api-key'

    Raises ValueError if no key found.
    """
    _load_env()
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        return api_key.strip()

    # Try Azure Key Vault
    vault_url = os.environ.get("AZURE_KEYVAULT_URL")
    if vault_url:
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vault_url, credential=credential)
            secret_name = os.environ.get("ANTHROPIC_KEY_SECRET", "anthropic-api-key")
            secret = client.get_secret(secret_name)
            api_key = secret.value.strip().replace("\xa0", "")
            if api_key:
                return api_key
        except Exception as e:
            log.error(f"Azure Key Vault error (Anthropic): {e}")

    raise ValueError(
        "No Anthropic API key found. Set ANTHROPIC_API_KEY in .env "
        "or configure Azure Key Vault with secret 'anthropic-api-key'."
    )


# ---------------------------------------------------------------------------
# LLM Parser
# ---------------------------------------------------------------------------

_cached_api_key: Optional[str] = None


def _get_cached_api_key() -> str:
    """Cache the API key for the session to avoid repeated vault lookups."""
    global _cached_api_key
    if _cached_api_key is None:
        _cached_api_key = get_anthropic_api_key()
    return _cached_api_key


def _build_system_prompt() -> str:
    """Build the system prompt with the current instrument map."""
    return SYSTEM_PROMPT_TEMPLATE.format(instrument_map=get_instrument_map_json())


def _call_llm(body: str, api_key: str, model: str) -> dict:
    """Call the Anthropic API and return parsed JSON response.

    Retries up to 2 times with exponential backoff on transient errors.
    """
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    system_prompt = _build_system_prompt()

    user_message = (
        "Parse the following Nenner research email for trading signals, "
        "cycle directions, and price targets. Return ONLY valid JSON.\n\n"
        f"{body}"
    )

    last_error = None
    for attempt in range(3):
        try:
            message = client.messages.create(
                model=model,
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )
            response_text = message.content[0].text

            # Strip markdown code fences if present
            response_text = response_text.strip()
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                # Remove first and last fence lines
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                response_text = "\n".join(lines)

            return json.loads(response_text)

        except json.JSONDecodeError as e:
            log.error(f"LLM returned invalid JSON (attempt {attempt+1}): {e}")
            log.debug(f"Raw response: {response_text[:500]}")
            last_error = e
            # Don't retry JSON errors — the model gave a bad response
            break

        except Exception as e:
            last_error = e
            if attempt < 2:
                wait = 2 ** (attempt + 1)
                log.warning(f"LLM API error (attempt {attempt+1}), retrying in {wait}s: {e}")
                time.sleep(wait)
            else:
                log.error(f"LLM API failed after 3 attempts: {e}")

    # Return empty on failure
    log.error(f"LLM parsing failed: {last_error}")
    return {"signals": [], "cycles": [], "price_targets": []}


def _validate_signal(sig: dict) -> dict:
    """Ensure a signal dict has all required fields with correct types."""
    defaults = {
        "instrument": "Unknown",
        "ticker": "UNK",
        "asset_class": "Unknown",
        "signal_type": "BUY",
        "signal_status": "ACTIVE",
        "origin_price": None,
        "cancel_direction": "ABOVE",
        "cancel_level": None,
        "trigger_direction": None,
        "trigger_level": None,
        "price_target": None,
        "target_direction": None,
        "note_the_change": 0,
        "uses_hourly_close": 0,
        "raw_text": "",
    }
    result = {}
    for key, default in defaults.items():
        val = sig.get(key, default)
        # Normalize types
        if key in ("note_the_change", "uses_hourly_close"):
            result[key] = 1 if val else 0
        elif key in ("origin_price", "cancel_level", "trigger_level",
                      "price_target"):
            result[key] = float(val) if val is not None else None
        elif key in ("signal_type", "signal_status", "cancel_direction",
                      "trigger_direction", "target_direction"):
            result[key] = str(val).upper() if val else default
        else:
            result[key] = str(val) if val is not None else (default or "")
    return result


def _validate_cycle(cyc: dict) -> dict:
    """Ensure a cycle dict has all required fields."""
    return {
        "instrument": cyc.get("instrument", "Unknown"),
        "ticker": cyc.get("ticker", "UNK"),
        "timeframe": cyc.get("timeframe", "daily"),
        "direction": str(cyc.get("direction", "UP")).upper(),
        "until_description": str(cyc.get("until_description", ""))[:200],
        "raw_text": str(cyc.get("raw_text", ""))[:500],
    }


def _validate_target(tgt: dict) -> dict:
    """Ensure a price target dict has all required fields."""
    return {
        "instrument": tgt.get("instrument", "Unknown"),
        "ticker": tgt.get("ticker", "UNK"),
        "target_price": float(tgt["target_price"]) if tgt.get("target_price") else None,
        "direction": str(tgt.get("direction", "DOWNSIDE")).upper(),
        "condition": str(tgt.get("condition", "")),
        "raw_text": str(tgt.get("raw_text", ""))[:500],
    }


def _apply_crypto_fix(signals: list[dict]):
    """Fix crypto attribution by price magnitude (safety net)."""
    for sig in signals:
        if sig["ticker"] == "GBTC" and sig["origin_price"] and sig["origin_price"] > 1000:
            sig["instrument"] = "Bitcoin"
            sig["ticker"] = "BTC"
            sig["asset_class"] = "Crypto"
        elif sig["ticker"] == "ETHE" and sig["origin_price"] and sig["origin_price"] > 100:
            sig["instrument"] = "Ethereum"
            sig["ticker"] = "ETH"
            sig["asset_class"] = "Crypto"


def _validate_ticker(sig: dict) -> bool:
    """Check that the ticker exists in INSTRUMENT_MAP."""
    valid_tickers = {info["ticker"] for info in INSTRUMENT_MAP.values()}
    return sig["ticker"] in valid_tickers


def parse_email_signals_llm(body: str, email_date: str, email_id: int,
                             api_key: str = None,
                             model: str = DEFAULT_MODEL) -> dict:
    """Parse all signals, cycles, and price targets from an email body using LLM.

    Drop-in replacement for parser.parse_email_signals(). Returns the same
    dict structure: {"signals": [...], "cycles": [...], "price_targets": [...]}

    Args:
        body: Raw email body text.
        email_date: Date string (YYYY-MM-DD).
        email_id: Database email ID.
        api_key: Anthropic API key (auto-retrieved if None).
        model: Claude model ID.

    Returns:
        Dict with 'signals', 'cycles', and 'price_targets' lists.
    """
    if not body or len(body) < 50:
        return {"signals": [], "cycles": [], "price_targets": []}

    # Get API key
    if api_key is None:
        try:
            api_key = _get_cached_api_key()
        except ValueError as e:
            log.error(f"Cannot parse with LLM: {e}")
            return {"signals": [], "cycles": [], "price_targets": []}

    # Call LLM
    raw_result = _call_llm(body, api_key, model)

    # Validate and post-process signals
    signals = []
    for sig in raw_result.get("signals", []):
        validated = _validate_signal(sig)
        validated["email_id"] = email_id
        validated["date"] = email_date
        validated["raw_text"] = str(validated.get("raw_text", ""))[:500]
        if _validate_ticker(validated):
            signals.append(validated)
        else:
            log.warning(f"LLM returned unknown ticker '{validated['ticker']}', skipping")

    # Apply crypto fix as safety net
    _apply_crypto_fix(signals)

    # Validate and post-process cycles
    cycles = []
    for cyc in raw_result.get("cycles", []):
        validated = _validate_cycle(cyc)
        validated["email_id"] = email_id
        validated["date"] = email_date
        cycles.append(validated)

    # Validate and post-process price targets
    price_targets = []
    for tgt in raw_result.get("price_targets", []):
        validated = _validate_target(tgt)
        validated["email_id"] = email_id
        validated["date"] = email_date
        price_targets.append(validated)

    log.info(f"LLM parsed: {len(signals)} signals, {len(cycles)} cycles, "
             f"{len(price_targets)} targets")

    return {
        "signals": signals,
        "cycles": cycles,
        "price_targets": price_targets,
    }
