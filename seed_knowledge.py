"""
Seed Stanley Knowledge Base
=============================
Foundational trading rules and market nuances for the knowledge base.
Run once to populate, safe to re-run (uses INSERT OR IGNORE pattern).

Usage:
    python seed_knowledge.py
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "nenner_signals.db")

RULES = [
    # --- Cycle Logic ---
    ("cycle_logic", None,
     "When daily, weekly, and monthly cycles all align in the same direction, "
     "this is the highest conviction setup. Use full position size.",
     0.95, "institutional_experience"),

    ("cycle_logic", None,
     "Never fight the monthly cycle with a full-size position. If monthly is against you, "
     "reduce to 50% max or skip entirely.",
     0.90, "institutional_experience"),

    ("cycle_logic", None,
     "Daily cycle reversals within an aligned weekly+monthly trend are noise, not signals. "
     "Hold through daily cycle pullbacks when the larger cycles support the position.",
     0.85, "institutional_experience"),

    ("cycle_logic", None,
     "When cycles are about to turn (e.g., 'daily cycle is up into Friday' and today is Thursday), "
     "do NOT enter new positions. Wait for the turn to confirm.",
     0.80, "institutional_experience"),

    # --- Correlation Warnings ---
    ("correlation_warning", "GC",
     "Gold and Silver are highly correlated (typically >0.8). When Gold gets a cancel signal, "
     "expect Silver to follow within 1-3 days. Consider preemptive tightening on Silver "
     "when Gold breaks its cancel level.",
     0.85, "market_observation"),

    ("correlation_warning", "ES",
     "ES, NQ, and YM are essentially the same trade (equity beta). Never hold full-size "
     "positions in all three simultaneously. Pick the one with the best score and trade that.",
     0.90, "institutional_experience"),

    ("correlation_warning", None,
     "DXY strength typically pressures Gold, Silver, and Commodities. When DXY gets a BUY signal "
     "and Gold has a concurrent BUY signal, one of them is likely wrong. Wait for resolution.",
     0.75, "macro_framework"),

    ("correlation_warning", "ZB",
     "Bonds (ZB/ZN/TLT) and Equities (ES/NQ) tend to be inversely correlated in risk-off events. "
     "A long bond position can serve as a hedge to a long equity position. "
     "Don't count them as additive risk in the same cluster.",
     0.80, "institutional_experience"),

    ("correlation_warning", None,
     "In a true risk-off event, correlations converge to 1.0 — everything sells except bonds and "
     "USD. Historical cluster correlations may understate crisis risk.",
     0.85, "institutional_experience"),

    # --- Price Interpretation ---
    ("price_interpretation", None,
     "Hourly close cancel levels (uses_hourly_close=1) are tighter and more prone to whipsaw "
     "than daily close cancels. Reduce position size by 25% on hourly-close-based signals.",
     0.80, "market_observation"),

    ("price_interpretation", None,
     "When a signal is cancelled and then re-issued in the same direction within 5 days, "
     "this 're-entry' setup has lower conviction. The first attempt failed. Size at 85%.",
     0.75, "market_observation"),

    ("price_interpretation", None,
     "Price targets that cluster near round numbers (e.g., 3000, 5000, 100.00) "
     "are more significant because institutional orders and options strikes concentrate there.",
     0.70, "institutional_experience"),

    ("price_interpretation", None,
     "'Note the change' signals have historically been the highest-conviction Nenner signals. "
     "These represent explicit direction reversals and deserve full attention and sizing.",
     0.90, "backtesting"),

    # --- Risk Rules ---
    ("risk_rule", None,
     "If a cancel level is within 0.5% of current price at time of entry, "
     "the risk/reward is almost certainly unfavorable. Skip the trade or wait for a pullback "
     "that gives better distance.",
     0.85, "institutional_experience"),

    ("risk_rule", None,
     "After 3 consecutive losing trades in the same instrument, skip the next signal in that name. "
     "Either the instrument doesn't trend well for Nenner's methodology, "
     "or market conditions have changed.",
     0.75, "risk_management"),

    ("risk_rule", None,
     "Portfolio drawdown > 10% from peak triggers mandatory position reduction to 50% size. "
     "Drawdown > 15% triggers full flatten to cash. Preserving capital is the first rule.",
     0.95, "risk_management"),

    # --- Macro Framework ---
    ("macro_framework", None,
     "In a rising rate environment (ZB selling off, Fed hiking), equity indices and "
     "duration-sensitive assets (TLT, utilities, REITs) face headwinds. "
     "Short signals in these are higher conviction.",
     0.75, "macro_framework"),

    ("macro_framework", None,
     "When VIX spikes above 25 and equity signals are mixed, reduce ALL equity position sizes "
     "by 25%. Elevated VIX means wider stops get tested more frequently.",
     0.70, "institutional_experience"),

    ("macro_framework", "CL",
     "Crude oil is highly event-driven (OPEC, geopolitics, SPR releases). "
     "Nenner cycle timing on crude is less reliable around OPEC meetings and geopolitical events. "
     "Reduce conviction by 25% around scheduled OPEC dates.",
     0.70, "market_observation"),

    # --- Fixed Income Specific (trader background) ---
    ("fi_expertise", "ZB",
     "30-year bond futures (ZB) move approximately 31.25 per 1/32nd. "
     "A 1-point move = $1,000 per contract. Size accordingly for notional risk.",
     0.95, "institutional_experience"),

    ("fi_expertise", "ZN",
     "10-year note futures (ZN) have lower duration than ZB. "
     "A 1-point move = $1,000 per contract but the typical daily range is smaller. "
     "ZN requires larger size for equivalent P&L impact vs ZB.",
     0.95, "institutional_experience"),

    ("fi_expertise", None,
     "When yield curve is inverting (2s10s going negative) and Nenner signals BUY on ZB, "
     "this is a high-conviction recession/risk-off trade. The curve inversion confirms "
     "the cycle timing for a bond rally.",
     0.80, "institutional_experience"),

    # --- Agriculture ---
    ("asset_class_note", "SOYB",
     "Agricultural commodities (SOYB, WEAT, CORN) are heavily seasonal and weather-dependent. "
     "Nenner cycles are more reliable during planting/harvest seasons (March-May, August-October) "
     "when supply uncertainty drives price discovery.",
     0.70, "market_observation"),

    # --- Options Overlay ---
    ("options_overlay", None,
     "When entering a directional position with aligned cycles and SQS > 70, "
     "consider a covered call or put overlay to enhance income. "
     "Sell strikes beyond the Nenner price target — you're getting paid for upside "
     "you don't expect to capture anyway.",
     0.70, "institutional_experience"),

    ("options_overlay", None,
     "For positions with wide cancel distances (> 3.5%), a collar strategy "
     "(buy protective put near cancel level, sell covered call near target) "
     "defines max risk and partially funds the hedge.",
     0.70, "institutional_experience"),
]


def seed():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Check which rules already exist (by rule_text prefix match to avoid exact dupes)
    existing = set()
    try:
        rows = conn.execute("SELECT rule_text FROM stanley_knowledge WHERE active = 1").fetchall()
        existing = {r["rule_text"][:60] for r in rows}
    except Exception:
        pass

    inserted = 0
    skipped = 0
    for category, instrument, rule_text, confidence, source in RULES:
        prefix = rule_text[:60]
        if prefix in existing:
            skipped += 1
            continue

        conn.execute(
            "INSERT INTO stanley_knowledge (category, instrument, rule_text, confidence, source) "
            "VALUES (?, ?, ?, ?, ?)",
            (category, instrument, rule_text, confidence, source)
        )
        existing.add(prefix)
        inserted += 1

    conn.commit()
    conn.close()
    print(f"Knowledge base seeded: {inserted} new rules inserted, {skipped} duplicates skipped.")
    print(f"Total rules in seed file: {len(RULES)}")


if __name__ == "__main__":
    seed()
