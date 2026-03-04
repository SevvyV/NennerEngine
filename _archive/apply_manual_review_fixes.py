"""
Apply Manual Review Fixes — Phase 2 of Signal Price Audit
==========================================================
1. Apply 24 user-reviewed corrections from Excel column O
2. Delete all TSX and MMM signals
3. Apply 19 mathematical corrections for remaining items (dollar notation,
   dropped digits, comma parsing, /100 factors)
4. Apply 8 YM/DIA *100 corrections (DIA ETF prices -> YM futures)
5. Re-attribute 9 ETF signals to correct tickers (WEAT, CUT, SOYB)
6. Clear 3 bad origin_prices (cross-instrument attribution)
7. Output final short list of ~12 truly ambiguous items
"""

import os
import shutil
import sqlite3
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nenner_signals.db")
BACKUP_PATH = DB_PATH.replace(".db", "_backup_pre_manual_review.db")


def main():
    # Backup first
    print(f"Backing up database...")
    shutil.copy2(DB_PATH, BACKUP_PATH)
    print(f"  Backup: {BACKUP_PATH}")

    conn = sqlite3.connect(DB_PATH)
    total_changes = 0

    # =====================================================================
    # PHASE 1: User-reviewed corrections (24 value updates)
    # =====================================================================
    print("\n" + "=" * 70)
    print("PHASE 1: User-reviewed corrections from Excel")
    print("=" * 70)

    user_fixes = [
        # (signal_id, field, old_value, new_value, note)
        (40156, "cancel_level", 11080, 523, "AEX sell level is 523 (was DAX value)"),
        (40168, "cancel_level", 1555, 15.55, "Silver notation 1555 = 15.55"),
        (40174, "cancel_level", 15555, 15.55, "Silver notation 15555 = 15.55"),
        (40192, "cancel_level", 1558, 15.58, "Silver notation 1558 = 15.58"),
        (40190, "cancel_level", 1820, 16.4, "VIX cancel 16.4 (was extreme 1820)"),
        (40212, "cancel_level", 1145.25, 145.25, "ZB bond notation 1145.25 = 145.25"),
        (40218, "cancel_level", 113.19, 1319, "Gold 113.19 -> 1319 (dropped leading 1)"),
        (41277, "cancel_level", 3.71, 371, "Corn dollar notation 3.71 = 371"),
        (41400, "origin_price", 3.92, 392, "Corn dollar notation 3.92 = 392"),
        (41400, "cancel_level", 3.92, 392, "Corn dollar notation 3.92 = 392"),
        (41401, "origin_price", 9.25, 925, "Soybeans dollar notation 9.25 = 925"),
        (41401, "cancel_level", 9.25, 925, "Soybeans dollar notation 9.25 = 925"),
        (41467, "origin_price", 3.92, 392, "Corn dollar notation 3.92 = 392"),
        (41467, "cancel_level", 3.82, 382, "Corn dollar notation 3.82 = 382"),
        (42093, "cancel_level", 3.85, 385, "Corn dollar notation 3.85 = 385"),
        (42115, "origin_price", 11, 1.11, "EUR/USD dropped digit: 11 -> 111 -> 1.11"),
        (42309, "cancel_level", 5720, 450, "AEX cancel 450 (5720 was DAX value, swapped)"),
        (42307, "cancel_level", 450, 8852, "DAX cancel 8852 (450 was AEX value, swapped)"),
        (42803, "cancel_level", 153.1, 15310, "TSX 153.1 -> 15310 (dropped digits)"),
        (42886, "origin_price", 96, 9600, "BTC 96 -> 9600 (dropped two zeros)"),
        (42886, "cancel_level", 96, 9600, "BTC 96 -> 9600 (dropped two zeros)"),
        (43136, "cancel_level", 18.16, 1816, "Gold 18.16 -> 1816 (was silver value on GC)"),
        (43584, "cancel_level", 17508, 175.08, "ZB bond notation 17508 = 175.08"),
        (43601, "cancel_level", 17508, 175.08, "ZB bond notation 17508 = 175.08"),
        (44876, "cancel_level", 178.9, 0.789, "AUD/USD: 178.9 -> 78.9 (dropped 1) -> 0.789"),
    ]

    for sig_id, field, old_val, new_val, note in user_fixes:
        conn.execute(f"UPDATE signals SET {field} = ? WHERE id = ?", (new_val, sig_id))
        print(f"  #{sig_id:6d} {field:14s}: {old_val} -> {new_val}  ({note})")
        total_changes += 1

    conn.commit()
    print(f"  Applied {len(user_fixes)} user-reviewed corrections")

    # =====================================================================
    # PHASE 2: Delete TSX and MMM signals
    # =====================================================================
    print("\n" + "=" * 70)
    print("PHASE 2: Remove TSX and MMM signals")
    print("=" * 70)

    for ticker in ["TSX", "MMM"]:
        count = conn.execute("SELECT COUNT(*) FROM signals WHERE ticker = ?", (ticker,)).fetchone()[0]
        conn.execute("DELETE FROM signals WHERE ticker = ?", (ticker,))
        print(f"  Deleted {count} {ticker} signals")
        total_changes += count

    conn.commit()

    # =====================================================================
    # PHASE 3: Mathematical corrections for remaining items
    # =====================================================================
    print("\n" + "=" * 70)
    print("PHASE 3: Mathematical corrections (dollar notation, dropped digits)")
    print("=" * 70)

    math_fixes = [
        # ZB bond notation
        (44241, "cancel_level", 17306, 173.06, "ZB bond notation 17306 = 173.06"),
        # ZC corn dollar notation (*100)
        (46197, "cancel_level", 5.8, 580, "Corn dollar notation 5.80 = 580 (close 659)"),
        (47519, "cancel_level", 5.3, 530, "Corn dollar notation 5.30 = 530 (close 539.5)"),
        (48172, "origin_price", 5.6, 560, "Corn dollar notation 5.60 = 560 (close 569)"),
        (48172, "cancel_level", 5.65, 565, "Corn dollar notation 5.65 = 565 (close 569)"),
        # AUD/USD missing digit
        (55066, "cancel_level", 0.0671, 0.6710, "AUD/USD .0671 -> .6710 (missing digit)"),
        # NQ comma parsing
        (55948, "cancel_level", 121420, 12142, "NQ comma parse: 121,420 -> 12,142 (close 12346)"),
        # GDXJ /100
        (56349, "cancel_level", 3350, 33.50, "GDXJ 3,350 -> 33.50 (/100, close 30.73)"),
        # USD/JPY *100
        (55760, "origin_price", 1.29, 129, "USD/JPY 1.29 -> 129 (*100, close 129.8)"),
        # ZS comma parsing
        (55054, "origin_price", 14075, 1407.5, "ZS 14,075 -> 1407.5 (comma parse, close 1480)"),
        # EUR/USD dropped digit
        (44541, "cancel_level", 22, 1.22, "EUR/USD 22 -> 122 -> 1.22 (dropped 1, close 1.216)"),
        # USD/CHF decimal error
        (45865, "cancel_level", 0.095, 0.9500, "USD/CHF .0950 -> .9500 (decimal shift, close 0.897)"),
        # GLD dropped leading 1
        (47476, "cancel_level", 64.5, 164.5, "GLD dropped 1: 64.5 -> 164.5 (close 161.32)"),
        (47552, "cancel_level", 64.5, 164.5, "GLD dropped 1: 64.5 -> 164.5 (close 164.59)"),
        # ZW dollar notation
        (50967, "cancel_level", 10.5, 1050, "Wheat dollar notation 10.50 = 1050 (close 1043.5)"),
        (55998, "origin_price", 7.9, 790, "Wheat dollar notation 7.90 = 790 (close 792)"),
        # ZS dollar notation
        (45701, "cancel_level", 14.5, 1450, "Soybeans dollar notation 14.50 = 1450 (close 1603)"),
        (45857, "cancel_level", 14.05, 1405, "Soybeans dollar notation 14.05 = 1405 (close 1526)"),
        # YM comma parsing
        (54492, "cancel_level", 3350, 33500, "YM 3,350 -> 33,500 (dropped 0, close 33591)"),
    ]

    for sig_id, field, old_val, new_val, note in math_fixes:
        conn.execute(f"UPDATE signals SET {field} = ? WHERE id = ?", (new_val, sig_id))
        print(f"  #{sig_id:6d} {field:14s}: {old_val} -> {new_val}  ({note})")
        total_changes += 1

    conn.commit()
    print(f"  Applied {len(math_fixes)} mathematical corrections")

    # =====================================================================
    # PHASE 4: YM/DIA corrections — DIA ETF price * 100 = YM
    # =====================================================================
    print("\n" + "=" * 70)
    print("PHASE 4: YM/DIA corrections (DIA ETF * 100 -> YM futures)")
    print("=" * 70)

    dia_fixes = [
        (48952, "cancel_level", 362, 36200, "DIA 362 * 100 = 36200 (YM close 36282)"),
        (48959, "cancel_level", 362, 36200, "DIA 362 * 100 = 36200 (YM close 36282)"),
        (49003, "cancel_level", 365, 36500, "DIA 365 * 100 = 36500 (YM close 36291)"),
        (49070, "cancel_level", 366, 36600, "DIA 366 * 100 = 36600 (YM close 36675)"),
        (49130, "cancel_level", 362, 36200, "DIA 362 * 100 = 36200 (YM close 36123)"),
        (49136, "cancel_level", 362, 36200, "DIA 362 * 100 = 36200 (YM close 36107)"),
        (54405, "cancel_level", 334, 33400, "DIA 334 * 100 = 33400 (YM close 33605)"),
        (54486, "cancel_level", 334, 33400, "DIA 334 * 100 = 33400 (YM close 33775)"),
    ]

    for sig_id, field, old_val, new_val, note in dia_fixes:
        conn.execute(f"UPDATE signals SET {field} = ? WHERE id = ?", (new_val, sig_id))
        print(f"  #{sig_id:6d} {field:14s}: {old_val} -> {new_val}  ({note})")
        total_changes += 1

    conn.commit()
    print(f"  Applied {len(dia_fixes)} DIA->YM corrections")

    # =====================================================================
    # PHASE 5: Re-attribute ETF signals to correct tickers
    # =====================================================================
    print("\n" + "=" * 70)
    print("PHASE 5: Re-attribute ETF signals (change ticker)")
    print("=" * 70)

    ticker_changes = [
        # ZW -> WEAT (explicitly WEAT ETF prices)
        (45174, "ZW", "WEAT", "WEAT ETF: buy above 6.10, sell stop 5.80"),
        (45286, "ZW", "WEAT", "WEAT ETF: long since close above 6.10"),
        (45365, "ZW", "WEAT", "WEAT ETF: long, cancel below 6.20"),
        (51618, "ZW", "WEAT", "WEAT ETF: explicitly says 'WEAT continues'"),
        # LBS -> CUT (explicitly CUT timber ETF)
        (46390, "LBS", "CUT", "CUT ETF: explicitly says 'CUT - the sell signal'"),
        (46759, "LBS", "CUT", "CUT ETF: explicitly says 'CUT continues'"),
        (47021, "LBS", "CUT", "CUT ETF: explicitly says 'CUT closed one tick above'"),
        (48214, "LBS", "CUT", "CUT ETF: explicitly says 'CUT the sell signal'"),
        # ZS -> SOYB (SOYB ETF price, not ZS futures)
        (46640, "ZS", "SOYB", "SOYB ETF: 30.80 is SOYB, not ZS at 1418"),
    ]

    for sig_id, old_ticker, new_ticker, note in ticker_changes:
        conn.execute("UPDATE signals SET ticker = ? WHERE id = ? AND ticker = ?",
                     (new_ticker, sig_id, old_ticker))
        print(f"  #{sig_id:6d} ticker: {old_ticker} -> {new_ticker}  ({note})")
        total_changes += 1

    # Also fix ZW #45174 cancel_level: raw text says "sell stop around 5.80" but stored as 6.1
    conn.execute("UPDATE signals SET cancel_level = 5.80 WHERE id = 45174")
    print(f"  #{45174:6d} cancel_level: 6.1 -> 5.80  (raw text: 'sell stop around 5.80')")
    total_changes += 1

    conn.commit()
    print(f"  Applied {len(ticker_changes)} ticker re-attributions + 1 cancel fix")

    # =====================================================================
    # PHASE 6: Clear bad origin_prices (cross-instrument attribution)
    # =====================================================================
    print("\n" + "=" * 70)
    print("PHASE 6: Clear bad origin_prices")
    print("=" * 70)

    origin_clears = [
        (40267, "YM", "origin_price is 2440 (S&P/ES), cancel_level 25820 is correct for YM"),
        (56447, "ZS", "origin 68 is unknown ETF; cancel_level 1490 is correct for ZS"),
        (56448, "LBS", "origin 68 is unknown ETF; cancel_level 395 is correct for LBS"),
    ]

    for sig_id, ticker, note in origin_clears:
        conn.execute("UPDATE signals SET origin_price = NULL WHERE id = ?", (sig_id,))
        print(f"  #{sig_id:6d} {ticker:6s} origin_price -> NULL  ({note})")
        total_changes += 1

    conn.commit()
    print(f"  Cleared {len(origin_clears)} bad origin_prices")

    # =====================================================================
    # FINAL: Output remaining ambiguous items
    # =====================================================================
    print("\n" + "=" * 70)
    print("REMAINING AMBIGUOUS ITEMS (for manual review)")
    print("=" * 70)

    ambiguous = [
        {
            "id": 41044, "ticker": "YM", "field": "cancel_level",
            "value": 6800, "close": 27309, "date": "2019-07-12",
            "note": "Dow was 27309. 6800 doesn't match any known instrument.",
            "suggestion": "DELETE or leave — genuinely unidentifiable"
        },
        {
            "id": 42418, "ticker": "YM", "field": "cancel_level",
            "value": 2347, "close": 22929, "date": "2020-04-21",
            "note": "User said 'Ignore' — hourly/day trading service signal.",
            "suggestion": "LEAVE as-is (day trading service, different instrument)"
        },
        {
            "id": 44423, "ticker": "YM", "field": "cancel_level",
            "value": 3440, "close": 30109, "date": "2020-12-27",
            "note": "ES (S&P) was 3727. Cancel at 3440 is plausible S&P support.",
            "suggestion": "Likely ES/S&P level misattributed to YM. Consider changing ticker to ES."
        },
        {
            "id": 46692, "ticker": "YM", "field": "cancel_level",
            "value": 3850, "close": 34690, "date": "2021-08-04",
            "note": "ES (S&P) was 4394. 3850 could be S&P or DIA-like.",
            "suggestion": "Likely cross-instrument. Consider changing ticker to ES."
        },
        {
            "id": 44910, "ticker": "NYFANG", "field": "origin+cancel",
            "value": 320, "close": 6775, "date": "2021-02-28",
            "note": "Raw text explicitly says 'QQQ closed below 320'. QQQ was ~320.",
            "suggestion": "Signal is for QQQ, not NYFANG. DELETE or re-attribute."
        },
        {
            "id": 47652, "ticker": "FXE", "field": "cancel_level",
            "value": 19.5, "close": 102.68, "date": "2021-10-10",
            "note": "FXE was 102.68. 19.5 doesn't match any obvious instrument.",
            "suggestion": "Possibly SLV (~20.9) or misattributed. DELETE or leave."
        },
        {
            "id": 47657, "ticker": "FXE", "field": "origin+cancel",
            "value": 19.5, "close": 102.68, "date": "2021-10-10",
            "note": "Same issue as #47652. From same/duplicate email.",
            "suggestion": "Possibly SLV (~20.9) or misattributed. DELETE or leave."
        },
        {
            "id": 49461, "ticker": "GDXJ", "field": "origin_price",
            "value": 140, "close": 34.94, "date": "2022-01-28",
            "note": "GDXJ was 34.94. 140 is exactly 4x — possible pre-split artifact?",
            "suggestion": "Unclear. Could be stale or wrong instrument."
        },
        {
            "id": 52413, "ticker": "GS", "field": "cancel_level",
            "value": 34, "close": 296.23, "date": "2022-07-24",
            "note": "GS was 296. 34 doesn't match any instrument cleanly.",
            "suggestion": "Likely misattributed from another instrument in same email."
        },
        {
            "id": 46317, "ticker": "LBS", "field": "cancel_level",
            "value": 145.5, "close": 792.9, "date": "2021-07-06",
            "note": "Lumber was 792.9. TLT was 126, not 145.5. No match found.",
            "suggestion": "Likely misattributed from multi-instrument intraday email."
        },
        {
            "id": 43412, "ticker": "ZC", "field": "cancel_level",
            "value": 12.11, "close": 327, "date": "2020-08-18",
            "note": "User noted: 'may be referring to CORN ETF'. Not ZC notation.",
            "suggestion": "Change ticker to CORN or leave with note."
        },
        {
            "id": 56679, "ticker": "NG", "field": "cancel_level",
            "value": 6.46, "close": 2.088, "date": "2023-03-27",
            "note": "NG was 2.09 but was ~6.46 in Nov 2022. Stale resistance?",
            "suggestion": "Could be legitimate stale level or UNG ETF. LEAVE as-is."
        },
    ]

    for i, item in enumerate(ambiguous, 1):
        print(f"\n  {i:2d}. #{item['id']} {item['ticker']} {item['date']}  "
              f"{item['field']}={item['value']}  (close={item['close']})")
        print(f"      {item['note']}")
        print(f"      -> {item['suggestion']}")

    # Write ambiguous list to file
    amb_path = os.path.join(os.path.dirname(DB_PATH), "remaining_ambiguous_12.txt")
    with open(amb_path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("REMAINING AMBIGUOUS ITEMS AFTER PHASE 2 FIXES\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"Total: {len(ambiguous)} items (down from 85)\n")
        f.write("=" * 80 + "\n\n")
        for i, item in enumerate(ambiguous, 1):
            f.write(f"{i:2d}. Signal #{item['id']}  {item['ticker']}  {item['date']}\n")
            f.write(f"    {item['field']} = {item['value']}  (actual close: {item['close']})\n")
            f.write(f"    {item['note']}\n")
            f.write(f"    Suggestion: {item['suggestion']}\n\n")

    # =====================================================================
    # SUMMARY
    # =====================================================================
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Phase 1 — User corrections:      {len(user_fixes)} value updates")
    print(f"  Phase 2 — TSX/MMM deletion:       971 signals removed")
    print(f"  Phase 3 — Math corrections:        {len(math_fixes)} value updates")
    print(f"  Phase 4 — DIA->YM (*100):           {len(dia_fixes)} value updates")
    print(f"  Phase 5 — ETF re-attribution:      {len(ticker_changes)} ticker changes + 1 cancel fix")
    print(f"  Phase 6 — Bad origin clearing:     {len(origin_clears)} set to NULL")
    print(f"  ─────────────────────────────────────────────")
    print(f"  Total changes:                     {total_changes}")
    print(f"  Remaining ambiguous:               {len(ambiguous)} items")
    print(f"  Ambiguous list:                    {amb_path}")
    print(f"  Backup:                            {BACKUP_PATH}")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
