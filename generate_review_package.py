"""
Generate a review package (ZIP) for the 79 MANUAL + 6 CHECK flagged signals.
Creates:
  1. manual_review_items.csv — all 85 items sortable/filterable
  2. review_summary.txt — narrative summary with unique email count
  3. emails_to_review/ — one .txt file per unique email with all flagged signals from that email
"""

import csv
import os
import sqlite3
import zipfile
from collections import defaultdict
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nenner_signals.db")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "review_package")
ZIP_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manual_review_85_signals.zip")

# All signal IDs from the MANUAL + CHECK categories in the post-fix audit
MANUAL_IDS = [
    40156, 42309,           # AEX
    44876, 55066,           # AUD/USD
    42886,                  # BTC
    42307,                  # DAX
    42115, 44541,           # EUR/USD
    47652, 47657,           # FXE
    40218, 43136,           # GC
    56349,                  # GDXJ
    52413,                  # GS
    46317, 46390, 46759, 47021, 48214, 56448,  # LBS
    42070,                  # NG
    55948,                  # NQ
    44910,                  # NYFANG
    40168, 40174, 40192,    # SI
    42803, 44247, 44298,    # TSX
    45865,                  # USD/CHF
    55760,                  # USD/JPY
    40190,                  # VIX
    40267, 42418, 44423, 46692, 48952, 48959, 49003, 49070, 49130, 49136, 54405, 54492, 54486,  # YM
    40212, 43584, 43601, 44241,  # ZB
    41277, 41400, 41467, 42093, 43412, 46197, 47519, 48172,  # ZC
    41401, 45701, 45857, 46640, 55054, 56447,  # ZS
    45174, 45286, 45365, 50967, 51618, 55998,  # ZW
]

CHECK_IDS = [
    49461,                  # GDXJ
    47476, 47552,           # GLD
    42766,                  # MMM
    56679,                  # NG
    41044,                  # YM
]

# Audit findings — which field is flagged and what the actual close was
# We'll re-derive this from the audit logic rather than hardcode
from signal_price_audit import (
    YFINANCE_MAP, ORIGIN_FLAG_RATIO, CANCEL_FLAG_RATIO,
    SPLIT_ADJUSTED_TICKERS, classify_error
)

import pandas as pd


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    all_ids = MANUAL_IDS + CHECK_IDS
    placeholders = ",".join("?" * len(all_ids))

    # Get signals with email info
    rows = conn.execute(f"""
        SELECT s.id, s.date, s.ticker, s.origin_price, s.cancel_level,
               s.signal_type, s.signal_status, s.raw_text, s.email_id,
               e.subject as email_subject, e.date_sent as email_date
        FROM signals s
        LEFT JOIN emails e ON s.email_id = e.id
        WHERE s.id IN ({placeholders})
        ORDER BY s.ticker, s.date
    """, all_ids).fetchall()

    # Load price history from DB for cross-referencing
    price_rows = conn.execute("""
        SELECT ticker, date, close FROM price_history
        WHERE close IS NOT NULL
        ORDER BY ticker, date
    """).fetchall()

    closes = {}
    for pr in price_rows:
        t = pr["ticker"]
        if t not in closes:
            closes[t] = {}
        closes[t][pr["date"]] = pr["close"]
    closes_series = {t: pd.Series(data) for t, data in closes.items()}

    # Build flagged items with full details
    items = []
    for row in rows:
        sig_id = row["id"]
        ticker = row["ticker"]
        date_str = row["date"]
        severity = "MANUAL" if sig_id in MANUAL_IDS else "CHECK"

        # Find actual close
        actual_close = None
        actual_date = None
        if ticker in closes_series:
            series = closes_series[ticker]
            if date_str in series.index:
                actual_close = float(series[date_str])
                actual_date = date_str
            else:
                from datetime import timedelta
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    for i in range(1, 8):
                        prior = (dt - timedelta(days=i)).strftime("%Y-%m-%d")
                        if prior in series.index:
                            actual_close = float(series[prior])
                            actual_date = prior
                            break
                except ValueError:
                    pass

        # Check which fields are flagged
        for field in ["origin_price", "cancel_level"]:
            val = row[field]
            if val is None or actual_close is None or actual_close <= 0:
                continue

            ratio = val / actual_close
            threshold = ORIGIN_FLAG_RATIO if field == "origin_price" else CANCEL_FLAG_RATIO

            if ratio > threshold or ratio < (1.0 / threshold):
                sev, suggestion, corrected = classify_error(ticker, field, val, actual_close, ratio)

                # Only include MANUAL and CHECK items
                if sev not in ("MANUAL", "CHECK"):
                    continue

                items.append({
                    "signal_id": sig_id,
                    "ticker": ticker,
                    "signal_date": date_str,
                    "field": field,
                    "signal_value": val,
                    "actual_close": actual_close,
                    "close_date": actual_date,
                    "ratio": ratio,
                    "severity": sev,
                    "suggestion": suggestion,
                    "signal_type": row["signal_type"],
                    "email_id": row["email_id"],
                    "email_date": row["email_date"],
                    "email_subject": (row["email_subject"] or "").strip(),
                    "raw_text": (row["raw_text"] or "").strip(),
                })

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- 1. CSV ---
    csv_path = os.path.join(OUTPUT_DIR, "manual_review_items.csv")
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "severity", "signal_id", "ticker", "signal_date", "field",
            "signal_value", "actual_close", "ratio", "suggestion",
            "signal_type", "email_date", "email_subject", "raw_text"
        ])
        writer.writeheader()
        for item in items:
            writer.writerow({
                "severity": item["severity"],
                "signal_id": item["signal_id"],
                "ticker": item["ticker"],
                "signal_date": item["signal_date"],
                "field": item["field"],
                "signal_value": f"{item['signal_value']:.4f}",
                "actual_close": f"{item['actual_close']:.4f}",
                "ratio": f"{item['ratio']:.3f}",
                "suggestion": item["suggestion"],
                "signal_type": item["signal_type"],
                "email_date": item["email_date"],
                "email_subject": item["email_subject"],
                "raw_text": item["raw_text"],
            })

    # --- 2. Group by email ---
    by_email = defaultdict(list)
    for item in items:
        by_email[item["email_id"]].append(item)

    unique_emails = len(by_email)

    # Create per-email text files
    emails_dir = os.path.join(OUTPUT_DIR, "emails_to_review")
    os.makedirs(emails_dir, exist_ok=True)

    for email_id, email_items in sorted(by_email.items()):
        first = email_items[0]
        safe_date = (first["email_date"] or "unknown").replace("/", "-")
        safe_subj = "".join(c for c in first["email_subject"][:60]
                           if c.isalnum() or c in " -_#").strip()
        filename = f"{safe_date}_{safe_subj}.txt"

        filepath = os.path.join(emails_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"EMAIL ID: {email_id}\n")
            f.write(f"DATE:     {first['email_date']}\n")
            f.write(f"SUBJECT:  {first['email_subject']}\n")
            f.write(f"FLAGGED SIGNALS: {len(email_items)}\n")
            f.write("=" * 80 + "\n\n")

            for item in email_items:
                f.write(f"  Signal #{item['signal_id']}  [{item['severity']}]\n")
                f.write(f"  Ticker:       {item['ticker']}\n")
                f.write(f"  Date:         {item['signal_date']}\n")
                f.write(f"  Field:        {item['field']}\n")
                f.write(f"  Signal Value: {item['signal_value']}\n")
                f.write(f"  Actual Close: {item['actual_close']:.4f}\n")
                f.write(f"  Ratio:        {item['ratio']:.3f}x\n")
                f.write(f"  Type:         {item['signal_type']}\n")
                f.write(f"  Suggestion:   {item['suggestion']}\n")
                f.write(f"  Raw Text:     {item['raw_text']}\n")
                f.write("\n" + "-" * 60 + "\n\n")

    # --- 3. Summary ---
    # Count by severity
    manual_count = sum(1 for i in items if i["severity"] == "MANUAL")
    check_count = sum(1 for i in items if i["severity"] == "CHECK")

    # Count by ticker
    by_ticker = defaultdict(lambda: {"MANUAL": 0, "CHECK": 0})
    for item in items:
        by_ticker[item["ticker"]][item["severity"]] += 1

    # Identify common patterns
    patterns = defaultdict(list)
    for item in items:
        # Detect pattern
        raw = item["raw_text"].lower()
        if any(etf in raw for etf in ["dia ", "qqq ", "fxe ", "weat ", "soyb ", "cut ", "gld "]):
            patterns["ETF price attributed to futures/index"].append(item)
        elif item["ratio"] > 50:
            patterns["Value way too large (parser comma/decimal issue)"].append(item)
        elif item["ratio"] < 0.02:
            patterns["Value way too small (ETF or missing digits)"].append(item)
        else:
            patterns["Other deviation"].append(item)

    summary_path = os.path.join(OUTPUT_DIR, "review_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("MANUAL REVIEW PACKAGE — SIGNAL PRICE AUDIT\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Total flagged items:  {len(items)}\n")
        f.write(f"  MANUAL:             {manual_count}\n")
        f.write(f"  CHECK (borderline): {check_count}\n")
        f.write(f"\nUnique emails:        {unique_emails}\n")
        f.write(f"(These {len(items)} items come from {unique_emails} distinct Nenner emails)\n\n")

        f.write("-" * 80 + "\n")
        f.write("PER-TICKER BREAKDOWN\n")
        f.write("-" * 80 + "\n")
        for ticker in sorted(by_ticker.keys()):
            counts = by_ticker[ticker]
            total = counts["MANUAL"] + counts["CHECK"]
            parts = []
            if counts["MANUAL"]:
                parts.append(f"{counts['MANUAL']} manual")
            if counts["CHECK"]:
                parts.append(f"{counts['CHECK']} check")
            f.write(f"  {ticker:12s}:  {total:3d} items  ({', '.join(parts)})\n")

        f.write("\n" + "-" * 80 + "\n")
        f.write("COMMON PATTERNS (what to look for)\n")
        f.write("-" * 80 + "\n\n")

        for pattern, pitems in sorted(patterns.items(), key=lambda x: -len(x[1])):
            f.write(f"  {pattern}: {len(pitems)} items\n")
            # Show a few examples
            for ex in pitems[:3]:
                f.write(f"    e.g. #{ex['signal_id']} {ex['ticker']} {ex['field']}: "
                        f"signal={ex['signal_value']}, close={ex['actual_close']:.2f}, "
                        f"ratio={ex['ratio']:.3f}x\n")
                f.write(f"         \"{ex['raw_text'][:100]}\"\n")
            if len(pitems) > 3:
                f.write(f"    ... and {len(pitems) - 3} more\n")
            f.write("\n")

        f.write("-" * 80 + "\n")
        f.write("HOW TO REVIEW\n")
        f.write("-" * 80 + "\n\n")
        f.write("1. Open manual_review_items.csv in Excel — sort/filter by ticker or severity\n")
        f.write("2. For each item, look at the 'raw_text' column to see what the parser extracted\n")
        f.write("3. Common issues to look for:\n")
        f.write("   - DIA/QQQ/WEAT/SOYB ETF prices attributed to YM/NQ/ZW/ZS futures\n")
        f.write("   - Comma-parsing errors (e.g., '121,420' parsed as 121420 for NQ)\n")
        f.write("   - Silver values attributed to Gold (GC), or vice versa\n")
        f.write("   - CUT (lumber ETF) prices attributed to LBS (lumber futures)\n")
        f.write("   - Bond notation issues (ZB: 145.25 stored as 1145.25 or 17508)\n")
        f.write("4. The 'emails_to_review' folder has one file per email for context\n")
        f.write("5. Look up the original email by date + subject to see the full Nenner text\n")

    # --- 4. Create ZIP ---
    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zf:
        # Add CSV
        zf.write(csv_path, "manual_review_items.csv")
        # Add summary
        zf.write(summary_path, "review_summary.txt")
        # Add email files
        for fname in sorted(os.listdir(emails_dir)):
            fpath = os.path.join(emails_dir, fname)
            zf.write(fpath, f"emails_to_review/{fname}")

    print(f"\n{'='*60}")
    print(f"REVIEW PACKAGE CREATED")
    print(f"{'='*60}")
    print(f"  Total items:    {len(items)} ({manual_count} MANUAL + {check_count} CHECK)")
    print(f"  Unique emails:  {unique_emails}")
    print(f"  ZIP file:       {ZIP_PATH}")
    print(f"  Contents:")
    print(f"    - manual_review_items.csv  ({len(items)} rows)")
    print(f"    - review_summary.txt")
    print(f"    - emails_to_review/        ({unique_emails} email files)")
    print(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    main()
