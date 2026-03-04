"""
Re-parse emails in the database using the LLM parser.

Supports resuming, batching, and parallel processing:
  --fresh            Wipe all parsed data and start from email #1
  --resume           Pick up where the last run stopped (default)
  --batch N          Process N emails then stop (default: all remaining)
  --parallel N       Process N emails concurrently (default: 3)
  --newest-first     Process emails in reverse chronological order
  --model MODEL      Override the default LLM model

Examples:
  python reparse_with_llm.py --fresh --batch 100 --newest-first  # Test 100 most recent
  python reparse_with_llm.py --resume --parallel 3               # Continue, finish all
  python reparse_with_llm.py --fresh --batch 500 --parallel 3    # Fresh start, first 500
"""
import argparse
import os
import sys
import time
import sqlite3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure we're in the project directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Load .env
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())

from nenner_engine.db import init_db, migrate_db, compute_current_state
from nenner_engine.llm_parser import parse_email_signals_llm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("reparse_llm.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
# Suppress noisy httpx request logging
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("reparse")

DB_PATH = "nenner_signals.db"


def get_already_parsed_email_ids(conn):
    """Return set of email_ids that already have parsed data."""
    rows = conn.execute("SELECT DISTINCT email_id FROM signals").fetchall()
    parsed = {r["email_id"] for r in rows}
    rows = conn.execute("SELECT DISTINCT email_id FROM cycles").fetchall()
    parsed.update(r["email_id"] for r in rows)
    rows = conn.execute("SELECT DISTINCT email_id FROM price_targets").fetchall()
    parsed.update(r["email_id"] for r in rows)
    # Also count emails explicitly marked as parsed (signal_count >= 0)
    rows = conn.execute("SELECT id FROM emails WHERE signal_count IS NOT NULL AND signal_count >= 0").fetchall()
    parsed.update(r["id"] for r in rows)
    return parsed


def parse_one_email(email, model=None):
    """Parse a single email (thread-safe — no DB writes here)."""
    email_id = email["id"]
    date_sent = email["date_sent"]
    body = email["raw_text"]

    if not body or len(body) < 50:
        return {"email_id": email_id, "skip": True, "results": None}

    try:
        kwargs = {}
        if model:
            kwargs["model"] = model
        results = parse_email_signals_llm(body, date_sent, email_id, **kwargs)
        return {"email_id": email_id, "skip": False, "results": results, "error": None}
    except Exception as e:
        return {"email_id": email_id, "skip": False, "results": None, "error": str(e)}


def store_results(conn, email_id, results):
    """Store parsed results into the database (single-threaded)."""
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
             sig.get("price_target"), sig.get("target_direction"),
             sig["note_the_change"], sig["uses_hourly_close"], sig["raw_text"])
        )
    for cyc in results["cycles"]:
        conn.execute(
            "INSERT INTO cycles (email_id, date, instrument, ticker, timeframe, "
            "direction, until_description, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (cyc["email_id"], cyc["date"], cyc["instrument"], cyc["ticker"],
             cyc["timeframe"], cyc["direction"], cyc["until_description"],
             cyc["raw_text"])
        )
    for tgt in results["price_targets"]:
        conn.execute(
            "INSERT INTO price_targets (email_id, date, instrument, ticker, "
            "target_price, direction, condition, raw_text) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (tgt["email_id"], tgt["date"], tgt["instrument"], tgt["ticker"],
             tgt["target_price"], tgt["direction"], tgt["condition"],
             tgt["raw_text"])
        )

    sig_count = len(results["signals"])
    cyc_count = len(results["cycles"])
    tgt_count = len(results["price_targets"])
    total_parsed = sig_count + cyc_count + tgt_count
    conn.execute("UPDATE emails SET signal_count = ? WHERE id = ?",
                 (total_parsed, email_id))
    conn.commit()
    return sig_count, cyc_count, tgt_count


def main():
    parser = argparse.ArgumentParser(description="Re-parse emails with LLM")
    parser.add_argument("--fresh", action="store_true",
                        help="Wipe all parsed data and start from scratch")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from where the last run stopped (default)")
    parser.add_argument("--batch", type=int, default=0,
                        help="Process N emails then stop (0 = all remaining)")
    parser.add_argument("--parallel", type=int, default=3,
                        help="Number of concurrent API calls (default: 3)")
    parser.add_argument("--newest-first", action="store_true",
                        help="Process emails newest-first (default: oldest-first)")
    parser.add_argument("--model", type=str, default=None,
                        help="Override LLM model (e.g., claude-sonnet-4-5-20250929)")
    args = parser.parse_args()

    # --fresh overrides --resume
    if args.fresh:
        args.resume = False

    conn = init_db(DB_PATH)
    migrate_db(conn)

    # Get all emails ordered by date
    order = "DESC" if args.newest_first else "ASC"
    all_emails = conn.execute(f"""
        SELECT id, subject, date_sent, email_type, raw_text
        FROM emails
        ORDER BY date_sent {order}, id {order}
    """).fetchall()

    total_emails = len(all_emails)

    if args.fresh:
        log.info(f"FRESH START: Clearing all parsed data...")
        conn.execute("DELETE FROM current_state")
        conn.execute("DELETE FROM price_targets")
        conn.execute("DELETE FROM cycles")
        conn.execute("DELETE FROM signals")
        conn.execute("UPDATE emails SET signal_count = NULL")
        conn.commit()
        emails_to_process = all_emails
        already_done = 0
    else:
        # Resume: skip emails that already have parsed data
        parsed_ids = get_already_parsed_email_ids(conn)
        already_done = len(parsed_ids)
        emails_to_process = [e for e in all_emails if e["id"] not in parsed_ids]
        log.info(f"RESUME: {already_done} emails already parsed, {len(emails_to_process)} remaining")

    # Apply batch limit
    if args.batch > 0:
        emails_to_process = emails_to_process[:args.batch]
        log.info(f"BATCH: Processing {len(emails_to_process)} emails this run")

    if not emails_to_process:
        log.info("Nothing to process! All emails already parsed.")
        log.info("Final current_state rebuild...")
        compute_current_state(conn)
        conn.close()
        return

    from nenner_engine.llm_parser import DEFAULT_MODEL
    effective_model = args.model or DEFAULT_MODEL
    log.info(f"Starting LLM parse of {len(emails_to_process)} emails "
             f"({already_done} already done, {total_emails} total) "
             f"with {args.parallel} parallel workers, model={effective_model}"
             f"{', newest-first' if args.newest_first else ''}...")

    success = 0
    errors = 0
    batch_signals = 0
    batch_cycles = 0
    batch_targets = 0
    start_time = time.time()
    processed = 0

    # Process emails in parallel, but write to DB sequentially
    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        # Submit all emails to the thread pool
        future_to_email = {}
        for email in emails_to_process:
            future = executor.submit(parse_one_email, email, model=args.model)
            future_to_email[future] = email

        # Collect results as they complete
        for future in as_completed(future_to_email):
            email = future_to_email[future]
            processed += 1

            try:
                result = future.result()

                if result["skip"]:
                    # Empty email, mark as parsed
                    conn.execute("UPDATE emails SET signal_count = 0 WHERE id = ?",
                                 (result["email_id"],))
                    conn.commit()
                    continue

                if result["error"]:
                    errors += 1
                    log.error(f"[{already_done + processed}/{total_emails}] "
                              f"ERROR on {email['subject'][:60]}: {result['error']}")
                    continue

                # Store results in DB (single-threaded)
                sig_count, cyc_count, tgt_count = store_results(
                    conn, result["email_id"], result["results"])
                batch_signals += sig_count
                batch_cycles += cyc_count
                batch_targets += tgt_count
                success += 1

            except Exception as e:
                errors += 1
                log.error(f"[{already_done + processed}/{total_emails}] "
                          f"UNEXPECTED ERROR on {email['subject'][:60]}: {e}")

            # Progress logging every 50 emails
            if processed % 50 == 0 or processed == len(emails_to_process):
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = len(emails_to_process) - processed
                eta = remaining / rate if rate > 0 else 0
                log.info(
                    f"[{already_done + processed}/{total_emails}] "
                    f"Signals:{batch_signals} Cycles:{batch_cycles} Targets:{batch_targets} | "
                    f"Rate:{rate:.1f}/s ETA:{eta:.0f}s ({eta/60:.1f}min) | "
                    f"Errors:{errors}"
                )

    # Rebuild current_state after batch completes
    log.info("Rebuilding current_state...")
    compute_current_state(conn)

    elapsed = time.time() - start_time
    log.info(f"\n{'='*60}")
    log.info(f"BATCH COMPLETE")
    log.info(f"{'='*60}")
    log.info(f"This batch:       {success}/{len(emails_to_process)} ({errors} errors)")
    log.info(f"Overall progress: {already_done + success}/{total_emails}")
    log.info(f"Batch signals:    {batch_signals}")
    log.info(f"Batch cycles:     {batch_cycles}")
    log.info(f"Batch targets:    {batch_targets}")
    log.info(f"Time elapsed:     {elapsed:.0f}s ({elapsed/60:.1f}min)")
    remaining = total_emails - (already_done + success)
    if remaining > 0:
        log.info(f"Remaining:        {remaining} emails")
        log.info(f"Resume with:      python reparse_with_llm.py --resume")
    else:
        log.info(f"ALL EMAILS PARSED!")
    log.info(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    main()
