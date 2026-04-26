"""
IMAP Client & Processing Pipeline
===================================
Handles Gmail IMAP connection, email fetching, and the processing
pipeline that ties parsing to storage.
"""

import imaplib
import os
import logging
from email import policy
from email.parser import BytesParser
from datetime import datetime, timedelta
from pathlib import Path

from .parser import classify_email, extract_text_from_email
from .llm_parser import parse_email_signals_llm
from .db import store_email, store_parsed_results, compute_current_state
from .anomaly_check import check_signal_anomalies, alert_anomalies

from .config import NENNER_SENDER, IMAP_SERVER, IMAP_TIMEOUT, load_env_once

log = logging.getLogger(__name__)

# Email types that are expected to contain at least one signal. If one of
# these parses to an empty signal list, treat it as an LLM failure rather
# than as the email genuinely having no signals — re-parse once, then fall
# back to rolling the email row back so the next IMAP poll can retry.
_SIGNAL_BEARING_TYPES = frozenset({
    "stocks_update",
    "sunday_cycles",
    "morning_update",
    "weekly_overview",
})


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def get_credentials() -> tuple[str, str]:
    """Get Gmail credentials from env vars, .env file, or Azure Key Vault."""

    load_env_once()
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
            pass_secret = os.environ.get("GMAIL_PASSWORD_SECRET", "GmailAppPassword")
            gmail_addr = client.get_secret(addr_secret).value.strip().replace("\xa0", "")
            gmail_pass = client.get_secret(pass_secret).value.strip().replace("\xa0", "")
            return gmail_addr, gmail_pass
        except Exception as e:
            log.error(f"Azure Key Vault error: {e}")

    raise ValueError(
        "No credentials found. Set GMAIL_ADDRESS and GMAIL_APP_PASSWORD "
        "in environment variables or a .env file, or configure Azure Key Vault."
    )


# ---------------------------------------------------------------------------
# IMAP Connection
# ---------------------------------------------------------------------------

def connect_imap(gmail_addr: str, gmail_pass: str) -> imaplib.IMAP4_SSL:
    """Connect to Gmail IMAP with a socket timeout so the scheduler thread
    can't be hung forever by a stalled handshake or login."""
    log.info(f"Connecting to {IMAP_SERVER}...")
    imap = imaplib.IMAP4_SSL(IMAP_SERVER, timeout=IMAP_TIMEOUT)
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

        status, msg_data = imap.fetch(uid, "(BODY.PEEK[])")
        if status == "OK" and msg_data[0] is not None:
            raw = msg_data[0][1]
            msg = BytesParser(policy=policy.default).parsebytes(raw)
            # IMAP search filters by FROM header alone, which is spoofable.
            # Gmail already ran SPF/DKIM/DMARC at delivery and stamped the
            # verdict in Authentication-Results — surface it in logs so a
            # spoofed signal injection would show up as a WARN entry rather
            # than silently parse. Logging-only for now; flip to drop+alert
            # once we've confirmed Nenner's outbound mail always passes.
            if not _email_authenticated(msg):
                log.warning(
                    f"UID {uid.decode()} from {NENNER_SENDER} failed Gmail "
                    f"auth check (spoof candidate) — processing anyway: "
                    f"{msg.get('subject', '?')[:80]}"
                )
            messages.append((uid.decode(), msg))

    log.info(f"Fetched {len(messages)} emails")
    return messages


def _email_authenticated(msg, expected_domain: str = "charlesnenner.com") -> bool:
    """Did Gmail's own SPF/DKIM/DMARC check pass for this message?

    Trust Gmail's Authentication-Results header — it's authoritative for
    mail delivered to this account and avoids pulling in a DKIM library
    + DNS lookup. Returns False if header absent or doesn't show a pass.
    """
    auth = (msg.get("Authentication-Results", "") or "").lower()
    if not auth:
        return False
    if "dmarc=pass" in auth:
        return True
    if "dkim=pass" in auth and expected_domain.lower() in auth:
        return True
    return False


# ---------------------------------------------------------------------------
# Processing Pipeline
# ---------------------------------------------------------------------------

def process_email(conn, msg, source_id: str = None) -> bool:
    """Process a single email message atomically: store the email row,
    parse signals via LLM, persist signals/cycles/targets, and rebuild
    current_state — all as one transaction. If anything between the
    initial store and the final state rebuild fails, the entire
    transaction rolls back so the email row is removed and the next
    IMAP poll re-tries the whole pipeline cleanly.

    Returns True if a new email was successfully stored, False if it
    was a duplicate or got rolled back for any reason.
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

    # Open the atomic transaction. Any return path that does not reach the
    # final conn.commit() rolls back via the except clause, taking the
    # email row with it so message_id dedup will not block a retry.
    if conn.in_transaction:
        log.error("process_email called inside an existing transaction — refusing")
        return False
    conn.execute("BEGIN")
    rolled_back = False
    try:
        email_id = store_email(
            conn, message_id, subject, email_date, email_type, body,
            commit=False,
        )
        if email_id is None:
            conn.commit()
            return False  # Duplicate — nothing to roll back

        # Parse signals via LLM
        results = parse_email_signals_llm(body, email_date, email_id)

        # Sanity check: a signal-bearing email type returning zero signals
        # is almost always an LLM parse failure. Retry once. If still empty,
        # roll back so message_id dedup does not block the next IMAP poll.
        if (email_type in _SIGNAL_BEARING_TYPES
                and len(results.get("signals", [])) == 0):
            log.warning(
                f"{email_type} email returned 0 signals — retrying LLM parse "
                f"(email_id={email_id})"
            )
            results = parse_email_signals_llm(body, email_date, email_id)
            if len(results.get("signals", [])) == 0:
                log.error(
                    f"Retry still returned 0 signals for email {email_id}: "
                    f"{subject[:60]} — rolling back so next poll can re-try"
                )
                conn.rollback()
                rolled_back = True
                try:
                    from nenner_engine.alert_dispatch import get_telegram_config, send_telegram
                    token, chat_id = get_telegram_config()
                    if token and chat_id:
                        send_telegram(
                            f"⚠️ NennerEngine: {email_type} parsed 0 signals "
                            f"after retry. Email not marked read; next IMAP poll "
                            f"will re-try.\nEmail: {subject[:80]}",
                            token, chat_id,
                        )
                except Exception:
                    pass
                return False  # caller must NOT mark \Seen

        # Sanity check: Stocks Cycle Charts emails should always have cycles.
        # If signals are present but cycles are missing, retry once. (No
        # rollback: signals are still useful even without cycles.)
        if (email_type == "stocks_update"
                and len(results.get("signals", [])) > 0
                and len(results.get("cycles", [])) == 0):
            log.warning(
                "Stocks Cycle Charts email returned signals but 0 cycles — "
                "retrying LLM parse"
            )
            results = parse_email_signals_llm(body, email_date, email_id)
            if len(results.get("cycles", [])) == 0:
                log.error(
                    f"Retry still returned 0 cycles for email {email_id}: "
                    f"{subject[:60]}"
                )
                try:
                    from nenner_engine.alert_dispatch import get_telegram_config, send_telegram
                    token, chat_id = get_telegram_config()
                    if token and chat_id:
                        send_telegram(
                            f"⚠️ NennerEngine: Stocks Cycle Charts parsed "
                            f"{len(results['signals'])} signals but 0 cycles. "
                            f"Cycle data is missing — stock report will use stale data.\n"
                            f"Email: {subject[:80]}",
                            token, chat_id,
                        )
                except Exception:
                    pass

        # Anomaly check is read-only (queries history) so safe inside tx.
        anomalies = check_signal_anomalies(conn, results.get("signals", []))
        if anomalies:
            alert_anomalies(anomalies)

        # Store signals/cycles/targets and rebuild current_state, all
        # under our open transaction. compute_current_state notices it is
        # already inside a transaction and skips its own with-block.
        store_parsed_results(
            conn, results, email_id,
            commit=False, rebuild_state=False,
        )
        compute_current_state(conn)

        conn.commit()
    except Exception:
        if not rolled_back:
            try:
                conn.rollback()
            except Exception:
                pass
        raise

    sig_count = len(results["signals"])
    cyc_count = len(results["cycles"])
    tgt_count = len(results["price_targets"])

    log.info(
        f"Parsed: {subject[:60]}  |  "
        f"Signals: {sig_count}  Cycles: {cyc_count}  Targets: {tgt_count}"
    )
    return True


def backfill_imap(conn):
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


def check_new_emails(conn) -> int:
    """Check for new emails since last run (incremental mode).

    Returns the number of genuinely new emails parsed and stored.
    """
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
                # Mark as read only after successful parse+store
                uid_bytes = uid.encode() if isinstance(uid, str) else uid
                imap.store(uid_bytes, '+FLAGS', '\\Seen')
                new_count += 1

        if new_count > 0:
            log.info(f"Found {new_count} new emails")
        else:
            log.info("No new emails")

        return new_count
    finally:
        imap.logout()


def import_eml_folder(conn, folder_path: str):
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
