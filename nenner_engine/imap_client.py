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

from .parser import classify_email, extract_text_from_email, parse_email_signals
from .db import store_email, store_parsed_results

log = logging.getLogger("nenner")

NENNER_SENDER = "newsletter@charlesnenner.com"
IMAP_SERVER = "imap.gmail.com"


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def get_credentials() -> tuple[str, str]:
    """Get Gmail credentials from env vars, .env file, or Azure Key Vault."""

    # Try .env file first (look in project root, not package dir)
    for search_dir in [os.getcwd(), os.path.dirname(os.path.dirname(os.path.abspath(__file__)))]:
        env_path = os.path.join(search_dir, ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        key, val = line.split("=", 1)
                        os.environ.setdefault(key.strip(), val.strip())
            break

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
# IMAP Connection
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

def process_email(conn, msg, source_id: str = None) -> bool:
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


def check_new_emails(conn):
    """Check for new emails since last run (incremental mode)."""
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
