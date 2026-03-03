"""
Fischer Subscription Service
==============================
Subscriber management, on-demand refresh polling, and scheduled distribution
for the Fischer options scan reports.

Subscribers are stored in ``fischer_subscribers`` with a linked portfolio
(``fischer_portfolios``).  On-demand refreshes are triggered by emails with
subject containing "Refresh" and rate-limited via ``fischer_refresh_log``.
"""

import json
import logging
import sqlite3
from datetime import datetime
from typing import Optional

log = logging.getLogger("nenner")


# ---------------------------------------------------------------------------
# Portfolio CRUD
# ---------------------------------------------------------------------------

def get_portfolio(conn: sqlite3.Connection,
                  portfolio_name: str) -> Optional[dict]:
    """Load a portfolio definition, parsing tickers and share_alloc."""
    row = conn.execute(
        "SELECT * FROM fischer_portfolios WHERE portfolio_name = ?",
        (portfolio_name,)
    ).fetchone()
    if not row:
        return None
    d = dict(row)
    d["tickers"] = tuple(t.strip() for t in d["tickers"].split(",") if t.strip())
    try:
        d["share_alloc"] = json.loads(d["share_alloc"])
    except (json.JSONDecodeError, TypeError):
        d["share_alloc"] = {}
    d["show_conviction"] = bool(d["show_conviction"])
    return d


def add_portfolio(conn: sqlite3.Connection, portfolio_name: str, label: str,
                  tickers: list[str], share_alloc: dict[str, int],
                  show_conviction: bool = False) -> str:
    """Insert a new portfolio.  Returns portfolio_name."""
    conn.execute(
        "INSERT INTO fischer_portfolios "
        "(portfolio_name, label, tickers, share_alloc, show_conviction) "
        "VALUES (?, ?, ?, ?, ?)",
        (portfolio_name, label, ",".join(tickers),
         json.dumps(share_alloc), int(show_conviction))
    )
    conn.commit()
    log.info(f"Portfolio added: {portfolio_name} ({label}) -> {tickers}")
    return portfolio_name


def list_portfolios(conn: sqlite3.Connection) -> list[dict]:
    """List all portfolios."""
    rows = conn.execute("SELECT * FROM fischer_portfolios ORDER BY portfolio_name").fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["tickers"] = tuple(t.strip() for t in d["tickers"].split(",") if t.strip())
        try:
            d["share_alloc"] = json.loads(d["share_alloc"])
        except (json.JSONDecodeError, TypeError):
            d["share_alloc"] = {}
        d["show_conviction"] = bool(d["show_conviction"])
        result.append(d)
    return result


# ---------------------------------------------------------------------------
# Subscriber CRUD
# ---------------------------------------------------------------------------

def get_subscriber_by_email(conn: sqlite3.Connection,
                            email: str) -> Optional[dict]:
    """Look up an active subscriber by email address."""
    row = conn.execute(
        "SELECT * FROM fischer_subscribers WHERE email = ? AND active = 1",
        (email.strip().lower(),)
    ).fetchone()
    return dict(row) if row else None


def get_subscriber_by_last_name(conn: sqlite3.Connection,
                                last_name: str) -> Optional[dict]:
    """Look up an active subscriber by last name (case-insensitive).

    Returns None if not found or if multiple subscribers share the same
    last name (ambiguous).
    """
    rows = conn.execute(
        "SELECT * FROM fischer_subscribers "
        "WHERE LOWER(last_name) = LOWER(?) AND active = 1",
        (last_name.strip(),)
    ).fetchall()
    if len(rows) == 1:
        return dict(rows[0])
    if len(rows) > 1:
        log.warning(f"Ambiguous last_name lookup: {last_name} "
                    f"matched {len(rows)} subscribers")
    return None


def _match_subscriber_by_subject(conn: sqlite3.Connection,
                                  subject: str) -> Optional[dict]:
    """Match a subscriber by finding their last name in the email subject.

    Subject must contain both "Refresh" and a subscriber's last name.
    E.g. "Refresh Maher" matches Chris Maher.
    Returns None if no match or ambiguous.
    """
    subject_lower = subject.lower()
    if "refresh" not in subject_lower:
        return None

    # Get all active subscribers and check if their last name is in the subject
    rows = conn.execute(
        "SELECT * FROM fischer_subscribers WHERE active = 1"
    ).fetchall()

    matches = [dict(r) for r in rows if r["last_name"].lower() in subject_lower]

    if len(matches) == 1:
        log.info(f"Fischer refresh: matched subscriber {matches[0]['last_name']} "
                 f"from subject '{subject}'")
        return matches[0]
    if len(matches) > 1:
        names = [m["last_name"] for m in matches]
        log.warning(f"Fischer refresh: ambiguous subject '{subject}' "
                    f"matched {names}")
    return None


def get_all_active_subscribers(conn: sqlite3.Connection) -> list[dict]:
    """Return all active subscribers joined with their portfolio data."""
    rows = conn.execute(
        "SELECT s.*, p.label AS portfolio_label, p.tickers AS portfolio_tickers, "
        "p.share_alloc AS portfolio_share_alloc, p.show_conviction AS portfolio_show_conviction "
        "FROM fischer_subscribers s "
        "JOIN fischer_portfolios p ON s.portfolio_name = p.portfolio_name "
        "WHERE s.active = 1 "
        "ORDER BY s.last_name, s.first_name"
    ).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["portfolio_tickers"] = tuple(
            t.strip() for t in (d.get("portfolio_tickers") or "").split(",") if t.strip()
        )
        try:
            d["portfolio_share_alloc"] = json.loads(d.get("portfolio_share_alloc") or "{}")
        except (json.JSONDecodeError, TypeError):
            d["portfolio_share_alloc"] = {}
        d["portfolio_show_conviction"] = bool(d.get("portfolio_show_conviction", 0))
        result.append(d)
    return result


def add_subscriber(conn: sqlite3.Connection, email: str, first_name: str,
                   last_name: str, portfolio_name: str = "fischer_daily",
                   max_daily_refreshes: int = 25) -> int:
    """Insert a new subscriber.  Returns subscriber id."""
    cur = conn.execute(
        "INSERT INTO fischer_subscribers "
        "(email, first_name, last_name, portfolio_name, max_daily_refreshes) "
        "VALUES (?, ?, ?, ?, ?)",
        (email.strip().lower(), first_name.strip(), last_name.strip(),
         portfolio_name, max_daily_refreshes)
    )
    conn.commit()
    log.info(f"Subscriber added: {first_name} {last_name} <{email}> "
             f"-> {portfolio_name}")
    return cur.lastrowid


def deactivate_subscriber(conn: sqlite3.Connection, email: str) -> bool:
    """Set active=0 for subscriber.  Returns True if found and updated."""
    cur = conn.execute(
        "UPDATE fischer_subscribers SET active = 0 WHERE email = ?",
        (email.strip().lower(),)
    )
    conn.commit()
    if cur.rowcount > 0:
        log.info(f"Subscriber deactivated: {email}")
        return True
    return False


def reactivate_subscriber(conn: sqlite3.Connection, email: str) -> bool:
    """Set active=1 for subscriber.  Returns True if found and updated."""
    cur = conn.execute(
        "UPDATE fischer_subscribers SET active = 1 WHERE email = ?",
        (email.strip().lower(),)
    )
    conn.commit()
    if cur.rowcount > 0:
        log.info(f"Subscriber reactivated: {email}")
        return True
    return False


def list_subscribers(conn: sqlite3.Connection) -> list[dict]:
    """List all subscribers with portfolio info."""
    rows = conn.execute(
        "SELECT s.*, p.label AS portfolio_label "
        "FROM fischer_subscribers s "
        "LEFT JOIN fischer_portfolios p ON s.portfolio_name = p.portfolio_name "
        "ORDER BY s.active DESC, s.last_name, s.first_name"
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Rate Limiting
# ---------------------------------------------------------------------------

def check_rate_limit(conn: sqlite3.Connection, subscriber_id: int,
                     max_refreshes: int) -> tuple[bool, int]:
    """Check if subscriber has remaining refreshes today.

    Returns (allowed, used_count).
    """
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COUNT(*) FROM fischer_refresh_log "
        "WHERE subscriber_id = ? AND status = 'completed' "
        "AND date(requested_at) = ?",
        (subscriber_id, today_str)
    ).fetchone()
    used = row[0] if row else 0
    return (used < max_refreshes, used)


def _log_refresh(conn: sqlite3.Connection, subscriber_id: int,
                 subject: str, portfolio_name: str, status: str,
                 error_message: Optional[str] = None):
    """Insert a row into fischer_refresh_log."""
    conn.execute(
        "INSERT INTO fischer_refresh_log "
        "(subscriber_id, email_subject, portfolio_name, status, error_message) "
        "VALUES (?, ?, ?, ?, ?)",
        (subscriber_id, subject, portfolio_name, status, error_message)
    )
    conn.commit()


# ---------------------------------------------------------------------------
# IMAP Refresh Polling
# ---------------------------------------------------------------------------

def poll_refresh_requests(db_path: str) -> int:
    """Poll Gmail IMAP for unseen 'Refresh' emails.

    If the FischerReliability singleton is active, delegates to the
    ResilientIMAPPoller (exponential backoff + admin alerts).
    Otherwise falls through to the raw polling implementation.

    Returns number of refreshes successfully processed.
    """
    try:
        from .fischer_reliability import FischerReliability
        rel = FischerReliability.get_instance()
        if rel and rel.imap_poller:
            return rel.imap_poller.poll(db_path)
    except ImportError:
        pass
    return _poll_refresh_raw(db_path)


def _poll_refresh_raw(db_path: str) -> int:
    """Raw IMAP polling — no resilience wrapper.

    For each matched email:
    1. Verify sender is an active subscriber
    2. Check rate limit
    3. Generate fresh scan with live prices
    4. Email report to subscriber
    5. Log in refresh_log and mark email as SEEN

    Returns number of refreshes successfully processed.
    """
    import imaplib
    from email import policy
    from email.parser import BytesParser
    from email.utils import parseaddr

    from .db import init_db
    from .imap_client import get_credentials, IMAP_SERVER

    gmail_addr, gmail_pass = get_credentials()

    try:
        imap = imaplib.IMAP4_SSL(IMAP_SERVER)
        imap.login(gmail_addr, gmail_pass)
    except Exception as e:
        log.error(f"Fischer refresh: IMAP connect failed: {e}")
        raise  # Let resilient poller handle retries

    processed = 0

    try:
        imap.select("INBOX")
        status, data = imap.search(None, '(UNSEEN SUBJECT "Refresh")')
        if status != "OK" or not data[0]:
            return 0

        msg_ids = data[0].split()
        log.info(f"Fischer refresh: found {len(msg_ids)} unseen 'Refresh' email(s)")

        conn = init_db(db_path)
        try:
            for msg_id in msg_ids:
                try:
                    processed += _process_refresh_email(
                        imap, conn, msg_id, db_path
                    )
                except Exception as e:
                    log.error(f"Fischer refresh: error processing msg {msg_id}: {e}",
                              exc_info=True)
                finally:
                    # Always mark as SEEN to avoid reprocessing
                    imap.store(msg_id, "+FLAGS", "\\Seen")
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Fischer refresh: polling error: {e}", exc_info=True)
    finally:
        try:
            imap.logout()
        except Exception:
            pass

    if processed > 0:
        log.info(f"Fischer refresh: processed {processed} request(s)")
    return processed


def _process_refresh_email(imap, conn: sqlite3.Connection,
                           msg_id: bytes, db_path: str) -> int:
    """Process a single refresh request email.  Returns 1 on success, 0 on skip."""
    from email import policy
    from email.parser import BytesParser
    from email.utils import parseaddr

    # Fetch the email
    status, msg_data = imap.fetch(msg_id, "(RFC822)")
    if status != "OK":
        log.warning(f"Fischer refresh: failed to fetch msg {msg_id}")
        return 0

    raw_email = msg_data[0][1]
    msg = BytesParser(policy=policy.default).parsebytes(raw_email)

    # Extract sender and subject
    sender_name, sender_email = parseaddr(msg.get("From", ""))
    sender_email = sender_email.strip().lower()
    subject = msg.get("Subject", "").strip()

    log.info(f"Fischer refresh: processing from={sender_email} subject='{subject}'")

    # Telegram notification — fire for every Refresh email received
    try:
        from .alerts import notify_fischer_refresh
        notify_fischer_refresh(sender_email)
    except Exception as e:
        log.debug(f"Fischer refresh Telegram notify failed: {e}")

    # 1. Look up subscriber — match by last name in subject
    #    Subject must contain "Refresh" AND the subscriber's last name.
    #    This lets any sender (including the admin) trigger a refresh for a specific subscriber.
    subscriber = _match_subscriber_by_subject(conn, subject)
    if not subscriber:
        # Fallback: try matching by sender email (backward compat)
        subscriber = get_subscriber_by_email(conn, sender_email)
    if not subscriber:
        log.info(f"Fischer refresh: no subscriber matched for "
                 f"subject='{subject}' sender={sender_email}, skipping")
        return 0

    # 2. Use sender's own portfolio
    portfolio_name = subscriber["portfolio_name"]

    # 3. Check rate limit
    allowed, used = check_rate_limit(
        conn, subscriber["id"], subscriber["max_daily_refreshes"]
    )
    if not allowed:
        log.info(f"Fischer refresh: {sender_email} rate-limited "
                 f"({used}/{subscriber['max_daily_refreshes']} today)")
        _log_refresh(conn, subscriber["id"], subject, portfolio_name, "rate_limited")
        return 0

    # 3b. Market hours check (if reliability layer active)
    try:
        from .fischer_reliability import FischerReliability
        rel = FischerReliability.get_instance()
        if rel and rel.market_hours:
            check = rel.market_hours.check_request()
            if not check["allowed"]:
                defer_str = (check["defer_until"].strftime("%I:%M %p ET %a")
                             if check["defer_until"] else "next open")
                log.info(f"Fischer refresh: deferred for {sender_email} — "
                         f"{check['reason']} (next open: {defer_str})")
                _log_refresh(conn, subscriber["id"], subject, portfolio_name,
                             "deferred", f"Market closed — deferred to {defer_str}")
                # Send courtesy reply
                try:
                    from .postmaster import send_email, wrap_document
                    body = wrap_document(
                        f'<p style="font-size:14px;">Your Fischer scan request has been '
                        f'received but the market is currently closed.</p>'
                        f'<p style="font-size:14px;">Your scan will be processed at '
                        f'<strong>{defer_str}</strong>.</p>',
                        title="Fischer Options",
                        subtitle="Request Deferred",
                    )
                    send_email(f"Fischer — Request Deferred to {defer_str}",
                               body, to_addr=sender_email)
                except Exception as e:
                    log.error(f"Fischer refresh: deferred reply failed: {e}")
                return 0
    except ImportError:
        pass

    # 4. Load portfolio
    portfolio = get_portfolio(conn, portfolio_name)
    if not portfolio:
        log.error(f"Fischer refresh: portfolio '{portfolio_name}' not found")
        _log_refresh(conn, subscriber["id"], subject, portfolio_name, "error",
                     f"Portfolio '{portfolio_name}' not found")
        return 0

    # 5. Generate fresh scan and send
    try:
        _generate_and_send_refresh(subscriber, portfolio, subject)
        _log_refresh(conn, subscriber["id"], subject, portfolio_name, "completed")
        return 1
    except Exception as e:
        log.error(f"Fischer refresh: scan/send failed for {sender_email}: {e}",
                  exc_info=True)
        _log_refresh(conn, subscriber["id"], subject, portfolio_name, "error", str(e))
        return 0


def _generate_and_send_refresh(subscriber: dict, portfolio: dict,
                               original_subject: str):
    """Generate a fresh Fischer scan and email it to the subscriber.

    Uses the same _build_unified_email format as the admin Daily Scan.
    """
    from datetime import date as _date
    from .fischer_daily_report import (
        generate_fresh_scan, _build_unified_email, REPORT_SECTIONS,
    )
    from .postmaster import send_email

    # Send dedup check (if reliability layer active)
    try:
        from .fischer_reliability import FischerReliability
        rel = FischerReliability.get_instance()
        if rel and rel.dedup:
            job_id = rel.dedup.make_job_id(subscriber["email"])
            if not rel.dedup.check_and_mark(
                subscriber["email"], "refresh", job_id
            ):
                log.info(f"Fischer refresh: duplicate blocked for {subscriber['email']}")
                return
    except ImportError:
        pass

    tickers = portfolio["tickers"]
    share_alloc = portfolio["share_alloc"]
    label = portfolio["label"]

    log.info(f"Fischer refresh: generating live scan for {subscriber['email']} "
             f"({label}: {tickers})")

    put_recs, call_recs, put_weekly, call_weekly, failed = generate_fresh_scan(
        tickers=tickers,
        share_alloc=share_alloc,
    )

    # Map fresh scan results into sections_data keyed by ReportSection
    # REPORT_SECTIONS[0] = covered puts 0-7 DTE, [1] = covered puts 8-14 DTE
    sections_data = {}
    if put_recs and len(REPORT_SECTIONS) > 0:
        sections_data[REPORT_SECTIONS[0]] = put_recs
    if put_weekly and len(REPORT_SECTIONS) > 1:
        sections_data[REPORT_SECTIONS[1]] = put_weekly

    if not sections_data:
        log.info(f"Fischer refresh: no results for {subscriber['email']}, skipping")
        return

    now_str = datetime.now().strftime("%I:%M %p")
    slot_label = f"Refresh \u2014 {now_str}"

    html = _build_unified_email(sections_data, failed, slot_label)

    today_fmt = _date.today().strftime('%b %d')
    email_subject = f"Fischer Daily Scan \u2014 {slot_label} \u2014 {today_fmt}"
    send_email(email_subject, html, to_addr=subscriber["email"])
    log.info(f"Fischer refresh: sent to {subscriber['email']}")


# ---------------------------------------------------------------------------
# Manual Refresh (CLI)
# ---------------------------------------------------------------------------

def manual_refresh(conn: sqlite3.Connection, email: str,
                   db_path: str = "") -> bool:
    """Manually trigger a Fischer refresh for a subscriber by email."""
    subscriber = get_subscriber_by_email(conn, email)
    if not subscriber:
        log.error(f"Manual refresh: subscriber '{email}' not found or inactive")
        return False

    portfolio = get_portfolio(conn, subscriber["portfolio_name"])
    if not portfolio:
        log.error(f"Manual refresh: portfolio '{subscriber['portfolio_name']}' not found")
        return False

    try:
        _generate_and_send_refresh(subscriber, portfolio, "Manual Refresh")
        _log_refresh(conn, subscriber["id"], "Manual Refresh",
                     subscriber["portfolio_name"], "completed")
        return True
    except Exception as e:
        log.error(f"Manual refresh failed: {e}", exc_info=True)
        _log_refresh(conn, subscriber["id"], "Manual Refresh",
                     subscriber["portfolio_name"], "error", str(e))
        return False
