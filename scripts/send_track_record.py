"""Send the track record analysis report to a specific email address."""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from email_report import markdown_to_html
from nenner_engine.stock_report import send_email

REPORT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "track_record_report.md")
TO_ADDR = "sevagv@vartaniancapital.com"
SUBJECT = "Nenner Signal Track Record Analysis - Full History (Nov 2018 - Feb 2026)"


def main():
    with open(REPORT_PATH, "r", encoding="utf-8") as f:
        md_text = f.read()

    html_body = markdown_to_html(md_text)

    print(f"Sending track record report to {TO_ADDR}...")
    ok = send_email(SUBJECT, html_body, to_addr=TO_ADDR)

    if ok:
        print(f"Report emailed successfully to {TO_ADDR}")
    else:
        print("ERROR: Email send failed. Check credentials and network.")
        sys.exit(1)


if __name__ == "__main__":
    main()
