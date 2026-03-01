"""
Email Report Utility
====================
Send a markdown report as a styled HTML email via
Postmaster (nenner_engine.postmaster).

Usage:
    python email_report.py "Subject Line" /path/to/report.md
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))


def markdown_to_html(md_text: str) -> str:
    """Convert markdown report to styled HTML email body.

    Delegates to nenner_engine.postmaster.markdown_to_html() which owns
    the canonical template, colors, and document shell.
    """
    from nenner_engine.postmaster import markdown_to_html as _md_to_html
    return _md_to_html(md_text)


def main():
    if len(sys.argv) < 3:
        print("Usage: python email_report.py \"Subject\" /path/to/report.md")
        sys.exit(1)

    subject = sys.argv[1]
    md_path = sys.argv[2]

    if not os.path.isfile(md_path):
        print(f"Error: File not found: {md_path}")
        sys.exit(1)

    with open(md_path, "r", encoding="utf-8") as f:
        md_text = f.read()

    html_body = markdown_to_html(md_text)

    from nenner_engine.postmaster import send_email
    ok = send_email(subject, html_body)

    if ok:
        print(f"Report emailed successfully: \"{subject}\"")
    else:
        print("Error: Email send failed. Check credentials and network.")
        sys.exit(1)


if __name__ == "__main__":
    main()
