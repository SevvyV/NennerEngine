"""
Nenner Signal Engine
=====================
Vartanian Capital Management, LLC

Automated parsing and tracking of Charles Nenner cycle research signals.

Usage:
    python -m nenner_engine --status
    python -m nenner_engine --backfill
    python -m nenner_engine --history Gold
"""

# Public API re-exports for programmatic access
from .instruments import (
    INSTRUMENT_MAP,
    identify_instrument,
    get_section_instrument,
)
from .parser import (
    RE_ACTIVE,
    RE_CANCELLED,
    RE_TRIGGER,
    RE_TARGET,
    RE_CYCLE,
    RE_NOTE_CHANGE,
    parse_price,
    parse_email_signals,
    classify_email,
    extract_text_from_email,
)
from .db import (
    init_db,
    migrate_db,
    compute_current_state,
    store_email,
    store_parsed_results,
)
from .imap_client import (
    get_credentials,
    process_email,
    backfill_imap,
    check_new_emails,
    import_eml_folder,
)
from .reporting import (
    show_status,
    show_history,
    export_csv,
)

__all__ = [
    # Instruments
    "INSTRUMENT_MAP", "identify_instrument", "get_section_instrument",
    # Parser
    "RE_ACTIVE", "RE_CANCELLED", "RE_TRIGGER", "RE_TARGET", "RE_CYCLE",
    "RE_NOTE_CHANGE", "parse_price", "parse_email_signals", "classify_email",
    "extract_text_from_email",
    # Database
    "init_db", "migrate_db", "compute_current_state",
    "store_email", "store_parsed_results",
    # IMAP
    "get_credentials", "process_email", "backfill_imap",
    "check_new_emails", "import_eml_folder",
    # Reporting
    "show_status", "show_history", "export_csv",
]
