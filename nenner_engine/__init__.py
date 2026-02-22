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
from .prices import (
    YFINANCE_MAP,
    LSEG_RIC_MAP,
    fetch_yfinance_daily,
    backfill_yfinance,
    read_t1_prices,
    store_t1_prices,
    setup_t1_sheet,
    get_current_prices,
    get_prices_with_signal_context,
    store_prices,
    get_cached_prices,
)

from .positions import (
    read_positions,
    parse_option_code,
    compute_position_pnl,
    get_positions_with_signal_context,
    get_held_tickers,
)

from .alerts import (
    AlertConfig,
    evaluate_price_alerts,
    detect_signal_changes,
    run_monitor,
    show_alert_history,
    send_toast,
    send_telegram,
    dispatch_alert,
    is_intraday_ticker,
    build_scheduled_summary,
    PROXIMITY_DANGER_PCT,
    PROXIMITY_WARNING_PCT,
    ALERT_COOLDOWN_MINUTES,
)
from .llm_parser import (
    parse_email_signals_llm,
    get_anthropic_api_key,
)
from .auto_cancel import (
    check_auto_cancellations,
)

from .email_scheduler import (
    EmailScheduler,
    run_email_check,
)
from .trade_stats import (
    extract_trades_from_db,
    compute_instrument_stats,
    get_profit_factor,
    build_top_trades_message,
)

__all__ = [
    # Instruments
    "INSTRUMENT_MAP", "identify_instrument", "get_section_instrument",
    # Parser (legacy regex — kept for reference)
    "RE_ACTIVE", "RE_CANCELLED", "RE_TRIGGER", "RE_TARGET", "RE_CYCLE",
    "RE_NOTE_CHANGE", "parse_price", "parse_email_signals", "classify_email",
    "extract_text_from_email",
    # LLM Parser (primary)
    "parse_email_signals_llm", "get_anthropic_api_key",
    # Auto-Cancel
    "check_auto_cancellations",
    # Database
    "init_db", "migrate_db", "compute_current_state",
    "store_email", "store_parsed_results",
    # IMAP
    "get_credentials", "process_email", "backfill_imap",
    "check_new_emails", "import_eml_folder",
    # Reporting
    "show_status", "show_history", "export_csv",
    # Prices
    "YFINANCE_MAP", "LSEG_RIC_MAP",
    "fetch_yfinance_daily", "backfill_yfinance",
    "read_t1_prices", "store_t1_prices", "setup_t1_sheet",
    "get_current_prices", "get_prices_with_signal_context",
    "store_prices", "get_cached_prices",
    # Positions
    "read_positions", "parse_option_code", "compute_position_pnl",
    "get_positions_with_signal_context", "get_held_tickers",
    # Alerts
    "AlertConfig",
    "evaluate_price_alerts", "detect_signal_changes",
    "run_monitor", "show_alert_history",
    "send_toast", "send_telegram", "dispatch_alert",
    "is_intraday_ticker", "build_scheduled_summary",
    "PROXIMITY_DANGER_PCT", "PROXIMITY_WARNING_PCT", "ALERT_COOLDOWN_MINUTES",
    # Email Scheduler
    "EmailScheduler", "run_email_check",
    # Trade Stats
    "extract_trades_from_db", "compute_instrument_stats",
    "get_profit_factor", "build_top_trades_message",
]
