"""Characterization tests for stock_report.

Pins the byte-for-byte output of build_stock_report_html and
build_report_subject against a golden file, so a refactor (e.g. the
monolith → package split in Phase 3.3) cannot silently change the
rendered HTML.

On first run (golden file missing) the test writes the file and skips,
so the workflow is: run once to generate, commit the golden, then the
test enforces equivalence on every subsequent run.
"""

import hashlib
import importlib
import sys
from datetime import datetime as real_datetime
from pathlib import Path

import pytest


FIXTURE_DIR = Path(__file__).parent / "fixtures"
GOLDEN_HTML = FIXTURE_DIR / "stock_report_golden.html"
GOLDEN_SUBJECT = FIXTURE_DIR / "stock_report_subject.txt"

# Frozen clock for header/footer/subject — picked to be unambiguous in any TZ.
FROZEN_NOW = real_datetime(2026, 4, 22, 8, 30, 0)


def _freeze_time(monkeypatch):
    """Freeze datetime.now() in every stock_report module that uses it.

    Stock_report imports datetime via `from datetime import datetime`, so
    monkeypatching the symbol on the module replaces the binding the
    rendering code uses. Patches both the monolith path and the split
    package paths so this test survives the refactor.
    """

    class FrozenDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return FROZEN_NOW

    # All the places `datetime` may be imported into.
    # postmaster.wrap_document() adds its own timestamped footer, so freeze
    # there too — without it the wrapper's clock leaks through.
    candidate_modules = [
        "nenner_engine.stock_report",          # monolith + package __init__
        "nenner_engine.stock_report.html",     # post-split HTML builders
        "nenner_engine.stock_report.data",     # post-split data (uses date.today)
        "nenner_engine.postmaster",            # wrap_document footer timestamp
    ]
    for name in candidate_modules:
        try:
            mod = importlib.import_module(name)
        except ImportError:
            continue
        if hasattr(mod, "datetime"):
            monkeypatch.setattr(f"{name}.datetime", FrozenDateTime)


def _make_fixture_stocks():
    """Return a 3-stock fixture exercising the major code paths.

    Covers: SELL with cancel danger, BUY with target staircase, BUY with
    cycle alignment + trade stats, implied reversal flag, NTC churn,
    trade aging, and a None-target stock. Order matches FOCUS_STOCKS so
    the stable iteration in build_report_subject is deterministic.
    """
    return [
        # AAPL — SELL with active inflection: CANCEL_DANGER + REVERSAL
        {
            "ticker": "AAPL",
            "display_ticker": "AAPL",
            "name": "Apple Inc.",
            "instrument": "Apple",
            "signal": "SELL",
            "origin_price": 230.50,
            "cancel_level": 215.00,
            "cancel_direction": "above",
            "implied_reversal": True,
            "last_signal_date": "2026-04-15",
            "price": 213.40,
            "price_source": "yfinance",
            "price_as_of": "2026-04-22T08:00:00",
            "pnl_pct": 7.42,
            "cancel_dist_pct": 0.75,  # < CANCEL_DANGER_PCT → DANGER flag
            "target_price": 200.00,
            "target_direction": "DOWNSIDE",
            "target_condition": "price target",
            "target_dist_pct": 6.28,
            "reward_risk": 8.6,
            "cancel_trajectory": [225.0, 220.0, 218.0, 217.0, 215.0],
            "ntc_count_30d": 4,  # HIGH_CHURN
            "cycles": [
                {"timeframe": "daily", "direction": "DOWN", "until_description": "early May"},
                {"timeframe": "weekly", "direction": "DOWN", "until_description": "mid May"},
                {"timeframe": "monthly", "direction": "DOWN", "until_description": "Q3 2026"},
            ],
            "cycle_alignment": "ALIGNED",
            "trade_stats": {
                "sharpe": 1.42, "win_rate": 64.0, "kelly": 0.18, "trades": 27,
                "avg_duration": 12, "median_duration": 9,
            },
            "risk_flag": "",
            "signal_history": [],
            "trade_age_days": 7,
            "avg_duration": 12,
            "median_duration": 9,
            "trade_age_ratio": 0.58,
            "target_progression": [],
            "target_staircase": {
                "targets_reached": 0, "latest_target": 200.0,
                "previous_target": None, "is_staircasing": False,
                "staircase_direction": None,
            },
            "inflection_flags": ["CANCEL_DANGER", "REVERSAL", "HIGH_CHURN"],
        },
        # TSLA — BUY with target staircase + trade aging (urgency factors)
        {
            "ticker": "TSLA",
            "display_ticker": "TSLA",
            "name": "Tesla Inc.",
            "instrument": "Tesla",
            "signal": "BUY",
            "origin_price": 245.00,
            "cancel_level": 235.00,
            "cancel_direction": "below",
            "implied_reversal": False,
            "last_signal_date": "2026-03-25",
            "price": 268.50,
            "price_source": "yfinance",
            "price_as_of": "2026-04-22T08:00:00",
            "pnl_pct": 9.59,
            "cancel_dist_pct": -12.48,  # safely above cancel
            "target_price": 280.00,
            "target_direction": "UPSIDE",
            "target_condition": None,
            "target_dist_pct": 4.28,
            "reward_risk": 0.34,  # below 1 → LOW_RR + urgency
            "cancel_trajectory": [220.0, 225.0, 230.0, 235.0],
            "ntc_count_30d": 2,
            "cycles": [
                {"timeframe": "daily", "direction": "UP", "until_description": "late April"},
                {"timeframe": "weekly", "direction": "UP", "until_description": "early May"},
            ],
            "cycle_alignment": "ALIGNED",
            "trade_stats": {
                "sharpe": 0.95, "win_rate": 55.0, "kelly": 0.12, "trades": 18,
                "avg_duration": 22, "median_duration": 18,
            },
            "risk_flag": "",
            "signal_history": [],
            "trade_age_days": 28,
            "avg_duration": 22,
            "median_duration": 18,
            "trade_age_ratio": 1.27,  # past avg → TRADE_AGING + urgency
            "target_progression": [],
            "target_staircase": {
                "targets_reached": 2,
                "latest_target": 280.0,
                "previous_target": 265.0,
                "is_staircasing": True,
                "staircase_direction": "HIGHER",
            },
            "inflection_flags": ["LOW_RR", "TRADE_AGING", "TARGET_REACHED"],
        },
        # ES — BUY with conflicting cycles, no target, no trade stats
        {
            "ticker": "ES",
            "display_ticker": "S&P 500",
            "name": "S&P 500 Futures",
            "instrument": "ES Futures",
            "signal": "BUY",
            "origin_price": 5420.0,
            "cancel_level": 5350.0,
            "cancel_direction": "below",
            "implied_reversal": False,
            "last_signal_date": "2026-04-18",
            "price": 5485.25,
            "price_source": "yfinance",
            "price_as_of": "2026-04-22T08:00:00",
            "pnl_pct": 1.20,
            "cancel_dist_pct": -2.47,
            "target_price": None,
            "target_direction": None,
            "target_condition": None,
            "target_dist_pct": None,
            "reward_risk": None,
            "cancel_trajectory": [5350.0],
            "ntc_count_30d": 0,
            "cycles": [
                {"timeframe": "daily", "direction": "DOWN", "until_description": "near term"},
                {"timeframe": "weekly", "direction": "DOWN", "until_description": "early May"},
            ],
            "cycle_alignment": "CONFLICTING",
            "trade_stats": None,
            "risk_flag": "",
            "signal_history": [],
            "trade_age_days": 4,
            "avg_duration": None,
            "median_duration": None,
            "trade_age_ratio": None,
            "target_progression": [],
            "target_staircase": {
                "targets_reached": 0, "latest_target": None,
                "previous_target": None, "is_staircasing": False,
                "staircase_direction": None,
            },
            "inflection_flags": [],
        },
    ]


def _render(monkeypatch):
    """Render HTML and subject with a frozen clock."""
    _freeze_time(monkeypatch)
    # Re-import to pick up monkeypatched datetime if needed
    from nenner_engine.stock_report import (
        build_stock_report_html, build_report_subject,
    )
    stocks = _make_fixture_stocks()
    html = build_stock_report_html(stocks, stanley_take="<b>Stanley test take</b>")
    subject = build_report_subject(stocks)
    return html, subject


def test_build_stock_report_html_matches_golden(monkeypatch):
    """build_stock_report_html output must match the committed golden file."""
    html, _ = _render(monkeypatch)

    if not GOLDEN_HTML.exists():
        FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
        GOLDEN_HTML.write_text(html, encoding="utf-8")
        pytest.skip(
            f"Golden file created at {GOLDEN_HTML}. "
            f"Commit it and re-run; the test will then enforce equivalence."
        )

    expected = GOLDEN_HTML.read_text(encoding="utf-8")
    if html != expected:
        # Surface a digest so failures point at the byte-level drift
        actual_sha = hashlib.sha256(html.encode("utf-8")).hexdigest()[:12]
        expected_sha = hashlib.sha256(expected.encode("utf-8")).hexdigest()[:12]
        # Write the diff side-by-side to make debugging easier
        actual_path = FIXTURE_DIR / "stock_report_actual.html"
        actual_path.write_text(html, encoding="utf-8")
        raise AssertionError(
            f"HTML output drifted from golden.\n"
            f"  expected sha256={expected_sha}\n"
            f"  actual   sha256={actual_sha}\n"
            f"  actual written to {actual_path} for diffing\n"
            f"  expected len={len(expected)}, actual len={len(html)}"
        )


def test_build_report_subject_matches_golden(monkeypatch):
    """build_report_subject output must match the committed golden file."""
    _, subject = _render(monkeypatch)

    if not GOLDEN_SUBJECT.exists():
        FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
        GOLDEN_SUBJECT.write_text(subject, encoding="utf-8")
        pytest.skip(
            f"Golden file created at {GOLDEN_SUBJECT}. "
            f"Commit it and re-run; the test will then enforce equivalence."
        )

    expected = GOLDEN_SUBJECT.read_text(encoding="utf-8")
    assert subject == expected, (
        f"Subject drifted.\n  expected: {expected!r}\n  actual:   {subject!r}"
    )


def test_public_api_importable():
    """The symbols downstream code imports must remain available."""
    from nenner_engine.stock_report import (  # noqa: F401
        # Data-section exports used by tests/test_nenner_engine.py
        _get_cancel_trajectory,
        _count_ntc,
        _compute_reward_risk,
        _assess_cycle_alignment,
        _detect_inflection_flags,
        gather_report_data,
        # HTML/orchestration
        build_stock_report_html,
        build_report_subject,
        # Email (re-exported from postmaster)
        send_email,
        # LLM (used via @patch in test_nenner_engine.py)
        _generate_stanley_take,
        # Constants
        FOCUS_STOCKS,
        CANCEL_DANGER_PCT,
    )
    # Entry point used by scripts/send_track_record.py and the scheduler
    from nenner_engine.stock_report import (  # noqa: F401
        generate_and_send_stock_report,
        generate_stock_report_on_demand,
    )
