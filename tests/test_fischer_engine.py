"""
Unit tests for Fischer Engine — options pricing, Greeks, IV solver, EV.

Validates against known option prices and published values.
Uses pytest with tolerances appropriate for financial calculations.
"""

import math
from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pytest
from scipy.stats import norm

from nenner_engine.fischer_engine import (
    ET,
    GAMMA_PROXIMITY_PCT,
    EVResult,
    Greeks,
    OptionType,
    _d1_d2,
    baw_price,
    bsm_price,
    compute_ev,
    estimate_margin,
    fit_iv_smile,
    gamma_warning,
    greeks,
    implied_earnings_move,
    implied_volatility,
    price_option,
    rank_strikes,
    select_pricer,
    smile_adjusted_iv,
    time_to_expiry,
)


# ---------------------------------------------------------------------------
# Known reference values for validation
# ---------------------------------------------------------------------------
# Standard BSM example: S=100, K=100, T=1, r=0.05, σ=0.20, q=0
# Expected call ≈ 10.4506, put ≈ 5.5735
# Source: Hull, Options, Futures, and Other Derivatives

BSM_REF_S = 100.0
BSM_REF_K = 100.0
BSM_REF_T = 1.0
BSM_REF_R = 0.05
BSM_REF_SIGMA = 0.20
BSM_REF_Q = 0.0
BSM_REF_CALL = 10.4506
BSM_REF_PUT = 5.5735


class TestBSMPricing:
    """§2.1 — Black-Scholes-Merton pricing."""

    def test_call_price_reference(self):
        """Validate call price against Hull textbook value."""
        price = bsm_price(BSM_REF_S, BSM_REF_K, BSM_REF_T, BSM_REF_R,
                          BSM_REF_SIGMA, BSM_REF_Q, "C")
        assert price == pytest.approx(BSM_REF_CALL, abs=0.01)

    def test_put_price_reference(self):
        """Validate put price against Hull textbook value."""
        price = bsm_price(BSM_REF_S, BSM_REF_K, BSM_REF_T, BSM_REF_R,
                          BSM_REF_SIGMA, BSM_REF_Q, "P")
        assert price == pytest.approx(BSM_REF_PUT, abs=0.01)

    def test_put_call_parity(self):
        """C - P = S*exp(-qT) - K*exp(-rT) must hold for European options."""
        call = bsm_price(BSM_REF_S, BSM_REF_K, BSM_REF_T, BSM_REF_R,
                         BSM_REF_SIGMA, BSM_REF_Q, "C")
        put = bsm_price(BSM_REF_S, BSM_REF_K, BSM_REF_T, BSM_REF_R,
                        BSM_REF_SIGMA, BSM_REF_Q, "P")
        parity = BSM_REF_S * math.exp(-BSM_REF_Q * BSM_REF_T) - \
                 BSM_REF_K * math.exp(-BSM_REF_R * BSM_REF_T)
        assert call - put == pytest.approx(parity, abs=0.001)

    def test_put_call_parity_with_dividend(self):
        """Put-call parity with continuous dividend yield."""
        S, K, T, r, sigma, q = 100, 100, 0.5, 0.05, 0.25, 0.02
        call = bsm_price(S, K, T, r, sigma, q, "C")
        put = bsm_price(S, K, T, r, sigma, q, "P")
        parity = S * math.exp(-q * T) - K * math.exp(-r * T)
        assert call - put == pytest.approx(parity, abs=0.001)

    def test_atm_call_higher_than_put(self):
        """For ATM with r > 0, call > put due to carry cost."""
        call = bsm_price(100, 100, 1.0, 0.05, 0.20, 0.0, "C")
        put = bsm_price(100, 100, 1.0, 0.05, 0.20, 0.0, "P")
        assert call > put

    def test_deep_itm_call_approaches_intrinsic(self):
        """Deep ITM call should approach S - K*exp(-rT)."""
        price = bsm_price(200, 100, 1.0, 0.05, 0.20, 0.0, "C")
        lower_bound = 200 - 100 * math.exp(-0.05)
        assert price >= lower_bound - 0.01

    def test_deep_otm_put_near_zero(self):
        """Deep OTM put should be near zero."""
        price = bsm_price(200, 100, 0.1, 0.05, 0.20, 0.0, "P")
        assert price < 0.01

    def test_zero_vol_call(self):
        """With zero vol, call = max(S*exp(-qT) - K*exp(-rT), 0)."""
        # Use a small vol since zero exactly triggers the guard
        price = bsm_price(110, 100, 1.0, 0.05, 0.001, 0.0, "C")
        expected = 110 - 100 * math.exp(-0.05)
        assert price == pytest.approx(expected, abs=0.1)

    def test_expired_call(self):
        """At expiry, value = intrinsic."""
        assert bsm_price(105, 100, 0.0, 0.05, 0.20, 0.0, "C") == 5.0
        assert bsm_price(95, 100, 0.0, 0.05, 0.20, 0.0, "C") == 0.0

    def test_expired_put(self):
        """At expiry, value = intrinsic."""
        assert bsm_price(95, 100, 0.0, 0.05, 0.20, 0.0, "P") == 5.0
        assert bsm_price(105, 100, 0.0, 0.05, 0.20, 0.0, "P") == 0.0

    def test_higher_vol_higher_price(self):
        """Higher IV should produce higher option prices (both puts and calls)."""
        low_vol = bsm_price(100, 100, 1.0, 0.05, 0.15, 0.0, "C")
        high_vol = bsm_price(100, 100, 1.0, 0.05, 0.30, 0.0, "C")
        assert high_vol > low_vol

    def test_option_type_enum(self):
        """OptionType enum should work the same as string."""
        p1 = bsm_price(100, 100, 1.0, 0.05, 0.20, 0.0, OptionType.PUT)
        p2 = bsm_price(100, 100, 1.0, 0.05, 0.20, 0.0, "P")
        assert p1 == p2

    def test_short_dated_put(self):
        """Short-dated ATM put should have meaningful value."""
        # SPY-like: S=590, K=590, 1 day, r=4.5%, σ=15%
        T = 1.0 / 365.25
        price = bsm_price(590, 590, T, 0.045, 0.15, 0.0, "P")
        assert 0.5 < price < 5.0  # reasonable range for 1-day ATM put


class TestGreeks:
    """§2.2 — Greeks validation."""

    def test_call_delta_range(self):
        """Call delta should be in [0, 1]."""
        g = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "C")
        assert 0 < g.delta < 1

    def test_put_delta_range(self):
        """Put delta should be in [-1, 0]."""
        g = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "P")
        assert -1 < g.delta < 0

    def test_delta_put_call_relation(self):
        """Delta_call - Delta_put = exp(-qT) for same S, K, T, r, σ."""
        gc = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "C")
        gp = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "P")
        assert gc.delta - gp.delta == pytest.approx(1.0, abs=0.001)

    def test_gamma_positive(self):
        """Gamma is always positive for both calls and puts."""
        gc = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "C")
        gp = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "P")
        assert gc.gamma > 0
        assert gp.gamma > 0

    def test_gamma_same_for_put_call(self):
        """Gamma is identical for put and call with same params."""
        gc = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "C")
        gp = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "P")
        assert gc.gamma == pytest.approx(gp.gamma, abs=1e-8)

    def test_theta_negative_for_long(self):
        """Theta is negative (time decay erodes value) for ATM options."""
        gc = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "C")
        gp = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "P")
        assert gc.theta < 0
        assert gp.theta < 0

    def test_vega_positive(self):
        """Vega is always positive."""
        gc = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "C")
        assert gc.vega > 0

    def test_vega_same_for_put_call(self):
        """Vega is identical for put and call with same params."""
        gc = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "C")
        gp = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "P")
        assert gc.vega == pytest.approx(gp.vega, abs=1e-8)

    def test_0dte_gamma_is_large_atm(self):
        """0DTE ATM gamma should be much larger than longer-dated."""
        T_short = 0.5 / 365.25  # half a day
        T_long = 30 / 365.25    # 30 days
        g_short = greeks(590, 590, T_short, 0.045, 0.15, 0.0, "P")
        g_long = greeks(590, 590, T_long, 0.045, 0.15, 0.0, "P")
        assert g_short.gamma > g_long.gamma * 3  # much higher for 0DTE

    def test_deep_otm_delta_near_zero(self):
        """Deep OTM put should have delta near zero."""
        g = greeks(100, 70, 0.1, 0.05, 0.20, 0.0, "P")
        assert abs(g.delta) < 0.01

    def test_deep_itm_put_delta_near_neg_one(self):
        """Deep ITM put should have delta near -1."""
        g = greeks(80, 120, 0.1, 0.05, 0.20, 0.0, "P")
        assert g.delta < -0.95

    def test_greeks_at_expiry(self):
        """At expiry, delta is ±1 or 0, others are 0."""
        g = greeks(105, 100, 0.0, 0.05, 0.20, 0.0, "C")
        assert g.delta == 1.0
        assert g.gamma == 0.0

    def test_greeks_as_dict(self):
        """as_dict should return all five Greeks."""
        g = greeks(100, 100, 1.0, 0.05, 0.20, 0.0, "P")
        d = g.as_dict()
        assert set(d.keys()) == {"delta", "gamma", "theta", "vega", "rho"}


class TestBAW:
    """§2.1 — Barone-Adesi-Whaley American option pricing."""

    def test_american_put_geq_european(self):
        """American put must be >= European put."""
        S, K, T, r, sigma, q = 100, 110, 0.5, 0.05, 0.25, 0.02
        euro = bsm_price(S, K, T, r, sigma, q, "P")
        amer = baw_price(S, K, T, r, sigma, q, "P")
        assert amer >= euro - 0.001

    def test_baw_equals_bsm_for_otm_put(self):
        """For OTM put, early exercise premium should be minimal."""
        S, K, T, r, sigma, q = 100, 90, 0.5, 0.05, 0.20, 0.01
        euro = bsm_price(S, K, T, r, sigma, q, "P")
        amer = baw_price(S, K, T, r, sigma, q, "P")
        # OTM — premium should be very small
        assert amer - euro < 0.5

    def test_baw_deep_itm_put(self):
        """Deep ITM American put should exceed European put."""
        S, K, T, r, sigma, q = 80, 120, 0.5, 0.05, 0.25, 0.02
        euro = bsm_price(S, K, T, r, sigma, q, "P")
        amer = baw_price(S, K, T, r, sigma, q, "P")
        # American >= European, and both should be substantial
        assert amer >= euro - 0.01
        assert amer > 30  # K-S=40, option should be worth most of that

    def test_baw_no_dividend_call_equals_bsm(self):
        """American call on non-dividend stock = European call."""
        S, K, T, r, sigma = 100, 100, 1.0, 0.05, 0.20
        euro = bsm_price(S, K, T, r, sigma, 0.0, "C")
        amer = baw_price(S, K, T, r, sigma, 0.0, "C")
        assert amer == pytest.approx(euro, abs=0.001)

    def test_baw_at_expiry(self):
        """At expiry, BAW = intrinsic."""
        assert baw_price(95, 100, 0.0, 0.05, 0.20, 0.02, "P") == 5.0
        assert baw_price(105, 100, 0.0, 0.05, 0.20, 0.02, "P") == 0.0


class TestSelectPricer:
    """§2.1 — Model selection logic."""

    def test_bsm_for_no_dividend(self):
        """No dividends → BSM."""
        assert select_pricer(100, 100, 0.0, "P") == "BSM"

    def test_bsm_for_otm_put_with_dividend(self):
        """OTM put with dividend → BSM (not >2% ITM)."""
        assert select_pricer(100, 95, 0.02, "P") == "BSM"

    def test_baw_for_deep_itm_put_with_dividend(self):
        """Deep ITM put with dividend → BAW."""
        assert select_pricer(100, 105, 0.02, "P") == "BAW"

    def test_bsm_for_call_no_dividend(self):
        """Call without dividend → BSM regardless of moneyness."""
        assert select_pricer(100, 80, 0.0, "C") == "BSM"


class TestIVSolver:
    """§2.3 — Newton-Raphson implied volatility solver."""

    def test_round_trip_call(self):
        """Price → IV → Price should recover the original vol."""
        S, K, T, r, sigma, q = 100, 100, 1.0, 0.05, 0.25, 0.0
        price = bsm_price(S, K, T, r, sigma, q, "C")
        iv = implied_volatility(price, S, K, T, r, q, "C")
        assert iv is not None
        assert iv == pytest.approx(sigma, abs=0.001)

    def test_round_trip_put(self):
        """Price → IV → Price should recover the original vol for puts."""
        S, K, T, r, sigma, q = 100, 95, 0.5, 0.05, 0.30, 0.0
        price = bsm_price(S, K, T, r, sigma, q, "P")
        iv = implied_volatility(price, S, K, T, r, q, "P")
        assert iv is not None
        assert iv == pytest.approx(sigma, abs=0.001)

    def test_round_trip_with_dividend(self):
        """IV solver works with continuous dividend yield."""
        S, K, T, r, sigma, q = 100, 100, 1.0, 0.05, 0.20, 0.02
        price = bsm_price(S, K, T, r, sigma, q, "P")
        iv = implied_volatility(price, S, K, T, r, q, "P")
        assert iv is not None
        assert iv == pytest.approx(sigma, abs=0.001)

    def test_high_iv(self):
        """Solver handles high IV (e.g., meme stocks)."""
        S, K, T, r, sigma, q = 100, 100, 0.1, 0.05, 1.50, 0.0
        price = bsm_price(S, K, T, r, sigma, q, "C")
        iv = implied_volatility(price, S, K, T, r, q, "C")
        assert iv is not None
        assert iv == pytest.approx(sigma, abs=0.01)

    def test_low_iv(self):
        """Solver handles low IV (e.g., treasuries)."""
        S, K, T, r, sigma, q = 100, 100, 1.0, 0.05, 0.05, 0.0
        price = bsm_price(S, K, T, r, sigma, q, "P")
        iv = implied_volatility(price, S, K, T, r, q, "P")
        assert iv is not None
        assert iv == pytest.approx(sigma, abs=0.001)

    def test_short_dated(self):
        """Solver works for 0DTE options."""
        T = 0.5 / 365.25
        S, K, r, sigma, q = 590, 588, 0.045, 0.15, 0.0
        price = bsm_price(S, K, T, r, sigma, q, "P")
        iv = implied_volatility(price, S, K, T, r, q, "P")
        assert iv is not None
        assert iv == pytest.approx(sigma, abs=0.005)

    def test_zero_price_returns_none(self):
        """Zero market price should return None."""
        assert implied_volatility(0.0, 100, 100, 1.0, 0.05, 0.0, "C") is None

    def test_negative_time_returns_none(self):
        """Negative time should return None."""
        assert implied_volatility(5.0, 100, 100, -0.1, 0.05, 0.0, "C") is None

    def test_convergence_tolerance(self):
        """IV should produce a price within $0.001 of market (§2.3 spec)."""
        S, K, T, r, q = 100, 105, 0.25, 0.05, 0.0
        market_price = 3.50
        iv = implied_volatility(market_price, S, K, T, r, q, "C")
        assert iv is not None
        recovered = bsm_price(S, K, T, r, iv, q, "C")
        assert abs(recovered - market_price) < 0.001


class TestSmileInterpolation:
    """§2.1 — 0DTE volatility smile fitting."""

    def test_fit_parabolic_smile(self):
        """Symmetric smile should produce a parabola with minimum near ATM."""
        spot = 100.0
        strikes = np.array([95, 97, 99, 100, 101, 103, 105])
        # Typical smile: higher IV at wings
        ivs = np.array([0.25, 0.22, 0.20, 0.19, 0.20, 0.22, 0.25])
        fit = fit_iv_smile(strikes, ivs, spot)
        assert fit is not None
        # Minimum should be near ATM (moneyness ≈ 1.0)
        atm_iv = smile_adjusted_iv(100, spot, fit, 0.20)
        wing_iv = smile_adjusted_iv(95, spot, fit, 0.20)
        assert wing_iv > atm_iv

    def test_insufficient_data_returns_none(self):
        """Less than 3 data points should return None."""
        fit = fit_iv_smile([100, 105], [0.20, 0.22], 100.0)
        assert fit is None

    def test_nan_values_filtered(self):
        """NaN IVs should be excluded from fitting."""
        strikes = np.array([95, 97, 99, 100, 101, 103, 105])
        ivs = np.array([0.25, np.nan, 0.20, 0.19, np.nan, 0.22, 0.25])
        fit = fit_iv_smile(strikes, ivs, 100.0)
        assert fit is not None

    def test_fallback_when_no_smile(self):
        """With no smile fit, should return fallback IV."""
        iv = smile_adjusted_iv(100, 100, None, 0.20)
        assert iv == 0.20

    def test_sanity_bounds(self):
        """Extreme smile values should fall back to flat IV."""
        # Create a fit that would produce negative IV for some strikes
        fit = np.poly1d([-100, 200, -100])  # extreme parabola
        iv = smile_adjusted_iv(100, 100, fit, 0.20)
        assert iv == 0.20  # should fall back


class TestTimeToExpiry:
    """§2.5 — Timezone-aware T calculation."""

    def test_same_day_morning(self):
        """Morning of expiry day: T should be small but positive."""
        expiry = date(2026, 2, 27)
        now = datetime(2026, 2, 27, 9, 30, 0, tzinfo=ET)  # 9:30 AM
        T = time_to_expiry(expiry, now)
        # 6.5 hours to 4 PM = 390 minutes
        expected = 390 / 525_960
        assert T == pytest.approx(expected, rel=0.01)

    def test_previous_day_close(self):
        """Previous day close: T should be about 1 day."""
        expiry = date(2026, 2, 27)
        now = datetime(2026, 2, 26, 16, 0, 0, tzinfo=ET)
        T = time_to_expiry(expiry, now)
        expected = 24 * 60 / 525_960  # 1 calendar day in years
        assert T == pytest.approx(expected, rel=0.01)

    def test_expired_returns_zero(self):
        """Past expiry should return 0."""
        expiry = date(2026, 2, 25)
        now = datetime(2026, 2, 26, 10, 0, 0, tzinfo=ET)
        assert time_to_expiry(expiry, now) == 0.0

    def test_one_week_out(self):
        """7 days out should be about 7/365.25 years."""
        expiry = date(2026, 3, 4)
        now = datetime(2026, 2, 25, 16, 0, 0, tzinfo=ET)
        T = time_to_expiry(expiry, now)
        expected = 7 * 24 * 60 / 525_960
        assert T == pytest.approx(expected, rel=0.01)


class TestEVCalculator:
    """§3.1 — Expected Value engine."""

    def test_positive_ev_for_overpriced_option(self):
        """If bid > theoretical price, EV should be positive."""
        result = compute_ev(
            S=100, K=95, T=0.1, r=0.05, sigma=0.20, q=0.0,
            bid=2.50, ask=2.80, option_type="P",
            expiry=date(2026, 3, 7), capital=100_000,
        )
        theo = bsm_price(100, 95, 0.1, 0.05, 0.20, 0.0, "P")
        if 2.50 > theo:
            assert result.net_ev_per_contract > 0
        else:
            assert result.net_ev_per_contract < 0

    def test_probability_sums_to_one(self):
        """P(expire worthless) + P(assignment) should equal 1."""
        result = compute_ev(
            S=100, K=100, T=0.5, r=0.05, sigma=0.20, q=0.0,
            bid=4.0, ask=4.50, option_type="P",
            expiry=date(2026, 8, 25), capital=100_000,
        )
        assert result.p_expire_worthless + result.p_assignment == \
            pytest.approx(1.0, abs=0.001)

    def test_margin_determines_contracts(self):
        """Contract count should be bounded by capital / margin."""
        result = compute_ev(
            S=590, K=590, T=1/365.25, r=0.045, sigma=0.15, q=0.0,
            bid=1.50, ask=1.80, option_type="P",
            expiry=date(2026, 2, 26), capital=500_000,
        )
        margin_per = 590 * 100 * 0.20  # = 11,800
        expected_contracts = int(500_000 / margin_per)  # = 42
        assert result.contracts == expected_contracts

    def test_wide_spread_flag(self):
        """Bid-ask > 15% of mid should flag WIDE_SPREAD."""
        result = compute_ev(
            S=100, K=100, T=0.5, r=0.05, sigma=0.20, q=0.0,
            bid=1.0, ask=2.0, option_type="P",
            expiry=date(2026, 8, 25), capital=100_000,
        )
        assert "WIDE_SPREAD" in result.flags

    def test_thin_oi_flag(self):
        """OI < 100 should flag THIN_OI."""
        result = compute_ev(
            S=100, K=100, T=0.5, r=0.05, sigma=0.20, q=0.0,
            bid=4.0, ask=4.50, option_type="P",
            expiry=date(2026, 8, 25), capital=100_000,
            oi=50,
        )
        assert "THIN_OI" in result.flags

    def test_gamma_flag_0dte(self):
        """0DTE near ATM should flag GAMMA_ELEVATED."""
        result = compute_ev(
            S=590, K=590, T=0.001, r=0.045, sigma=0.15, q=0.0,
            bid=0.50, ask=0.70, option_type="P",
            expiry=date.today(), capital=100_000,
        )
        assert "GAMMA_ELEVATED" in result.flags

    def test_as_dict_completeness(self):
        """as_dict should contain all key fields."""
        result = compute_ev(
            S=100, K=100, T=0.5, r=0.05, sigma=0.20, q=0.0,
            bid=4.0, ask=4.50, option_type="P",
            expiry=date(2026, 8, 25), capital=100_000,
        )
        d = result.as_dict()
        assert "strike" in d
        assert "net_ev_contract" in d
        assert "nenner_score" in d
        assert "earnings_flag" in d


class TestStrikeRanking:
    """§3.3 — Strike ranking logic."""

    def _make_results(self) -> list[EVResult]:
        """Create sample EVResults for ranking tests."""
        results = []
        for K, ev in [(95, 15.0), (97, 25.0), (99, 10.0), (100, 5.0)]:
            r = EVResult(
                strike=K, expiry=date(2026, 2, 27), dte=2,
                option_type="P", bid=2.0, ask=2.20, mid=2.10,
                iv=0.20, model_used="BSM", theoretical_price=1.80,
                delta=-0.15 if K < 100 else -0.50,
                gamma=0.05, theta=-0.02, vega=0.10, rho=-0.01,
                p_expire_worthless=0.85, p_assignment=0.15,
                premium_collected=200, expected_assignment_loss=180,
                net_ev_per_contract=ev, net_ev_per_position=ev * 10,
                contracts=10, nenner_score=70,
                spot=100, rate=0.05, div_yield=0.0,
            )
            results.append(r)
        return results

    def test_sorted_by_ev_descending(self):
        """Results should be sorted by Net EV descending."""
        results = self._make_results()
        ranked = rank_strikes(results, delta_cap=0.35)
        evs = [r.net_ev_per_contract for r in ranked]
        assert evs == sorted(evs, reverse=True)

    def test_best_label(self):
        """Top result should have BEST flag."""
        results = self._make_results()
        ranked = rank_strikes(results, delta_cap=0.35)
        assert "BEST" in ranked[0].flags

    def test_alternate_label(self):
        """Second result should have ALTERNATE flag."""
        results = self._make_results()
        ranked = rank_strikes(results, delta_cap=0.35)
        # Find the one with ALTERNATE
        alt = [r for r in ranked if "ALTERNATE" in r.flags]
        assert len(alt) == 1

    def test_delta_exceeded_flag(self):
        """Strikes exceeding delta cap should be flagged."""
        results = self._make_results()
        ranked = rank_strikes(results, delta_cap=0.35)
        exceeded = [r for r in ranked if "DELTA_EXCEEDED" in r.flags]
        # The K=100 strike has delta=-0.50 which exceeds 0.35
        assert any(r.strike == 100 for r in exceeded)

    def test_delta_exceeded_not_labeled_best(self):
        """A delta-exceeded strike should not be labeled BEST."""
        results = self._make_results()
        ranked = rank_strikes(results, delta_cap=0.35)
        best = [r for r in ranked if "BEST" in r.flags]
        assert len(best) == 1
        assert "DELTA_EXCEEDED" not in best[0].flags


class TestGammaWarning:
    """§2.2 — 0DTE gamma warning display."""

    def test_warning_fires_near_atm_0dte(self):
        """Should fire when 0DTE and strike within 0.5% of spot."""
        w = gamma_warning(590, 591, 0, 0.15, 0.001)
        assert w is not None
        assert "ELEVATED" in w["warning"]

    def test_no_warning_for_1dte(self):
        """Should not fire for DTE >= 1."""
        w = gamma_warning(590, 591, 1, 0.15, 1/365.25)
        assert w is None

    def test_no_warning_far_from_atm(self):
        """Should not fire when strike is far from spot."""
        w = gamma_warning(590, 580, 0, 0.15, 0.001)  # 1.7% away
        assert w is None

    def test_adverse_move_display(self):
        """Should show 0.5% and 1.0% adverse move P&L."""
        w = gamma_warning(590, 590, 0, 0.15, 0.001, contracts=10)
        assert "0.5%" in w["adverse_moves"]
        assert "1.0%" in w["adverse_moves"]


class TestMarginEstimation:
    """§7 — Margin estimation."""

    def test_single_contract(self):
        """Margin for 1 contract of $590 put = $11,800."""
        assert estimate_margin(590, 1) == pytest.approx(11_800)

    def test_multiple_contracts(self):
        """Margin scales linearly with contracts."""
        assert estimate_margin(100, 10) == pytest.approx(20_000)


class TestImpliedEarningsMove:
    """§5.4 — Implied earnings move from ATM straddle."""

    def test_basic_calculation(self):
        """(call_mid + put_mid) / spot should give the implied move."""
        move = implied_earnings_move(5.0, 4.0, 100.0)
        assert move == pytest.approx(0.09)  # ±9%

    def test_zero_spot(self):
        """Zero spot should return 0."""
        assert implied_earnings_move(5.0, 4.0, 0.0) == 0.0

    def test_realistic_spy(self):
        """SPY ATM straddle for 0DTE earnings."""
        # SPY at 590, ATM straddle might be ~$5 call + $5 put
        move = implied_earnings_move(5.0, 5.0, 590.0)
        assert move == pytest.approx(0.0169, abs=0.001)  # ~1.7%
