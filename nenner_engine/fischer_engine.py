"""
Fischer Engine — Options Pricing, Greeks, IV, and Expected Value
================================================================
Named for Fischer Black (1938–1995), co-creator of the Black-Scholes model.

Core math engine for Vartanian Capital Management's short-dated option strategies.
Implements BSM (European), BAW (American early exercise), Newton-Raphson IV solver,
full Greeks, 0DTE smile interpolation, and expected value ranking.

Spec reference: Fischer_Agent_Specification_v2.md
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np
from scipy.stats import norm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ET = ZoneInfo("America/New_York")
MINUTES_PER_YEAR = 525_960  # 365.25 * 24 * 60
TRADING_DAYS_PER_YEAR = 252

# Risk control defaults (§7)
DEFAULT_DELTA_CAP = 0.35
TIGHTENED_DELTA_CAP = 0.20
MINIMUM_DELTA_CAP = 0.15
LIQUIDITY_SPREAD_THRESHOLD = 0.15  # 15% of mid
LIQUIDITY_OI_THRESHOLD = 100
GAMMA_PROXIMITY_PCT = 0.005  # 0.5% of spot
MONEYNESS_BAND = 0.0275  # ±2.75% from ATM (matches Excel sheet formulas)


class OptionType(str, Enum):
    PUT = "P"
    CALL = "C"


class Intent(str, Enum):
    COVERED_PUT = "covered_put"
    COVERED_CALL = "covered_call"


# ---------------------------------------------------------------------------
# §2.5 — Time-to-Expiry with ET precision
# ---------------------------------------------------------------------------

def time_to_expiry(
    expiry_date: date,
    now: datetime | None = None,
) -> float:
    """Compute T in years from now to 4:00 PM ET on expiry_date.

    Uses minute-level precision when T < 1 day (§2.5).
    Returns 0.0 if expiry has passed.
    """
    if now is None:
        now = datetime.now(ET)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=ET)

    expiry_dt = datetime(
        expiry_date.year, expiry_date.month, expiry_date.day,
        16, 0, 0, tzinfo=ET,
    )

    diff = expiry_dt - now
    if diff.total_seconds() <= 0:
        return 0.0

    minutes_remaining = diff.total_seconds() / 60.0
    T = minutes_remaining / MINUTES_PER_YEAR
    return T


# ---------------------------------------------------------------------------
# §2.1 — Black-Scholes-Merton
# ---------------------------------------------------------------------------

def _d1_d2(
    S: float, K: float, T: float, r: float, sigma: float, q: float = 0.0,
) -> tuple[float, float]:
    """Compute d1 and d2 for BSM."""
    if T <= 0 or sigma <= 0:
        # At expiry: intrinsic value only
        return (float("inf"), float("inf")) if S > K else (float("-inf"), float("-inf"))

    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    return d1, d2


def bsm_price(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptionType = "P",
) -> float:
    """Black-Scholes-Merton price for European option.

    Parameters
    ----------
    S : spot price (mid of bid/ask)
    K : strike price
    T : time to expiry in years
    r : risk-free rate (annualized, continuous)
    sigma : implied volatility (annualized)
    q : continuous dividend yield (annualized)
    option_type : 'P' for put, 'C' for call
    """
    if T <= 0:
        # At or past expiry — intrinsic only
        if str(option_type).upper() in ("C", "CALL"):
            return max(S - K, 0.0)
        return max(K - S, 0.0)

    d1, d2 = _d1_d2(S, K, T, r, sigma, q)
    discount = math.exp(-r * T)
    fwd_discount = math.exp(-q * T)

    if str(option_type).upper() in ("C", "CALL"):
        return S * fwd_discount * norm.cdf(d1) - K * discount * norm.cdf(d2)
    else:
        return K * discount * norm.cdf(-d2) - S * fwd_discount * norm.cdf(-d1)


# ---------------------------------------------------------------------------
# §2.2 — Greeks
# ---------------------------------------------------------------------------

@dataclass
class Greeks:
    delta: float
    gamma: float
    theta: float  # per calendar day
    vega: float   # per 1% (0.01) IV change
    rho: float    # per 1% (0.01) rate change

    def as_dict(self) -> dict:
        return {
            "delta": round(self.delta, 6),
            "gamma": round(self.gamma, 6),
            "theta": round(self.theta, 6),
            "vega": round(self.vega, 6),
            "rho": round(self.rho, 6),
        }


def greeks(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptionType = "P",
) -> Greeks:
    """Compute all five Greeks for a European option."""
    if T <= 0 or sigma <= 0:
        # At expiry: delta is 1 or 0, everything else is 0
        is_call = str(option_type).upper() in ("C", "CALL")
        if is_call:
            delta = 1.0 if S > K else 0.0
        else:
            delta = -1.0 if S < K else 0.0
        return Greeks(delta=delta, gamma=0.0, theta=0.0, vega=0.0, rho=0.0)

    d1, d2 = _d1_d2(S, K, T, r, sigma, q)
    sqrt_T = math.sqrt(T)
    n_d1 = norm.pdf(d1)  # standard normal density at d1
    discount = math.exp(-r * T)
    fwd_discount = math.exp(-q * T)
    is_call = str(option_type).upper() in ("C", "CALL")

    # Gamma — same for calls and puts
    gamma = fwd_discount * n_d1 / (S * sigma * sqrt_T)

    # Vega — same for calls and puts (per 1% = 0.01 IV move)
    vega = S * fwd_discount * n_d1 * sqrt_T * 0.01

    if is_call:
        delta = fwd_discount * norm.cdf(d1)
        theta = (
            -S * fwd_discount * n_d1 * sigma / (2.0 * sqrt_T)
            - r * K * discount * norm.cdf(d2)
            + q * S * fwd_discount * norm.cdf(d1)
        ) / 365.25  # per calendar day
        rho = K * T * discount * norm.cdf(d2) * 0.01
    else:
        delta = fwd_discount * (norm.cdf(d1) - 1.0)
        theta = (
            -S * fwd_discount * n_d1 * sigma / (2.0 * sqrt_T)
            + r * K * discount * norm.cdf(-d2)
            - q * S * fwd_discount * norm.cdf(-d1)
        ) / 365.25  # per calendar day
        rho = -K * T * discount * norm.cdf(-d2) * 0.01

    return Greeks(delta=delta, gamma=gamma, theta=theta, vega=vega, rho=rho)


# ---------------------------------------------------------------------------
# §2.1 — Barone-Adesi-Whaley (American early exercise premium)
# ---------------------------------------------------------------------------

def baw_price(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptionType = "P",
) -> float:
    """Barone-Adesi-Whaley approximation for American option price.

    Adds the early exercise premium to the European BSM price.
    Applied when: (a) underlying pays dividends, and (b) put is >2% ITM.
    For calls on non-dividend-paying stocks, American = European.
    """
    european = bsm_price(S, K, T, r, sigma, q, option_type)
    is_call = str(option_type).upper() in ("C", "CALL")

    if T <= 0:
        return european

    # For calls on non-dividend-paying stocks, no early exercise premium
    if is_call and q <= 0:
        return european

    # For puts when r <= 0, early exercise has no time-value benefit
    if not is_call and r <= 0:
        return european

    sigma2 = sigma * sigma
    h = 1.0 - math.exp(-r * T)

    if h < 1e-12:
        return european

    M = 2.0 * r / sigma2
    N_val = 2.0 * (r - q) / sigma2

    if is_call:
        # American call
        q1 = (-(N_val - 1.0) + math.sqrt((N_val - 1.0) ** 2 + 4.0 * M / h)) / 2.0
        S_star = _baw_critical_price_call(S, K, T, r, sigma, q, q1)
        if S >= S_star:
            return K  # deep ITM, exercise immediately — but shouldn't happen often
        d1_star = _d1_d2(S_star, K, T, r, sigma, q)[0]
        A1 = (S_star / q1) * (1.0 - math.exp(-q * T) * norm.cdf(d1_star))
        return european + A1 * (S / S_star) ** q1
    else:
        # American put
        q2 = (-(N_val - 1.0) - math.sqrt((N_val - 1.0) ** 2 + 4.0 * M / h)) / 2.0
        S_star = _baw_critical_price_put(S, K, T, r, sigma, q, q2)
        if S <= S_star:
            return K - S  # deep ITM, exercise immediately
        d1_star = _d1_d2(S_star, K, T, r, sigma, q)[0]
        A2 = -(S_star / q2) * (1.0 - math.exp(-q * T) * norm.cdf(-d1_star))
        return european + A2 * (S / S_star) ** q2


def _baw_critical_price_put(
    S: float, K: float, T: float, r: float, sigma: float,
    q: float, q2: float, tol: float = 1e-6, max_iter: int = 100,
) -> float:
    """Find the critical stock price S* for American put via Newton iteration."""
    # Initial guess: start near the strike
    S_star = K * 0.9
    fwd_discount = math.exp(-q * T)

    for _ in range(max_iter):
        euro_put = bsm_price(S_star, K, T, r, sigma, q, "P")
        d1, _ = _d1_d2(S_star, K, T, r, sigma, q)
        LHS = K - S_star
        b = (1.0 - fwd_discount * norm.cdf(-d1)) * (-S_star / q2)
        RHS = euro_put + b

        diff = LHS - RHS
        if abs(diff) < tol:
            break

        # Derivative for Newton step
        d_euro = -fwd_discount * norm.cdf(-d1)  # delta of euro put at S_star
        d_b = (1.0 / q2) * (
            fwd_discount * norm.cdf(-d1)
            - 1.0
            + fwd_discount * norm.pdf(d1) / (sigma * math.sqrt(T))
        )
        d_LHS = -1.0
        d_RHS = d_euro + d_b
        deriv = d_LHS - d_RHS

        if abs(deriv) < 1e-12:
            break

        S_star = S_star - diff / deriv
        S_star = max(S_star, 1e-6)  # keep positive

    return S_star


def _baw_critical_price_call(
    S: float, K: float, T: float, r: float, sigma: float,
    q: float, q1: float, tol: float = 1e-6, max_iter: int = 100,
) -> float:
    """Find the critical stock price S* for American call via Newton iteration."""
    S_star = K * 1.1
    fwd_discount = math.exp(-q * T)

    for _ in range(max_iter):
        euro_call = bsm_price(S_star, K, T, r, sigma, q, "C")
        d1, _ = _d1_d2(S_star, K, T, r, sigma, q)
        LHS = S_star - K
        b = (1.0 - fwd_discount * norm.cdf(d1)) * (S_star / q1)
        RHS = euro_call + b

        diff = LHS - RHS
        if abs(diff) < tol:
            break

        d_euro = fwd_discount * norm.cdf(d1)  # delta of euro call at S_star
        d_b = (1.0 / q1) * (
            1.0
            - fwd_discount * norm.cdf(d1)
            - fwd_discount * norm.pdf(d1) / (sigma * math.sqrt(T))
        )
        d_LHS = 1.0
        d_RHS = d_euro + d_b
        deriv = d_LHS - d_RHS

        if abs(deriv) < 1e-12:
            break

        S_star = S_star - diff / deriv
        S_star = max(S_star, K * 0.5)

    return S_star


def select_pricer(
    S: float,
    K: float,
    q: float,
    option_type: str | OptionType,
) -> str:
    """Select BSM or BAW based on spec rules (§2.1).

    BAW when: (a) underlying pays dividends, and (b) put is >2% ITM.
    """
    is_put = str(option_type).upper() in ("P", "PUT")

    if q > 0 and is_put:
        itm_pct = (K - S) / S if S > 0 else 0
        if itm_pct > 0.02:
            return "BAW"

    is_call = not is_put
    if q > 0 and is_call:
        # BAW also applies for calls on dividend-paying stocks
        itm_pct = (S - K) / S if S > 0 else 0
        if itm_pct > 0.02:
            return "BAW"

    return "BSM"


def price_option(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    q: float = 0.0,
    option_type: str | OptionType = "P",
) -> tuple[float, str]:
    """Price an option using the appropriate model (BSM or BAW).

    Returns (price, model_used).
    """
    model = select_pricer(S, K, q, option_type)
    if model == "BAW":
        return baw_price(S, K, T, r, sigma, q, option_type), "BAW"
    return bsm_price(S, K, T, r, sigma, q, option_type), "BSM"


# ---------------------------------------------------------------------------
# §2.3 — Newton-Raphson IV Solver
# ---------------------------------------------------------------------------

def implied_volatility(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    q: float = 0.0,
    option_type: str | OptionType = "P",
    tol: float = 0.001,
    max_iter: int = 100,
) -> float | None:
    """Solve for implied volatility using Newton-Raphson (§2.3).

    Uses mid-market price as target. Returns None if solver fails to converge.

    Initial seed: σ_0 = sqrt(2π/T) * (Market_Price / S)
    """
    if T <= 0 or market_price <= 0 or S <= 0:
        return None

    # Intrinsic value check
    is_call = str(option_type).upper() in ("C", "CALL")
    intrinsic = max(S - K, 0.0) if is_call else max(K - S, 0.0)
    if market_price < intrinsic - tol:
        return None  # below intrinsic — no valid IV

    # Initial seed (§2.3)
    sigma = math.sqrt(2.0 * math.pi / T) * (market_price / S)
    sigma = max(sigma, 0.01)  # floor at 1%
    sigma = min(sigma, 5.0)   # cap at 500%

    for _ in range(max_iter):
        price = bsm_price(S, K, T, r, sigma, q, option_type)
        diff = price - market_price

        if abs(diff) < tol:
            return sigma

        # Vega (un-scaled — dPrice/dSigma)
        d1, _ = _d1_d2(S, K, T, r, sigma, q)
        vega_raw = S * math.exp(-q * T) * norm.pdf(d1) * math.sqrt(T)

        if vega_raw < 1e-12:
            # Vega too small — switch to bisection fallback
            return _iv_bisection(market_price, S, K, T, r, q, option_type, tol)

        sigma = sigma - diff / vega_raw
        sigma = max(sigma, 0.001)
        sigma = min(sigma, 10.0)

    # Failed to converge — try bisection
    return _iv_bisection(market_price, S, K, T, r, q, option_type, tol)


def _iv_bisection(
    market_price: float,
    S: float, K: float, T: float, r: float, q: float,
    option_type: str, tol: float,
    lo: float = 0.001, hi: float = 10.0, max_iter: int = 200,
) -> float | None:
    """Bisection fallback when Newton-Raphson fails."""
    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        price = bsm_price(S, K, T, r, mid, q, option_type)
        if abs(price - market_price) < tol:
            return mid
        if price > market_price:
            hi = mid
        else:
            lo = mid
        if hi - lo < 1e-8:
            break
    mid = (lo + hi) / 2.0
    if abs(bsm_price(S, K, T, r, mid, q, option_type) - market_price) < tol * 10:
        return mid
    return None


# ---------------------------------------------------------------------------
# §2.1 — 0DTE Volatility Smile Interpolation
# ---------------------------------------------------------------------------

def fit_iv_smile(
    strikes: list[float] | np.ndarray,
    ivs: list[float] | np.ndarray,
    spot: float,
) -> np.poly1d | None:
    """Fit a second-order polynomial to IV as a function of moneyness.

    moneyness = K / S

    Returns the polynomial, or None if insufficient data.
    """
    strikes = np.asarray(strikes, dtype=float)
    ivs = np.asarray(ivs, dtype=float)

    # Filter out NaN / None / zero IVs
    valid = np.isfinite(ivs) & (ivs > 0) & np.isfinite(strikes)
    strikes = strikes[valid]
    ivs = ivs[valid]

    if len(strikes) < 3:
        return None

    moneyness = strikes / spot
    coeffs = np.polyfit(moneyness, ivs, deg=2)
    return np.poly1d(coeffs)


def smile_adjusted_iv(
    strike: float,
    spot: float,
    smile_fit: np.poly1d | None,
    fallback_iv: float,
) -> float:
    """Get smile-adjusted IV for a strike. Falls back to flat IV if no smile."""
    if smile_fit is None:
        return fallback_iv

    moneyness = strike / spot
    adjusted = float(smile_fit(moneyness))

    # Sanity bounds
    if adjusted < 0.01 or adjusted > 5.0:
        return fallback_iv

    return adjusted


# ---------------------------------------------------------------------------
# §3.1 — Expected Value Engine
# ---------------------------------------------------------------------------

@dataclass
class EVResult:
    """Full EV breakdown for a single option (§3.2)."""
    strike: float
    expiry: date
    dte: int
    option_type: str
    bid: float
    ask: float
    mid: float
    iv: float
    model_used: str  # "BSM" or "BAW"
    theoretical_price: float

    # Greeks
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float

    # Probabilities
    p_expire_worthless: float  # N(d2) for calls; N(-d2) for puts
    p_assignment: float        # 1 - p_expire_worthless

    # BSM Position EV components
    entry_used: float                 # entry price used (explicit or spot)
    expected_close: float             # BSM probability-weighted E[max(K, S_T)] or E[min(K, S_T)]
    max_profit_per_share: float       # best-case P&L per share (assigned scenario)
    p_profit: float                   # P(S_T < breakeven) for puts; P(S_T > breakeven) for calls
    premium_collected: float          # bid × 100 × contracts
    net_ev_per_contract: float        # BSM position EV per contract
    net_ev_per_position: float        # scaled by number of contracts
    contracts: int

    # Context
    nenner_score: int
    spot: float
    rate: float
    div_yield: float

    # Flags
    flags: list[str] = field(default_factory=list)

    # Fischer v2 — profit decomposition
    premium_ratio: float | None = None   # premium / directional distance
    theta_per_share: float = 0.0         # theta (time decay) per share per day

    # Earnings
    earnings_flag: str = "CLEAN"  # CLEAN / STRADDLES / EARNINGS_TODAY
    implied_move: float | None = None

    def as_dict(self) -> dict:
        return {
            "strike": self.strike,
            "expiry": str(self.expiry),
            "dte": self.dte,
            "type": self.option_type,
            "bid": round(self.bid, 4),
            "ask": round(self.ask, 4),
            "iv": round(self.iv, 4),
            "model": self.model_used,
            "theo_price": round(self.theoretical_price, 4),
            "delta": round(self.delta, 4),
            "gamma": round(self.gamma, 4),
            "theta": round(self.theta, 4),
            "vega": round(self.vega, 4),
            "rho": round(self.rho, 4),
            "p_expire_worthless": round(self.p_expire_worthless, 4),
            "p_profit": round(self.p_profit, 4),
            "entry_used": round(self.entry_used, 4),
            "expected_close": round(self.expected_close, 4),
            "max_profit_per_share": round(self.max_profit_per_share, 4),
            "premium_collected": round(self.premium_collected, 2),
            "net_ev_contract": round(self.net_ev_per_contract, 2),
            "net_ev_position": round(self.net_ev_per_position, 2),
            "contracts": self.contracts,
            "nenner_score": self.nenner_score,
            "flags": self.flags,
            "premium_ratio": round(self.premium_ratio, 4) if self.premium_ratio is not None else None,
            "theta_per_share": round(self.theta_per_share, 4),
            "earnings_flag": self.earnings_flag,
            "implied_move": round(self.implied_move, 4) if self.implied_move else None,
        }


def compute_ev(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    q: float,
    bid: float,
    ask: float,
    option_type: str | OptionType,
    expiry: date,
    capital: float,
    nenner_score: int = 50,
    entry_price: float | None = None,
    oi: int = 0,
    volume: int = 0,
) -> EVResult:
    """Compute full EV breakdown for a single option (§3.1).

    Parameters
    ----------
    entry_price : if provided, uses legged-in mode (§3.4). The equity entry
        price shifts the effective breakeven for covered puts/calls.
    """
    is_put = str(option_type).upper() in ("P", "PUT")
    opt_str = "P" if is_put else "C"

    # Price with appropriate model
    theo_price, model = price_option(S, K, T, r, sigma, q, opt_str)

    # Greeks
    g = greeks(S, K, T, r, sigma, q, opt_str)

    # Probabilities (§3.2) — smile-adjusted d2
    d1, d2 = _d1_d2(S, K, T, r, sigma, q)
    if is_put:
        p_expire_worthless = norm.cdf(d2)    # P(S_T > K)
        p_assignment = norm.cdf(-d2)         # P(S_T < K)
    else:
        p_expire_worthless = norm.cdf(-d2)   # P(S_T < K)
        p_assignment = norm.cdf(d2)          # P(S_T > K)

    # BSM Position EV (§3.1) — full probability-weighted distribution
    # σ (in d1, d2) is smile-adjusted IV derived from live market bid/ask.
    #
    # Covered put P&L at expiry:
    #   Assigned (S_T ≤ K):    (Entry - K) + Premium
    #   Not assigned (S_T > K): (Entry - S_T) + Premium
    #   Unified: P&L = Entry + Premium - max(K, S_T)
    #
    # E[P&L] = Entry + Premium - E[max(K, S_T)]
    # where  E[max(K, S_T)] = K × N(-d₂) + Fwd × N(d₁)
    #
    # Covered call (symmetric):
    #   E[P&L] = Premium + E[min(K, S_T)] - Entry
    #   where  E[min(K, S_T)] = K × N(d₂) + Fwd × N(-d₁)

    entry = entry_price if entry_price is not None else S
    n_d1 = norm.cdf(d1)
    fwd = S * math.exp((r - q) * T)

    if is_put:
        expected_close = K * p_assignment + fwd * n_d1
        ev_per_share = entry + bid - expected_close
        max_profit_ps = (entry - K) + bid
        directional_dist = S - K
    else:
        expected_close = K * p_assignment + fwd * (1 - n_d1)
        ev_per_share = bid + expected_close - entry
        max_profit_ps = (K - entry) + bid
        directional_dist = K - S

    premium_ratio = bid / directional_dist if directional_dist > 0 else None

    # P(Profit) — probability the combined position is profitable at expiry
    # Covered put breakeven:  S_T < Entry + Premium  → profit
    # Covered call breakeven: S_T > Entry - Premium  → profit
    if is_put:
        breakeven = entry + bid
    else:
        breakeven = entry - bid

    if T > 0 and sigma > 0:
        d1_be, d2_be = _d1_d2(S, breakeven, T, r, sigma, q)
        if is_put:
            p_profit = norm.cdf(-d2_be)  # P(S_T < breakeven) — stock stays below entry+premium
        else:
            p_profit = norm.cdf(d2_be)   # P(S_T > breakeven) — stock stays above entry-premium
    else:
        # At expiry: check deterministically
        p_profit = 1.0 if (is_put and S < breakeven) or (not is_put and S > breakeven) else 0.0

    # Position sizing
    margin_per_contract = K * 100 * 0.20  # Reg-T estimate (§7)
    contracts = max(1, int(capital / margin_per_contract)) if margin_per_contract > 0 else 1

    premium_collected = bid * 100 * contracts
    net_ev_per_contract = ev_per_share * 100
    net_ev_position = net_ev_per_contract * contracts

    # DTE
    dte = max(0, (expiry - date.today()).days)

    # Flags (§3.3, §7)
    flags = []
    mid = (bid + ask) / 2.0 if ask > 0 else bid
    if mid > 0 and (ask - bid) / mid > LIQUIDITY_SPREAD_THRESHOLD:
        flags.append("WIDE_SPREAD")
    if oi < LIQUIDITY_OI_THRESHOLD:
        flags.append("THIN_OI")
    if dte < 1 and S > 0 and abs(K - S) / S < GAMMA_PROXIMITY_PCT:
        flags.append("GAMMA_ELEVATED")

    return EVResult(
        strike=K,
        expiry=expiry,
        dte=dte,
        option_type=opt_str,
        bid=bid,
        ask=ask,
        mid=mid,
        iv=sigma,
        model_used=model,
        theoretical_price=theo_price,
        delta=g.delta,
        gamma=g.gamma,
        theta=g.theta,
        vega=g.vega,
        rho=g.rho,
        p_expire_worthless=p_expire_worthless,
        p_assignment=p_assignment,
        entry_used=entry,
        expected_close=expected_close,
        max_profit_per_share=max_profit_ps,
        p_profit=p_profit,
        premium_collected=premium_collected,
        net_ev_per_contract=net_ev_per_contract,
        net_ev_per_position=net_ev_position,
        contracts=contracts,
        nenner_score=nenner_score,
        spot=S,
        rate=r,
        div_yield=q,
        flags=flags,
        premium_ratio=premium_ratio,
        theta_per_share=g.theta,
    )


# ---------------------------------------------------------------------------
# §3.3 — Strike Ranking
# ---------------------------------------------------------------------------

def rank_strikes(
    results: list[EVResult],
    delta_cap: float = DEFAULT_DELTA_CAP,
    intent: str | Intent = "covered_put",
) -> list[EVResult]:
    """Rank EVResults by Net EV descending (§3.3).

    BEST/ALT are selected purely by EV — delta is shown as info only.
    Returns sorted list with BEST/ALTERNATE labels in flags.
    """
    # Sort by Net EV per contract descending — pure EV ranking
    ranked = sorted(results, key=lambda r: r.net_ev_per_contract, reverse=True)

    # Label top picks by EV regardless of delta
    if len(ranked) >= 1:
        ranked[0].flags.insert(0, "BEST")
    if len(ranked) >= 2:
        ranked[1].flags.insert(0, "ALTERNATE")

    return ranked


# ---------------------------------------------------------------------------
# §2.2 — 0DTE Gamma Warning
# ---------------------------------------------------------------------------

def gamma_warning(
    S: float, K: float, dte: int, sigma: float, T: float,
    contracts: int = 1,
) -> dict | None:
    """Generate adverse-move P&L display for 0DTE near-ATM (§2.2).

    Returns None if gamma warning doesn't apply.
    """
    if dte >= 1 or S <= 0:
        return None
    if abs(K - S) / S > GAMMA_PROXIMITY_PCT:
        return None

    # P&L impact of 0.5% and 1.0% adverse moves
    moves = {}
    for pct in (0.005, 0.01):
        adverse_S = S * (1.0 - pct)  # for short put, adverse = down
        intrinsic_at_adverse = max(K - adverse_S, 0.0)
        intrinsic_now = max(K - S, 0.0)
        loss_per_share = intrinsic_at_adverse - intrinsic_now
        loss_per_contract = loss_per_share * 100 * contracts
        moves[f"{pct*100:.1f}%"] = {
            "adverse_spot": round(adverse_S, 2),
            "loss_per_contract": round(loss_per_share * 100, 2),
            "total_loss": round(loss_per_contract, 2),
        }

    return {
        "warning": "Gamma Risk: ELEVATED",
        "spot": S,
        "strike": K,
        "proximity_pct": round(abs(K - S) / S * 100, 3),
        "adverse_moves": moves,
    }


# ---------------------------------------------------------------------------
# §7 — Margin Estimation
# ---------------------------------------------------------------------------

def estimate_margin(K: float, contracts: int = 1) -> float:
    """Conservative Reg-T margin estimate for short puts (§7).

    20% of underlying notional. Labeled as estimate — actual depends
    on broker and account type.
    """
    return K * 100 * 0.20 * contracts


# ---------------------------------------------------------------------------
# §5.4 — Implied Earnings Move
# ---------------------------------------------------------------------------

def implied_earnings_move(
    atm_call_mid: float,
    atm_put_mid: float,
    spot: float,
) -> float:
    """Compute the market-implied earnings move from ATM straddle (§5.4).

    Returns the implied move as a decimal (e.g., 0.032 = ±3.2%).
    """
    if spot <= 0:
        return 0.0
    return (atm_call_mid + atm_put_mid) / spot
