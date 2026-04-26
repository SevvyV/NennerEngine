"""Tests for prices module — price aggregation, NaN/Inf filtering, source priority.

This file targets the gaps the audit flagged: yfinance never had unit
coverage, NaN/Inf handling was implicit, and the get_current_prices
priority (DataBento → yfinance → DB cache) had no test pinning the
ordering. Today's hallucinated AAPL/GOOGL incident lived in this layer,
so the test surface earns its keep.
"""

import math
import time as _time
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd

from conftest import make_test_db
from nenner_engine import prices as prices_mod


class TestYfDownloadWithTimeout(unittest.TestCase):
    """The thread-bounded yfinance wrapper added in Phase 2a."""

    def test_returns_dataframe_on_success(self):
        df_in = pd.DataFrame({"Close": [100.0, 101.0]})
        with patch("yfinance.download", return_value=df_in):
            df_out = prices_mod._yf_download_with_timeout(["AAPL"], period="1d")
        self.assertIs(df_out, df_in)

    def test_raises_timeout_when_thread_doesnt_finish(self):
        """If yfinance hangs past the timeout budget, raise TimeoutError so
        the caller can fall back to cached data instead of being blocked."""
        # Use a short fake-timeout and patch yfinance to sleep longer.
        def slow(*a, **kw):
            _time.sleep(2.0)
            return pd.DataFrame()
        with patch("yfinance.download", side_effect=slow):
            with self.assertRaises(TimeoutError):
                prices_mod._yf_download_with_timeout(
                    ["AAPL"], period="1d", timeout=0.3,
                )

    def test_propagates_underlying_exception(self):
        """A network error inside yfinance must surface — not get swallowed
        as a timeout, since the caller's fallback path differs."""
        with patch("yfinance.download", side_effect=ValueError("yahoo 503")):
            with self.assertRaises(ValueError):
                prices_mod._yf_download_with_timeout(["AAPL"], period="1d")


class TestFetchYfinanceDailyFiltering(unittest.TestCase):
    """fetch_yfinance_daily must drop NaN / Inf / non-positive prices
    before they hit price_history. Today's AAPL=$136.88 incident proved
    we cannot trust upstream feeds blindly."""

    def setUp(self):
        self.conn = make_test_db()

    def tearDown(self):
        self.conn.close()

    def _stored(self, ticker: str) -> list[tuple]:
        return self.conn.execute(
            "SELECT date, close FROM price_history "
            "WHERE ticker = ? AND source = 'yfinance' ORDER BY date",
            (ticker,),
        ).fetchall()

    def _build_yf_df(self, rows: dict[str, list[tuple[str, float]]]) -> pd.DataFrame:
        """Build a yfinance-style DataFrame: MultiIndex columns (Price, Ticker)."""
        all_dates = sorted({d for r in rows.values() for d, _ in r})
        idx = pd.DatetimeIndex([pd.Timestamp(d) for d in all_dates])
        cols = pd.MultiIndex.from_product([["Close"], list(rows.keys())])
        data = {}
        for ticker, series in rows.items():
            d = dict(series)
            data[("Close", ticker)] = [d.get(date_str, float("nan")) for date_str in all_dates]
        return pd.DataFrame(data, index=idx, columns=cols)

    def test_nan_prices_are_dropped(self):
        df = self._build_yf_df({
            "AAPL=F": [("2026-04-22", 270.0), ("2026-04-23", float("nan")),
                       ("2026-04-24", 271.0)],
        })
        with patch.object(prices_mod, "_yf_download_with_timeout", return_value=df), \
             patch.dict(prices_mod.YFINANCE_MAP, {"AAPL": "AAPL=F"}, clear=False):
            prices_mod.fetch_yfinance_daily(self.conn, tickers=["AAPL"], period="5d")
        rows = self._stored("AAPL")
        self.assertEqual(len(rows), 2,
                         "NaN row must be filtered before DB write")
        for _, close in rows:
            self.assertTrue(math.isfinite(close))

    def test_infinite_prices_are_dropped(self):
        df = self._build_yf_df({
            "AAPL=F": [("2026-04-22", 270.0), ("2026-04-23", float("inf"))],
        })
        with patch.object(prices_mod, "_yf_download_with_timeout", return_value=df), \
             patch.dict(prices_mod.YFINANCE_MAP, {"AAPL": "AAPL=F"}, clear=False):
            prices_mod.fetch_yfinance_daily(self.conn, tickers=["AAPL"], period="5d")
        for _, close in self._stored("AAPL"):
            self.assertTrue(math.isfinite(close))

    def test_non_positive_prices_are_dropped(self):
        """Zero or negative prints are bad data for our universe (stocks +
        ETFs + most futures). The April 2020 negative-WTI scenario is the
        single accepted casualty — we'd rather drop a once-in-a-century
        edge case than let zero leak into divide-by-zero math elsewhere."""
        df = self._build_yf_df({
            "AAPL=F": [("2026-04-22", 270.0), ("2026-04-23", 0.0),
                       ("2026-04-24", -1.5)],
        })
        with patch.object(prices_mod, "_yf_download_with_timeout", return_value=df), \
             patch.dict(prices_mod.YFINANCE_MAP, {"AAPL": "AAPL=F"}, clear=False):
            prices_mod.fetch_yfinance_daily(self.conn, tickers=["AAPL"], period="5d")
        rows = self._stored("AAPL")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 270.0)

    def test_timeout_returns_empty_dict_without_raising(self):
        """A yfinance timeout must NOT propagate — the caller (auto_cancel,
        scheduler) needs to keep going on stale data, not crash."""
        with patch.object(
            prices_mod, "_yf_download_with_timeout",
            side_effect=TimeoutError("yfinance.download exceeded 30s"),
        ):
            result = prices_mod.fetch_yfinance_daily(self.conn, tickers=["AAPL"])
        self.assertEqual(result, {})


class TestFetchYfCached(unittest.TestCase):
    """The 5-minute TTL cache used by get_current_prices."""

    def setUp(self):
        # Reset cache state so tests don't leak into each other.
        prices_mod._yf_cache = {}
        prices_mod._yf_cache_time = 0.0

    def tearDown(self):
        prices_mod._yf_cache = {}
        prices_mod._yf_cache_time = 0.0

    def test_cache_hit_skips_network_call(self):
        prices_mod._yf_cache = {"AAPL": 270.0}
        prices_mod._yf_cache_time = _time.monotonic()
        with patch.object(prices_mod, "_yf_download_with_timeout") as m:
            out = prices_mod._fetch_yf_cached()
        m.assert_not_called()
        self.assertEqual(out["AAPL"], 270.0)

    def test_timeout_returns_old_cache(self):
        """If the TTL has expired but yfinance times out, return whatever
        cache we still have rather than an empty dict — stale prices beat
        no prices for the dashboard."""
        prices_mod._yf_cache = {"AAPL": 268.5}
        prices_mod._yf_cache_time = _time.monotonic() - 999  # expired
        with patch.object(
            prices_mod, "_yf_download_with_timeout",
            side_effect=TimeoutError("yahoo down"),
        ):
            out = prices_mod._fetch_yf_cached()
        self.assertEqual(out, {"AAPL": 268.5})


class TestGetCurrentPricesPriority(unittest.TestCase):
    """get_current_prices source priority: DataBento → yfinance → DB cache."""

    def setUp(self):
        self.conn = make_test_db()
        prices_mod._yf_cache = {}
        prices_mod._yf_cache_time = 0.0

    def tearDown(self):
        self.conn.close()
        prices_mod._yf_cache = {}
        prices_mod._yf_cache_time = 0.0

    def _seed_databento(self, ticker: str, price: float, age_seconds: int = 30):
        """Write a fresh DataBento row that should win over yfinance."""
        # The freshness check uses fetched_at compared against UTC now.
        from datetime import UTC
        ts = (datetime.now(UTC) - timedelta(seconds=age_seconds)).strftime(
            "%Y-%m-%d %H:%M:%S",
        )
        self.conn.execute(
            "INSERT INTO price_history "
            "(ticker, date, close, source, fetched_at) VALUES (?, ?, ?, ?, ?)",
            (ticker, datetime.now(UTC).strftime("%Y-%m-%d"),
             price, "DATABENTO_EQUITY", ts),
        )
        self.conn.commit()

    def test_databento_wins_over_yfinance(self):
        self._seed_databento("AAPL", 270.50, age_seconds=10)
        with patch.object(prices_mod, "_fetch_yf_cached",
                          return_value={"AAPL": 999.0}):
            out = prices_mod.get_current_prices(self.conn, ["AAPL"])
        self.assertEqual(out["AAPL"]["price"], 270.50)
        self.assertEqual(out["AAPL"]["source"], "DATABENTO_EQUITY")

    def test_stale_databento_is_ignored(self):
        """DataBento rows older than 10 minutes should fall through to
        yfinance — otherwise overnight stale midpoints would be served as
        live data."""
        self._seed_databento("AAPL", 270.50, age_seconds=15 * 60)
        with patch.object(prices_mod, "_fetch_yf_cached",
                          return_value={"AAPL": 271.0}):
            out = prices_mod.get_current_prices(self.conn, ["AAPL"])
        self.assertEqual(out["AAPL"]["price"], 271.0)
        self.assertEqual(out["AAPL"]["source"], "yfinance")

    def test_yfinance_fills_databento_gaps(self):
        """Tickers with no DataBento data (futures, FX) must still get a
        price via yfinance — DataBento equity stream only covers cash
        equities and ETFs."""
        with patch.object(prices_mod, "_fetch_yf_cached",
                          return_value={"GC": 4730.0}):
            out = prices_mod.get_current_prices(self.conn, ["GC"])
        self.assertEqual(out["GC"]["price"], 4730.0)
        self.assertEqual(out["GC"]["source"], "yfinance")

    def test_db_cache_used_when_both_feeds_silent(self):
        """If neither DataBento nor yfinance return data, fall back to the
        most recent cached close in price_history."""
        self.conn.execute(
            "INSERT INTO price_history (ticker, date, close, source) "
            "VALUES ('AAPL', '2026-04-22', 268.0, 'yfinance_backfill')",
        )
        self.conn.commit()
        with patch.object(prices_mod, "_fetch_yf_cached", return_value={}):
            out = prices_mod.get_current_prices(self.conn, ["AAPL"])
        self.assertEqual(out["AAPL"]["price"], 268.0)
        self.assertEqual(out["AAPL"]["source"], "yfinance_backfill")

    def test_databento_alias_googl_matches_goog(self):
        """The DataBento equity stream uses GOOGL; our canonical ticker is
        GOOG. The alias map must surface DataBento's GOOGL row when the
        caller asks for GOOG."""
        self._seed_databento("GOOGL", 343.25, age_seconds=10)
        with patch.object(prices_mod, "_fetch_yf_cached", return_value={}):
            out = prices_mod.get_current_prices(self.conn, ["GOOG"])
        self.assertIn("GOOG", out)
        self.assertEqual(out["GOOG"]["price"], 343.25)
        self.assertEqual(out["GOOG"]["source"], "DATABENTO_EQUITY")


if __name__ == "__main__":
    unittest.main()
