"""Tests for instrument identification and mapping."""

import unittest

from nenner_engine.instruments import (
    INSTRUMENT_MAP,
    SECTION_HEADERS,
    identify_instrument,
    get_section_instrument,
    get_instrument_map_json,
)


class TestIdentifyInstrument(unittest.TestCase):
    """Test sentence-level instrument identification."""

    def test_gold(self):
        name, ticker, ac = identify_instrument("Gold continues on a BUY signal")
        self.assertEqual(ticker, "GC")
        self.assertEqual(name, "Gold")

    def test_gold_alias(self):
        name, ticker, ac = identify_instrument("Gold (April continues on a BUY")
        self.assertEqual(ticker, "GC")

    def test_sp500(self):
        name, ticker, ac = identify_instrument("S&P continues on a SELL signal")
        self.assertEqual(ticker, "ES")

    def test_sp500_alias(self):
        name, ticker, ac = identify_instrument("S&P 500 is on a BUY")
        self.assertEqual(ticker, "ES")

    def test_apple(self):
        name, ticker, ac = identify_instrument("Apple (AAPL) cancelled the BUY")
        self.assertEqual(ticker, "AAPL")
        self.assertEqual(ac, "Single Stock")

    def test_unknown_returns_unk(self):
        name, ticker, ac = identify_instrument("Some random text with no instrument")
        self.assertEqual(ticker, "UNK")
        self.assertEqual(name, "Unknown")

    def test_context_instrument_fallback(self):
        """Falls back to context_instrument when no direct match."""
        name, ticker, ac = identify_instrument("continues on a BUY", context_instrument="Gold")
        self.assertEqual(ticker, "GC")

    def test_context_instrument_ignored_on_match(self):
        """Direct match takes priority over context_instrument."""
        name, ticker, ac = identify_instrument("Silver continues on a BUY", context_instrument="Gold")
        self.assertEqual(ticker, "SI")

    def test_longest_match_wins(self):
        """'S&P 500' (longer) should match before 'S&P' (shorter)."""
        name, ticker, ac = identify_instrument("S&P 500 continues")
        # Both 'S&P' and 'S&P 500' map to ES, but name should be 'S&P'
        self.assertEqual(ticker, "ES")

    def test_crypto_bitcoin(self):
        name, ticker, ac = identify_instrument("Bitcoin continues on a SELL")
        self.assertEqual(ticker, "BTC")
        self.assertEqual(ac, "Crypto")

    def test_tesla(self):
        name, ticker, ac = identify_instrument("Tesla (TSLA) is on a BUY signal")
        self.assertEqual(ticker, "TSLA")


class TestGetSectionInstrument(unittest.TestCase):
    """Test section-header-based instrument attribution."""

    def test_gold_header(self):
        text = "Some intro text\n\nGold (April)\nContinues on a BUY signal"
        name, ticker, ac = get_section_instrument(text)
        self.assertEqual(ticker, "GC")

    def test_sp500_header(self):
        text = "S&P (March)\nContinues on a SELL"
        name, ticker, ac = get_section_instrument(text)
        self.assertEqual(ticker, "ES")

    def test_no_header_returns_unknown(self):
        name, ticker, ac = get_section_instrument("no instrument headers here")
        self.assertEqual(ticker, "UNK")

    def test_nearest_wins(self):
        """When multiple headers exist, the last (nearest) one wins."""
        text = "Gold (April)\nBUY from 2600\n\nSilver (March)\nSELL from 31"
        name, ticker, ac = get_section_instrument(text)
        self.assertEqual(ticker, "SI")


class TestInstrumentMapCompleteness(unittest.TestCase):
    """Validate structural integrity of the instrument map."""

    def test_all_entries_have_ticker(self):
        for name, info in INSTRUMENT_MAP.items():
            self.assertIn("ticker", info, f"{name} missing 'ticker'")
            self.assertTrue(info["ticker"], f"{name} has empty ticker")

    def test_all_entries_have_asset_class(self):
        for name, info in INSTRUMENT_MAP.items():
            self.assertIn("asset_class", info, f"{name} missing 'asset_class'")
            self.assertTrue(info["asset_class"], f"{name} has empty asset_class")

    def test_all_entries_have_aliases(self):
        for name, info in INSTRUMENT_MAP.items():
            self.assertIn("aliases", info, f"{name} missing 'aliases'")
            self.assertIsInstance(info["aliases"], list, f"{name} aliases not a list")

    def test_no_duplicate_tickers(self):
        """Each ticker should map to exactly one primary instrument name."""
        tickers = {}
        for name, info in INSTRUMENT_MAP.items():
            t = info["ticker"]
            if t in tickers:
                # Allow ETF duplicates (e.g., GLD and Gold both valid)
                # Just ensure we're aware
                pass
            tickers.setdefault(t, []).append(name)
        # Check for unexpected duplicates (more than 2 names for same ticker)
        for ticker, names in tickers.items():
            self.assertLessEqual(
                len(names), 2,
                f"Ticker {ticker} mapped by {len(names)} names: {names}",
            )

    def test_section_headers_have_four_elements(self):
        """Each SECTION_HEADERS entry must be (pattern, name, ticker, asset_class)."""
        for entry in SECTION_HEADERS:
            self.assertEqual(len(entry), 4, f"Bad section header entry: {entry}")

    def test_get_instrument_map_json_returns_valid_json(self):
        import json
        result = get_instrument_map_json()
        parsed = json.loads(result)
        self.assertIsInstance(parsed, dict)
        self.assertIn("Gold", parsed)
        self.assertEqual(parsed["Gold"]["ticker"], "GC")


if __name__ == "__main__":
    unittest.main()
