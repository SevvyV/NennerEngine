"""Tests for nenner_engine.llm_parser — focused on the JSON salvage helper."""

import json

from nenner_engine.llm_parser import _salvage_truncated_json


class TestSalvageTruncatedJson:
    def test_recovers_object_truncated_mid_value(self):
        # Truncated after an incomplete string value
        text = '{"signals": [{"ticker": "GC", "origin_price": 2650.0, "cancel'
        result = _salvage_truncated_json(text)
        assert result is not None
        assert "signals" in result
        # The partial object inside is dropped; signals array is closed
        assert isinstance(result["signals"], list)

    def test_recovers_object_truncated_after_comma(self):
        text = '{"signals": [{"ticker": "GC"},'
        result = _salvage_truncated_json(text)
        assert result is not None
        assert result["signals"] == [{"ticker": "GC"}]

    def test_recovers_object_truncated_mid_array(self):
        text = '{"signals": [{"ticker": "GC"}, {"ticker": "SI"}'
        result = _salvage_truncated_json(text)
        assert result is not None
        assert len(result["signals"]) == 2
        assert result["signals"][1]["ticker"] == "SI"

    def test_recovers_object_truncated_with_null_literal(self):
        text = '{"signals": [{"ticker": "GC", "origin_price": null'
        result = _salvage_truncated_json(text)
        assert result is not None
        assert "signals" in result

    def test_complete_json_roundtrips(self):
        original = {"signals": [{"ticker": "GC"}], "cycles": [], "price_targets": []}
        text = json.dumps(original)
        result = _salvage_truncated_json(text)
        assert result == original

    def test_returns_none_on_garbage(self):
        result = _salvage_truncated_json("not json at all <<<")
        assert result is None

    def test_stops_at_digit_not_at_letter_e(self):
        """Regression: previously the allowlist collapsed to just 'e', so
        the stripper only stopped at the letter 'e'. Now digits terminate too."""
        text = '{"signals": [{"ticker": "GC", "cancel_level": 2580'
        result = _salvage_truncated_json(text)
        assert result is not None
        # The trailing 2580 should be preserved, not stripped back to an 'e'
        assert result["signals"][0]["cancel_level"] == 2580
