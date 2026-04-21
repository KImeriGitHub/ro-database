import json
import pytest
from pathlib import Path

from response_schemas.schema_inferrer import (
    infer_schema,
    infer_schema_from_samples,
    save_schema,
    load_schema,
)


# ---------------------------------------------------------------------------
# Sample AV responses
# ---------------------------------------------------------------------------

DAILY_ADJUSTED_RESPONSE = {
    "Meta Data": {
        "1. Information": "Daily Time Series with Splits and Dividend Events",
        "2. Symbol": "AAPL",
        "3. Last Refreshed": "2024-06-14",
        "4. Output Size": "Compact",
        "5. Time Zone": "US/Eastern",
    },
    "Time Series (Daily)": {
        "2024-06-14": {
            "1. open": "212.4900",
            "2. high": "213.0700",
            "3. low": "211.6800",
            "4. close": "212.4900",
            "5. adjusted close": "212.4900",
            "6. volume": "40015923",
            "7. dividend amount": "0.0000",
            "8. split coefficient": "1.0000",
        },
        "2024-06-13": {
            "1. open": "214.7400",
            "2. high": "216.7500",
            "3. low": "211.6000",
            "4. close": "212.4900",
            "5. adjusted close": "212.4900",
            "6. volume": "52585971",
            "7. dividend amount": "0.0000",
            "8. split coefficient": "1.0000",
        },
        "2024-06-12": {
            "1. open": "213.0000",
            "2. high": "214.2400",
            "3. low": "211.6000",
            "4. close": "213.0700",
            "5. adjusted close": "213.0700",
            "6. volume": "54564882",
            "7. dividend amount": "0.0000",
            "8. split coefficient": "1.0000",
        },
        "2024-06-11": {
            "1. open": "210.3900",
            "2. high": "212.2000",
            "3. low": "209.5100",
            "4. close": "211.6800",
            "5. adjusted close": "211.6800",
            "6. volume": "42073673",
            "7. dividend amount": "0.0000",
            "8. split coefficient": "1.0000",
        },
    },
}

EARNINGS_RESPONSE = {
    "symbol": "AAPL",
    "annualEarnings": [
        {"fiscalDateEnding": "2024-09-30", "reportedEPS": "6.08"},
        {"fiscalDateEnding": "2023-09-30", "reportedEPS": "6.13"},
    ],
    "quarterlyEarnings": [
        {
            "fiscalDateEnding": "2024-06-30",
            "reportedDate": "2024-08-01",
            "reportedEPS": "1.40",
            "estimatedEPS": "1.35",
            "surprise": "0.05",
            "surprisePercentage": "3.7037",
        },
        {
            "fiscalDateEnding": "2024-03-31",
            "reportedDate": "2024-05-02",
            "reportedEPS": "1.53",
            "estimatedEPS": "1.50",
            "surprise": "0.03",
            "surprisePercentage": "2.0000",
        },
    ],
}

INCOME_STATEMENT_RESPONSE = {
    "symbol": "AAPL",
    "annualReports": [
        {
            "fiscalDateEnding": "2024-09-30",
            "reportedCurrency": "USD",
            "grossProfit": "180683000000",
            "totalRevenue": "391035000000",
            "operatingIncome": "123216000000",
            "netIncome": "93736000000",
        },
    ],
    "quarterlyReports": [
        {
            "fiscalDateEnding": "2024-06-30",
            "reportedCurrency": "USD",
            "grossProfit": "47328000000",
            "totalRevenue": "94930000000",
            "operatingIncome": "30462000000",
            "netIncome": "21448000000",
        },
    ],
}

COMMODITY_RESPONSE = {
    "name": "West Texas Intermediate",
    "interval": "monthly",
    "unit": "dollars per barrel",
    "data": [
        {"date": "2024-06-01", "value": "77.95"},
        {"date": "2024-05-01", "value": "79.72"},
        {"date": "2024-04-01", "value": "85.39"},
    ],
}


# ---------------------------------------------------------------------------
# Tests: infer_schema
# ---------------------------------------------------------------------------

class TestInferSchema:
    def test_daily_adjusted_top_level_keys(self):
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        assert schema["_type"] == "dict"
        assert "Meta Data" in schema["children"]
        assert "Time Series (Daily)" in schema["children"]

    def test_daily_adjusted_meta_data(self):
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        meta = schema["children"]["Meta Data"]
        assert meta["_type"] == "dict"
        assert "1. Information" in meta["children"]
        assert meta["children"]["1. Information"]["_type"] == "str"

    def test_daily_adjusted_dynamic_keys(self):
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        ts = schema["children"]["Time Series (Daily)"]
        assert ts["_type"] == "dict"
        assert ts["_dynamic_keys"] is True
        assert "*" in ts["children"]

    def test_daily_adjusted_wildcard_children(self):
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        wildcard = schema["children"]["Time Series (Daily)"]["children"]["*"]
        assert wildcard["_type"] == "dict"
        expected_keys = {
            "1. open", "2. high", "3. low", "4. close",
            "5. adjusted close", "6. volume",
            "7. dividend amount", "8. split coefficient",
        }
        assert set(wildcard["children"].keys()) == expected_keys
        for child in wildcard["children"].values():
            assert child["_type"] == "str"

    def test_earnings_lists(self):
        schema = infer_schema(EARNINGS_RESPONSE)
        assert schema["children"]["symbol"]["_type"] == "str"
        annual = schema["children"]["annualEarnings"]
        assert annual["_type"] == "list"
        assert annual["element"]["_type"] == "dict"
        assert "fiscalDateEnding" in annual["element"]["children"]

    def test_commodity_structure(self):
        schema = infer_schema(COMMODITY_RESPONSE)
        assert schema["children"]["name"]["_type"] == "str"
        assert schema["children"]["data"]["_type"] == "list"
        elem = schema["children"]["data"]["element"]
        assert "date" in elem["children"]
        assert "value" in elem["children"]

    def test_empty_dict(self):
        schema = infer_schema({})
        assert schema == {"_type": "dict", "children": {}}

    def test_empty_list_element(self):
        schema = infer_schema({"items": []})
        assert schema["children"]["items"]["element"]["_type"] == "unknown"

    def test_primitive_types(self):
        schema = infer_schema({"s": "hi", "i": 42, "f": 3.14, "b": True, "n": None})
        assert schema["children"]["s"]["_type"] == "str"
        assert schema["children"]["i"]["_type"] == "int"
        assert schema["children"]["f"]["_type"] == "float"
        assert schema["children"]["b"]["_type"] == "bool"
        assert schema["children"]["n"]["_type"] == "null"

    def test_nested_dicts(self):
        data = {"a": {"b": {"c": "deep"}}}
        schema = infer_schema(data)
        assert schema["children"]["a"]["children"]["b"]["children"]["c"]["_type"] == "str"


# ---------------------------------------------------------------------------
# Tests: infer_schema_from_samples
# ---------------------------------------------------------------------------

class TestInferSchemaFromSamples:
    def test_optional_key_detection(self):
        """A key present in one sample but not the other becomes optional."""
        s1 = {"a": "x", "b": "y"}
        s2 = {"a": "x"}
        schema = infer_schema_from_samples([s1, s2])
        assert schema["children"]["a"].get("_optional") is None  # present in both
        assert schema["children"]["b"]["_optional"] is True

    def test_single_sample(self):
        schema = infer_schema_from_samples([{"a": 1}])
        assert schema["children"]["a"]["_type"] == "int"

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            infer_schema_from_samples([])

    def test_null_and_value_merge(self):
        """A field that is null in some samples and str in others becomes optional str."""
        s1 = {"x": "hello"}
        s2 = {"x": None}
        schema = infer_schema_from_samples([s1, s2])
        assert schema["children"]["x"]["_type"] == "str"
        assert schema["children"]["x"]["_optional"] is True

    def test_dynamic_keys_merge(self):
        """Two dynamic-key dicts merge their wildcard children."""
        s1 = {"ts": {"2024-01-01": {"a": "1"}, "2024-01-02": {"a": "2"}, "2024-01-03": {"a": "3"}}}
        s2 = {"ts": {"2024-02-01": {"a": "4"}, "2024-02-02": {"a": "5"}, "2024-02-03": {"a": "6"}}}
        schema = infer_schema_from_samples([s1, s2])
        ts = schema["children"]["ts"]
        assert ts["_dynamic_keys"] is True
        assert ts["children"]["*"]["children"]["a"]["_type"] == "str"


# ---------------------------------------------------------------------------
# Tests: save_schema / load_schema round-trip
# ---------------------------------------------------------------------------

class TestSaveLoadSchema:
    def test_round_trip(self, tmp_path):
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        save_schema(schema, "TEST_DAILY", schemas_dir=str(tmp_path))
        loaded = load_schema("TEST_DAILY", schemas_dir=str(tmp_path))
        assert loaded == schema

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_schema("NONEXISTENT", schemas_dir=str(tmp_path))

    def test_round_trip_earnings(self, tmp_path):
        schema = infer_schema(EARNINGS_RESPONSE)
        save_schema(schema, "EARNINGS", schemas_dir=str(tmp_path))
        loaded = load_schema("EARNINGS", schemas_dir=str(tmp_path))
        assert loaded == schema
