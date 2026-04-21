import pytest

from response_schemas.schema_inferrer import infer_schema
from response_schemas.schema_validator import validate_response


# ---------------------------------------------------------------------------
# Fixtures — reuse the same sample responses from inferrer tests
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
    ],
}


# ---------------------------------------------------------------------------
# Tests: valid responses pass
# ---------------------------------------------------------------------------

class TestValidResponsesPass:
    def test_daily_adjusted_valid(self):
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        violations = validate_response(DAILY_ADJUSTED_RESPONSE, schema)
        assert violations == []

    def test_earnings_valid(self):
        schema = infer_schema(EARNINGS_RESPONSE)
        violations = validate_response(EARNINGS_RESPONSE, schema)
        assert violations == []

    def test_dynamic_keys_different_dates(self):
        """A response with different dates should still pass the schema."""
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        # New response with different dates but same structure
        new_response = {
            "Meta Data": {
                "1. Information": "Daily Time Series with Splits and Dividend Events",
                "2. Symbol": "MSFT",
                "3. Last Refreshed": "2024-07-01",
                "4. Output Size": "Compact",
                "5. Time Zone": "US/Eastern",
            },
            "Time Series (Daily)": {
                "2024-07-01": {
                    "1. open": "450.00",
                    "2. high": "452.00",
                    "3. low": "448.00",
                    "4. close": "451.00",
                    "5. adjusted close": "451.00",
                    "6. volume": "20000000",
                    "7. dividend amount": "0.0000",
                    "8. split coefficient": "1.0000",
                },
                "2024-06-28": {
                    "1. open": "448.00",
                    "2. high": "450.00",
                    "3. low": "447.00",
                    "4. close": "449.50",
                    "5. adjusted close": "449.50",
                    "6. volume": "18000000",
                    "7. dividend amount": "0.0000",
                    "8. split coefficient": "1.0000",
                },
                "2024-06-27": {
                    "1. open": "446.00",
                    "2. high": "449.00",
                    "3. low": "445.00",
                    "4. close": "448.00",
                    "5. adjusted close": "448.00",
                    "6. volume": "19000000",
                    "7. dividend amount": "0.0000",
                    "8. split coefficient": "1.0000",
                },
            },
        }
        violations = validate_response(new_response, schema)
        assert violations == []


# ---------------------------------------------------------------------------
# Tests: missing keys are tolerated (structural validation only)
# ---------------------------------------------------------------------------

class TestMissingKeys:
    def test_missing_meta_data_keys_no_violation(self):
        """Missing keys in a sub-dict should not produce violations."""
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        bad = {
            "Meta Data": {
                "1. Information": "test",
                # missing keys 2-5 — that's fine
            },
            "Time Series (Daily)": {
                "2024-06-14": {
                    "1. open": "100",
                    "2. high": "101",
                    "3. low": "99",
                    "4. close": "100",
                    "5. adjusted close": "100",
                    "6. volume": "1000",
                    "7. dividend amount": "0",
                    "8. split coefficient": "1",
                },
                "2024-06-13": {
                    "1. open": "100",
                    "2. high": "101",
                    "3. low": "99",
                    "4. close": "100",
                    "5. adjusted close": "100",
                    "6. volume": "1000",
                    "7. dividend amount": "0",
                    "8. split coefficient": "1",
                },
                "2024-06-12": {
                    "1. open": "100",
                    "2. high": "101",
                    "3. low": "99",
                    "4. close": "100",
                    "5. adjusted close": "100",
                    "6. volume": "1000",
                    "7. dividend amount": "0",
                    "8. split coefficient": "1",
                },
            },
        }
        violations = validate_response(bad, schema)
        assert violations == []

    def test_missing_top_level_key_no_violation(self):
        """Missing a top-level key should not produce violations."""
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        bad = {
            "Meta Data": {
                "1. Information": "test",
                "2. Symbol": "AAPL",
                "3. Last Refreshed": "2024-06-14",
                "4. Output Size": "Compact",
                "5. Time Zone": "US/Eastern",
            },
            # missing "Time Series (Daily)" — that's fine
        }
        violations = validate_response(bad, schema)
        assert violations == []


# ---------------------------------------------------------------------------
# Tests: wrong types
# ---------------------------------------------------------------------------

class TestWrongTypes:
    def test_float_instead_of_str(self):
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        bad = {
            "Meta Data": {
                "1. Information": "test",
                "2. Symbol": "AAPL",
                "3. Last Refreshed": "2024-06-14",
                "4. Output Size": "Compact",
                "5. Time Zone": "US/Eastern",
            },
            "Time Series (Daily)": {
                "2024-06-14": {
                    "1. open": 212.49,  # float instead of str
                    "2. high": "213.07",
                    "3. low": "211.68",
                    "4. close": "212.49",
                    "5. adjusted close": "212.49",
                    "6. volume": "40015923",
                    "7. dividend amount": "0.0000",
                    "8. split coefficient": "1.0000",
                },
                "2024-06-13": {
                    "1. open": "214.74",
                    "2. high": "216.75",
                    "3. low": "211.60",
                    "4. close": "212.49",
                    "5. adjusted close": "212.49",
                    "6. volume": "52585971",
                    "7. dividend amount": "0.0000",
                    "8. split coefficient": "1.0000",
                },
                "2024-06-12": {
                    "1. open": "213.00",
                    "2. high": "214.24",
                    "3. low": "211.60",
                    "4. close": "213.07",
                    "5. adjusted close": "213.07",
                    "6. volume": "54564882",
                    "7. dividend amount": "0.0000",
                    "8. split coefficient": "1.0000",
                },
            },
        }
        violations = validate_response(bad, schema)
        assert any("expected str, got float" in v for v in violations)

    def test_int_where_str_expected(self):
        schema = infer_schema(EARNINGS_RESPONSE)
        bad = {
            "symbol": 123,  # int instead of str
            "annualEarnings": [],
            "quarterlyEarnings": [],
        }
        violations = validate_response(bad, schema)
        assert any("expected str, got int" in v for v in violations)


# ---------------------------------------------------------------------------
# Tests: unexpected keys
# ---------------------------------------------------------------------------

class TestUnexpectedKeys:
    def test_extra_top_level_key(self):
        schema = infer_schema(EARNINGS_RESPONSE)
        bad = {
            "symbol": "AAPL",
            "annualEarnings": [],
            "quarterlyEarnings": [],
            "extraField": "surprise",
        }
        violations = validate_response(bad, schema)
        assert any("unexpected keys" in v and "extraField" in v for v in violations)


# ---------------------------------------------------------------------------
# Tests: AV error responses
# ---------------------------------------------------------------------------

class TestErrorResponses:
    def test_av_error_detected(self):
        """An AV error response should fail validation against a normal schema."""
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        error_response = {
            "Error Message": "Invalid API call. Please retry or visit the documentation."
        }
        violations = validate_response(error_response, schema)
        assert len(violations) > 0

    def test_av_rate_limit_detected(self):
        schema = infer_schema(DAILY_ADJUSTED_RESPONSE)
        rate_limit_response = {
            "Note": "Thank you for using Alpha Vantage! API call frequency exceeded."
        }
        violations = validate_response(rate_limit_response, schema)
        assert len(violations) > 0


# ---------------------------------------------------------------------------
# Tests: optional fields
# ---------------------------------------------------------------------------

class TestOptionalFields:
    def test_optional_field_absent_passes(self):
        """A field marked _optional should not cause a violation when absent."""
        schema = {
            "_type": "dict",
            "children": {
                "required_key": {"_type": "str"},
                "optional_key": {"_type": "str", "_optional": True},
            },
        }
        response = {"required_key": "present"}
        violations = validate_response(response, schema)
        # Should only flag the unexpected-keys issue if strict, but not missing-required
        missing = [v for v in violations if "missing required key" in v]
        assert not any("optional_key" in v for v in missing)

    def test_optional_null_passes(self):
        schema = {
            "_type": "dict",
            "children": {
                "field": {"_type": "str", "_optional": True},
            },
        }
        response = {"field": None}
        violations = validate_response(response, schema)
        assert violations == []
