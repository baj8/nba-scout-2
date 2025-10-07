"""Tests for centralized coercion utilities."""

from nba_scraper.utils.coerce import (
    _is_null,
    safe_divide,
    safe_percentage,
    to_bool_or_none,
    to_float_or_none,
    to_int_or_none,
    to_str_or_none,
)


class TestNullDetection:
    """Test null/empty value detection."""

    def test_is_null_with_none(self):
        """Test None values are detected as null."""
        assert _is_null(None) is True

    def test_is_null_with_empty_string(self):
        """Test empty strings are detected as null."""
        assert _is_null("") is True
        assert _is_null("   ") is True  # Whitespace only

    def test_is_null_with_common_null_strings(self):
        """Test common null string representations."""
        null_values = ["-", "—", "N/A", "NA", "null", "NULL", "None", "NONE", "--"]
        for val in null_values:
            assert _is_null(val) is True, f"Expected {val} to be detected as null"

    def test_is_null_with_valid_values(self):
        """Test valid values are not detected as null."""
        valid_values = ["0", "false", "no", "123", "hello"]
        for val in valid_values:
            assert _is_null(val) is False, f"Expected {val} to NOT be detected as null"


class TestIntegerCoercion:
    """Test integer conversion with to_int_or_none."""

    def test_null_values_return_none(self):
        """Test that null values return None."""
        null_inputs = [None, "", "-", "—", "N/A", "NA", "null"]
        for val in null_inputs:
            assert to_int_or_none(val) is None, f"Expected {val} to return None"

    def test_valid_integers(self):
        """Test conversion of valid integer values."""
        test_cases = [
            (42, 42),
            ("42", 42),
            ("0", 0),
            ("-5", -5),
            ("  123  ", 123),  # With whitespace
        ]
        for input_val, expected in test_cases:
            assert to_int_or_none(input_val) == expected

    def test_comma_separated_numbers(self):
        """Test conversion of comma-separated numbers."""
        test_cases = [
            ("1,234", 1234),
            ("1,234,567", 1234567),
            ("12,345", 12345),
        ]
        for input_val, expected in test_cases:
            assert to_int_or_none(input_val) == expected

    def test_float_strings(self):
        """Test conversion of float strings to integers."""
        test_cases = [
            ("12.0", 12),
            ("42.00", 42),
            ("123.5", 123),  # Truncates decimal
            ("-5.9", -5),
        ]
        for input_val, expected in test_cases:
            assert to_int_or_none(input_val) == expected

    def test_invalid_values_return_none(self):
        """Test that invalid values return None."""
        invalid_inputs = ["abc", "12.34.56", "not_a_number", "∞", "NaN"]
        for val in invalid_inputs:
            assert to_int_or_none(val) is None, f"Expected {val} to return None"


class TestFloatCoercion:
    """Test float conversion with to_float_or_none."""

    def test_null_values_return_none(self):
        """Test that null values return None."""
        null_inputs = [None, "", "-", "—", "N/A", "NA", "null"]
        for val in null_inputs:
            assert to_float_or_none(val) is None, f"Expected {val} to return None"

    def test_valid_floats(self):
        """Test conversion of valid float values."""
        test_cases = [
            (42.5, 42.5),
            ("42.5", 42.5),
            ("0.0", 0.0),
            ("-5.25", -5.25),
            ("  123.45  ", 123.45),  # With whitespace
            ("42", 42.0),  # Integer to float
        ]
        for input_val, expected in test_cases:
            assert to_float_or_none(input_val) == expected

    def test_comma_separated_numbers(self):
        """Test conversion of comma-separated decimal numbers."""
        test_cases = [
            ("1,234.56", 1234.56),
            ("1,234,567.89", 1234567.89),
            ("12,345", 12345.0),
        ]
        for input_val, expected in test_cases:
            assert to_float_or_none(input_val) == expected

    def test_percentage_strings(self):
        """Test conversion of percentage strings."""
        test_cases = [
            ("45.6%", 45.6),
            ("100%", 100.0),
            ("0.5%", 0.5),
            ("12.34%", 12.34),
        ]
        for input_val, expected in test_cases:
            assert to_float_or_none(input_val) == expected

    def test_invalid_values_return_none(self):
        """Test that invalid values return None."""
        invalid_inputs = ["abc", "12.34.56", "not_a_number", "∞", "NaN"]
        for val in invalid_inputs:
            assert to_float_or_none(val) is None, f"Expected {val} to return None"


class TestStringCoercion:
    """Test string conversion with to_str_or_none."""

    def test_null_values_return_none(self):
        """Test that null values return None."""
        null_inputs = [None, "", "-", "—", "N/A", "NA", "null"]
        for val in null_inputs:
            assert to_str_or_none(val) is None, f"Expected {val} to return None"

    def test_valid_strings(self):
        """Test conversion of valid string values."""
        test_cases = [
            ("hello", "hello"),
            ("  world  ", "world"),  # Strips whitespace
            ("123", "123"),
            ("0", "0"),
            ("false", "false"),
        ]
        for input_val, expected in test_cases:
            assert to_str_or_none(input_val) == expected

    def test_non_string_conversion(self):
        """Test conversion of non-string types."""
        test_cases = [
            (123, "123"),
            (45.6, "45.6"),
            (True, "True"),
            (False, "False"),
        ]
        for input_val, expected in test_cases:
            assert to_str_or_none(input_val) == expected


class TestBooleanCoercion:
    """Test boolean conversion with to_bool_or_none."""

    def test_null_values_return_none(self):
        """Test that null values return None."""
        null_inputs = [None, "", "-", "—", "N/A", "NA", "null"]
        for val in null_inputs:
            assert to_bool_or_none(val) is None, f"Expected {val} to return None"

    def test_true_values(self):
        """Test values that should convert to True."""
        true_inputs = [True, "true", "True", "TRUE", "1", 1, "yes", "YES", "y", "Y"]
        for val in true_inputs:
            assert to_bool_or_none(val) is True, f"Expected {val} to return True"

    def test_false_values(self):
        """Test values that should convert to False."""
        false_inputs = [False, "false", "False", "FALSE", "0", 0, "no", "NO", "n", "N"]
        for val in false_inputs:
            assert to_bool_or_none(val) is False, f"Expected {val} to return False"

    def test_invalid_values_return_none(self):
        """Test that invalid boolean values return None."""
        invalid_inputs = ["maybe", "2", "abc", "invalid"]
        for val in invalid_inputs:
            assert to_bool_or_none(val) is None, f"Expected {val} to return None"


class TestSafeDivision:
    """Test safe division operations."""

    def test_valid_division(self):
        """Test valid division operations."""
        test_cases = [
            (10, 2, 5.0),
            ("10", "2", 5.0),
            (15.5, 3, 5.166666666666667),
            ("100", "4", 25.0),
        ]
        for num, den, expected in test_cases:
            result = safe_divide(num, den)
            assert abs(result - expected) < 1e-10, f"Expected {expected}, got {result}"

    def test_division_by_zero_returns_none(self):
        """Test that division by zero returns None."""
        assert safe_divide(10, 0) is None
        assert safe_divide("10", "0") is None
        assert safe_divide(5.5, 0.0) is None

    def test_null_values_return_none(self):
        """Test that null values in numerator or denominator return None."""
        assert safe_divide(None, 5) is None
        assert safe_divide(10, None) is None
        assert safe_divide("-", "5") is None
        assert safe_divide("10", "N/A") is None

    def test_invalid_values_return_none(self):
        """Test that invalid numeric values return None."""
        assert safe_divide("abc", 5) is None
        assert safe_divide(10, "xyz") is None


class TestSafePercentage:
    """Test safe percentage calculations."""

    def test_valid_percentage_default(self):
        """Test valid percentage calculations with default multiply by 100."""
        test_cases = [
            (1, 2, 50.0),  # 1/2 * 100 = 50%
            (3, 4, 75.0),  # 3/4 * 100 = 75%
            (1, 10, 10.0),  # 1/10 * 100 = 10%
            ("25", "100", 25.0),  # 25/100 * 100 = 25%
        ]
        for num, den, expected in test_cases:
            result = safe_percentage(num, den)
            assert abs(result - expected) < 1e-10, f"Expected {expected}, got {result}"

    def test_valid_percentage_no_multiply(self):
        """Test valid percentage calculations without multiplying by 100."""
        test_cases = [
            (1, 2, 0.5),  # 1/2 = 0.5
            (3, 4, 0.75),  # 3/4 = 0.75
            (1, 10, 0.1),  # 1/10 = 0.1
        ]
        for num, den, expected in test_cases:
            result = safe_percentage(num, den, multiply_by_100=False)
            assert abs(result - expected) < 1e-10, f"Expected {expected}, got {result}"

    def test_percentage_edge_cases(self):
        """Test percentage calculation edge cases."""
        # Division by zero
        assert safe_percentage(10, 0) is None

        # Null values
        assert safe_percentage(None, 5) is None
        assert safe_percentage(10, None) is None

        # Invalid values
        assert safe_percentage("abc", 5) is None
        assert safe_percentage(10, "xyz") is None


class TestRealWorldNBAData:
    """Test coercion with real-world NBA data scenarios."""

    def test_player_stats_coercion(self):
        """Test coercion of typical player statistics."""
        # Points
        assert to_int_or_none("28") == 28
        assert to_int_or_none("DNP") is None

        # Minutes played
        assert to_float_or_none("35.5") == 35.5
        assert to_float_or_none("—") is None

        # Field goal percentage
        assert to_float_or_none("45.6%") == 45.6
        assert to_float_or_none("N/A") is None

        # Large numbers with commas
        assert to_int_or_none("1,234,567") == 1234567

    def test_team_stats_coercion(self):
        """Test coercion of typical team statistics."""
        # Team totals
        assert to_int_or_none("112") == 112
        assert to_float_or_none("48.2%") == 48.2

        # Missing data representations
        assert to_int_or_none("--") is None
        assert to_float_or_none("—") is None

    def test_advanced_metrics_coercion(self):
        """Test coercion of advanced metrics."""
        # PER, BPM, etc.
        assert to_float_or_none("24.8") == 24.8
        assert to_float_or_none("-2.1") == -2.1

        # Efficiency ratings
        result = safe_percentage(112, 100)  # Points per 100 possessions
        assert abs(result - 112.0) < 1e-10  # Use approximate comparison for floating-point
