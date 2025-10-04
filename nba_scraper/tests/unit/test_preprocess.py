"""Unit tests for hardened data preprocessing to prevent clock string coercion."""

import pytest
from nba_scraper.utils.preprocess import preprocess_nba_stats_data


def test_clock_strings_not_coerced():
    """Test that clock-like strings are preserved as strings, not converted to floats."""
    data = {
        "CLOCK": "24:49",
        "PCTIMESTRING": "0:00", 
        "ISO": "PT10M24S",
        "FRACTIONAL": "1:23.4",
        "FRACTIONAL2": "01:23.45",
        "ISO_FRAC": "PT0M5.1S",
        "num": "123",
        "flt": "12.5",
        "negative": "-45",
        "zero": "0"
    }
    
    out = preprocess_nba_stats_data(data)
    
    # Clock strings should remain as strings
    assert out["CLOCK"] == "24:49"
    assert isinstance(out["CLOCK"], str)
    
    assert out["PCTIMESTRING"] == "0:00"
    assert isinstance(out["PCTIMESTRING"], str)
    
    assert out["ISO"] == "PT10M24S"
    assert isinstance(out["ISO"], str)
    
    assert out["FRACTIONAL"] == "1:23.4"
    assert isinstance(out["FRACTIONAL"], str)
    
    assert out["FRACTIONAL2"] == "01:23.45"
    assert isinstance(out["FRACTIONAL2"], str)
    
    assert out["ISO_FRAC"] == "PT0M5.1S"
    assert isinstance(out["ISO_FRAC"], str)
    
    # Numeric strings should be coerced
    assert out["num"] == 123
    assert isinstance(out["num"], int)
    
    assert out["flt"] == 12.5
    assert isinstance(out["flt"], float)
    
    assert out["negative"] == -45
    assert isinstance(out["negative"], int)
    
    assert out["zero"] == 0
    assert isinstance(out["zero"], int)


def test_nested_preprocess_respects_clocks():
    """Test that preprocessing works recursively and preserves clocks in nested structures."""
    data = {
        "a": [
            {"CLOCK": "12:34"},
            {"b": {"PCTIMESTRING": "5:00"}}
        ],
        "game_data": {
            "events": [
                {"time": "24:49", "score": "100"},
                {"time": "1:23.4", "player_id": "203999"}
            ]
        }
    }
    
    out = preprocess_nba_stats_data(data)
    
    # Verify nested clock preservation
    assert out["a"][0]["CLOCK"] == "12:34"
    assert isinstance(out["a"][0]["CLOCK"], str)
    
    assert out["a"][1]["b"]["PCTIMESTRING"] == "5:00"
    assert isinstance(out["a"][1]["b"]["PCTIMESTRING"], str)
    
    # Verify nested processing
    assert out["game_data"]["events"][0]["time"] == "24:49"
    assert isinstance(out["game_data"]["events"][0]["time"], str)
    
    assert out["game_data"]["events"][0]["score"] == 100
    assert isinstance(out["game_data"]["events"][0]["score"], int)
    
    assert out["game_data"]["events"][1]["time"] == "1:23.4"
    assert isinstance(out["game_data"]["events"][1]["time"], str)
    
    assert out["game_data"]["events"][1]["player_id"] == 203999
    assert isinstance(out["game_data"]["events"][1]["player_id"], int)


def test_edge_cases_and_null_handling():
    """Test edge cases and null value handling."""
    data = {
        "empty_string": "",
        "whitespace": "   ",
        "null_like": "null",
        "none_like": "None",
        "na_like": "N/A",
        "actual_none": None,
        "actual_bool": True,
        "zero_string": "0",
        "decimal_only": ".5",
        "invalid_clock": "25:70",  # Invalid minutes/seconds
        "not_clock": "12-34",  # Dash instead of colon
    }
    
    out = preprocess_nba_stats_data(data)
    
    assert out["empty_string"] == ""
    assert out["whitespace"] == ""  # Should be trimmed to empty
    assert out["null_like"] is None
    assert out["none_like"] is None
    assert out["na_like"] is None
    assert out["actual_none"] is None
    assert out["actual_bool"] is True
    assert out["zero_string"] == 0
    assert out["decimal_only"] == 0.5
    
    # Invalid clock formats should remain as strings
    assert out["invalid_clock"] == "25:70"
    assert isinstance(out["invalid_clock"], str)
    
    assert out["not_clock"] == "12-34"
    assert isinstance(out["not_clock"], str)


def test_original_problematic_case():
    """Test the specific case that was causing the original float conversion error."""
    data = {
        "PCTIMESTRING": "24:49",
        "EVENTNUM": "445",
        "PERIOD": "4",
        "EVENTMSGTYPE": "2"
    }
    
    out = preprocess_nba_stats_data(data)
    
    # This should NOT raise "could not convert string to float: '24:49'"
    assert out["PCTIMESTRING"] == "24:49"
    assert isinstance(out["PCTIMESTRING"], str)
    
    # Other numeric fields should still be converted
    assert out["EVENTNUM"] == 445
    assert isinstance(out["EVENTNUM"], int)
    
    assert out["PERIOD"] == 4
    assert isinstance(out["PERIOD"], int)
    
    assert out["EVENTMSGTYPE"] == 2
    assert isinstance(out["EVENTMSGTYPE"], int)


def test_list_processing():
    """Test that lists are processed correctly."""
    data = [
        {"CLOCK": "12:00", "value": "100"},
        {"CLOCK": "11:59", "value": "200"},
        "not a dict",
        123,
        None
    ]
    
    out = preprocess_nba_stats_data(data)
    
    assert len(out) == 5
    assert out[0]["CLOCK"] == "12:00"
    assert out[0]["value"] == 100
    assert out[1]["CLOCK"] == "11:59" 
    assert out[1]["value"] == 200
    assert out[2] == "not a dict"
    assert out[3] == 123
    assert out[4] is None