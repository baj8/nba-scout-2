"""Unit tests for preprocessing utilities."""

import pytest
from nba_scraper.utils.preprocess import preprocess_nba_stats_data


def test_clock_strings_not_coerced():
    """Test that clock-like strings are not coerced to floats."""
    data = {
        "CLOCK": "24:49",
        "PCTIMESTRING": "0:00", 
        "ISO": "PT10M24S",
        "num": "123",
        "flt": "12.5",
        "bool_true": True,
        "bool_false": False,
        "null_val": None,
        "empty_str": "",
        "fractional_clock": "1:23.4"
    }
    
    result = preprocess_nba_stats_data(data)
    
    # Clock strings should remain as strings
    assert result["CLOCK"] == "24:49"
    assert result["PCTIMESTRING"] == "0:00" 
    assert result["ISO"] == "PT10M24S"
    assert result["fractional_clock"] == "1:23.4"
    
    # Numeric strings should be coerced
    assert result["num"] == 123
    assert result["flt"] == 12.5
    
    # Other types should remain unchanged
    assert result["bool_true"] is True
    assert result["bool_false"] is False 
    assert result["null_val"] is None
    assert result["empty_str"] == ""


def test_nested_data_preprocessing():
    """Test preprocessing of nested dictionaries and lists."""
    data = {
        "resultSets": [
            {
                "headers": ["EVENTNUM", "PCTIMESTRING", "TEAM_ID"],
                "rowSet": [
                    [1, "12:00", "1610612737"],
                    [2, "11:30.5", "1610612738"]
                ]
            }
        ],
        "parameters": {
            "GameID": "0022300001",
            "StartPeriod": "1"
        }
    }
    
    result = preprocess_nba_stats_data(data)
    
    # Check that nested structures are processed
    assert result["resultSets"][0]["rowSet"][0][1] == "12:00"  # Clock preserved
    assert result["resultSets"][0]["rowSet"][0][2] == 1610612737  # Team ID coerced to int
    assert result["parameters"]["StartPeriod"] == 1  # Period coerced to int


def test_mixed_type_robustness():
    """Test preprocessing handles mixed and edge case types."""
    data = {
        "int_val": 42,
        "float_val": 3.14,
        "str_int": "99",
        "str_float": "2.71",
        "clock_min_sec": "5:30",
        "clock_with_frac": "2:15.125", 
        "iso_duration": "PT5M30S",
        "iso_with_frac": "PT2M15.125S",
        "not_clock": "25:70",  # Invalid clock
        "empty_list": [],
        "nested": {"inner_clock": "10:45"}
    }
    
    result = preprocess_nba_stats_data(data)
    
    assert result["int_val"] == 42
    assert result["float_val"] == 3.14
    assert result["str_int"] == 99
    assert result["str_float"] == 2.71
    assert result["clock_min_sec"] == "5:30"
    assert result["clock_with_frac"] == "2:15.125"
    assert result["iso_duration"] == "PT5M30S"
    assert result["iso_with_frac"] == "PT2M15.125S"
    assert result["not_clock"] == "25:70"  # Invalid clock stays as string
    assert result["empty_list"] == []
    assert result["nested"]["inner_clock"] == "10:45"