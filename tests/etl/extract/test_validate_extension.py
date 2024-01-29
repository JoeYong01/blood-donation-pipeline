import pytest
import os
from src.etl.extract import (
	validate_extension
)

def test_validate_extention_with_extension():
    filepath = r"tests\files\donations_state.csv"
    assert validate_extension(filepath) == filepath

def test_validate_extention_without_extension():
    filepath = r"tests\files\ds-data-granular"
    expected_output = filepath + '.parquet'
    assert validate_extension(filepath) == expected_output

