import pytest
from src.etl.transform import get_file_extension


def test_file_extension_with_extension():
    assert get_file_extension(r"tests\files\donations_state.csv") == 'csv', "should return 'csv'"

def test_file_extension_without_extension():
    assert get_file_extension(r"tests\files\ds-data-granular") == '', "should return ''"
