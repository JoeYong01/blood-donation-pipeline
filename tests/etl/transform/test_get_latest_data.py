import pandas as pd
import pytest
from unittest.mock import patch
from src.etl.transform import get_latest_data

# Create a mock DataFrame
data = {
    'date_column': ['2021-01-01', '2021-01-02', '2021-01-03'],
    'other_column': [1, 2, 3]
}
mock_df = pd.DataFrame(data)

@patch('src.etl.transform.get_file_extension')
@patch('src.etl.transform.pd.read_csv')
@patch('src.etl.transform.pd.read_parquet')
def test_get_latest_data_csv(mock_read_parquet, mock_read_csv, mock_get_file_extension):
    # Setup mock behavior for CSV
    mock_get_file_extension.return_value = 'csv'
    mock_read_csv.return_value = mock_df

    # Test function for CSV file
    latest_data = get_latest_data('dummy.csv', '2021-01-02', ['date_column'])

    # Assertions
    assert not latest_data.empty, "DataFrame should not be empty"
    assert all(pd.to_datetime(latest_data['date_column']) > pd.to_datetime('2021-01-02')), "Data should be filtered based on the date"

@patch('src.etl.transform.get_file_extension')
@patch('src.etl.transform.pd.read_csv')
@patch('src.etl.transform.pd.read_parquet')
def test_get_latest_data_parquet(mock_read_parquet, mock_read_csv, mock_get_file_extension):
    # Setup mock behavior for Parquet
    mock_get_file_extension.return_value = 'parquet'
    mock_read_parquet.return_value = mock_df

    # Test function for Parquet file
    latest_data = get_latest_data('dummy.parquet', '2021-01-02', ['date_column'])

    # Assertions
    assert not latest_data.empty, "DataFrame should not be empty"
    assert all(pd.to_datetime(latest_data['date_column']) > pd.to_datetime('2021-01-02')), "Data should be filtered based on the date"
