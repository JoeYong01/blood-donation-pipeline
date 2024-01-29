import pandas as pd
import pytest
import tempfile
from src.etl.transform import process_parquet

def test_process_parquet():
    # Create mock data
    mock_data = {
        'donor_id': ['09pZp', '09pZp', '09pZp'],
        'visit_date': ['2024-01-24', '2024-07-24', '2024-12-24'],
        'birth_date': ['1984-01-01', '1984-01-01', '1984-01-01']
    }
    df = pd.DataFrame(mock_data)

    # Write mock data to a temporary Parquet file
    with tempfile.NamedTemporaryFile(suffix='.parquet', mode='wb', delete=False) as tmp:
        df.to_parquet(tmp.name)
        temp_file_name = tmp.name

    # Call the function
    result_df = process_parquet(temp_file_name)

    # Perform your assertions
    # Example: Check if the result_df is not empty and has the expected columns
    assert not result_df.empty, "The result should not be empty"
    expected_columns = ['visits_before_churn', 'num_donors', 'percentage_of_total_donors']
    assert all(column in result_df.columns for column in expected_columns), "Result should have the expected columns"
