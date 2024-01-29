import pandas as pd
import numpy as np
import pytest
from src.etl.transform import validate_col

@pytest.fixture
def mock_df():
    # Create a DataFrame with various data types
    return pd.DataFrame({
        'col1': [1, -1, 3],          # Integers with a negative value
        'col2': ['4', '-2', '6'],    # Strings that can be converted to integers
        'col3': [7.5, -3.5, 9.5],    # Floats
        'ignore_col': ['a', 'b', 'c'] # Column to ignore
    })

def test_validate_col(mock_df):
    ignore_cols = ['ignore_col']
    validated_df = validate_col(ignore_cols, mock_df)

    # Check that 'ignore_col' remains unchanged
    assert all(validated_df['ignore_col'] == mock_df['ignore_col']), "Ignored columns should not be modified"

    # Check that other columns are converted to numeric and negative values are replaced with NaN
    for col in ['col1', 'col2', 'col3']:
        assert validated_df[col].dtype == np.float64 or validated_df[col].dtype == np.int64, f"{col} should be converted to a numeric type"
        assert all((validated_df[col] >= 0) | validated_df[col].isna()), f"{col} should have non-negative values or NaN"

def test_validate_col_with_non_numeric(mock_df):
    # Adding a non-numeric column that is not ignored
    mock_df['non_numeric'] = ['x', 'y', 'z']
    ignore_cols = ['ignore_col']

    validated_df = validate_col(ignore_cols, mock_df)

    # The non-numeric column should be all NaN after validation
    assert all(validated_df['non_numeric'].isna()), "Non-numeric values should be converted to NaN"
