import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from src.etl.load import upload_data

@patch('src.etl.load.create_engine')
@patch.object(pd.DataFrame, 'to_sql')
def test_upload_data(mock_to_sql, mock_create_engine):
    # Mock DataFrame
    mock_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['A', 'B', 'C']})

    # Test parameters
    sqlalchemy_conn_str = 'mysql+pymysql://user:password@host:port/dbname'
    table = 'test_table'
    if_exists_condition = 'append'

    # Call the function
    upload_data(sqlalchemy_conn_str, mock_df, table, if_exists_condition)

    # Assertions
    mock_create_engine.assert_called_once_with(sqlalchemy_conn_str)
    mock_to_sql.assert_called_once_with(
        table, mock_create_engine.return_value, 
        if_exists=if_exists_condition, index=False, chunksize=10000
    )
