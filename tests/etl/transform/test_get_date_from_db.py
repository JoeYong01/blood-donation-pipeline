import pytest
from unittest.mock import patch, MagicMock
from src.etl.transform import get_date_from_db

@patch('src.etl.transform.create_engine')
def test_get_date_from_db(mock_create_engine):
    # Set up the mock engine and connection
    mock_engine = mock_create_engine.return_value
    mock_connection = mock_engine.connect.return_value.__enter__.return_value
    mock_result = MagicMock()
    mock_result.fetchone.return_value = ['2021-01-01']
    mock_connection.execute.return_value = mock_result

    # Set test parameters
    sqlalchemy_conn_str = 'sqlite:///:memory:'
    query = 'SELECT MAX(date) FROM table_name'

    # Call the function
    result_date = get_date_from_db(sqlalchemy_conn_str, query)

    # Assertions
    assert result_date == '2021-01-01', "Should return the correct date from the database"
