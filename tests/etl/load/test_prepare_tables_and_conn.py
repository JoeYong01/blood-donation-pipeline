import pytest
from unittest.mock import patch, MagicMock
from src.etl.load import prepare_tables_and_conn

@patch('src.etl.load.create_engine')
def test_prepare_tables_and_conn(mock_create_engine):
    # Set up the mock engine and connection
    mock_engine = mock_create_engine.return_value
    mock_connection = mock_engine.connect.return_value.__enter__.return_value
    mock_connection.execute = MagicMock()

    # Set test parameters
    db_user = 'user'
    db_password = 'password'
    db_host = 'host'
    db_port = 'port'
    db_schema = 'schema'
    queries = ['CREATE TABLE test (id INT);', 'CREATE INDEX idx_test ON test (id);']

    # Expected connection string
    expected_conn_str = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_schema}"

    # Call the function
    conn_str = prepare_tables_and_conn(db_user, db_password, db_host, db_port, db_schema, queries)

    # Assertions
    mock_create_engine.assert_called_once_with(expected_conn_str)
    assert mock_connection.execute.call_count == len(queries), "Each query should be executed once"
    assert conn_str == expected_conn_str, "Function should return the correct connection string"
