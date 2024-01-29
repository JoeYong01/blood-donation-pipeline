import pytest
from unittest.mock import patch, MagicMock, call
from sqlalchemy import text
from src.etl.transform import call_procedure

@patch('src.etl.transform.create_engine')
def test_call_procedure(mock_create_engine):
    # Set up the mock engine and connection
    mock_engine = mock_create_engine.return_value
    mock_connection = mock_engine.connect.return_value.__enter__.return_value
    mock_connection.execute = MagicMock()

    # Set test parameters
    sqlalchemy_conn_str = 'mysql+pymysql://user:password@host:port/dbname'
    procedure = 'CALL my_procedure()'

    # Call the function
    call_procedure(sqlalchemy_conn_str, procedure)

    # Check that the 'execute' method was called with a 'text' object
    assert mock_connection.execute.call_args[0][0].text == text(procedure).text
