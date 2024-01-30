import pytest
import requests
from src.etl.extract import download_file
from unittest.mock import patch, mock_open

@patch('src.etl.extract.requests.get')
@patch('src.etl.extract.os.makedirs')
@patch('src.etl.extract.open', new_callable=mock_open)
@patch('src.etl.extract.validate_extension')
def test_download_file_success(mock_validate_extension, mock_file, mock_makedirs, mock_requests_get):
    # Set up a mock response for requests.get
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response._content = b'test file content'
    mock_requests_get.return_value = mock_response

    # Mock validate_extension to return a valid file path
    mock_validate_extension.return_value = '/fake/directory/file.txt'

    # Call the function
    download_file('https://raw.githubusercontent.com/MoH-Malaysia/data-darah-public/main/donations_facility.csv', '/fake/directory', 'file.txt')

    # Assert that the directory was created
    mock_makedirs.assert_called_once_with('/fake/directory', exist_ok=True)

    # Assert that the file was written
    mock_file.assert_called_once_with('/fake/directory/file.txt', 'wb')
    handle = mock_file()
    handle.write.assert_called_once_with(b'test file content')

@patch('src.etl.extract.requests.get')
def test_download_file_failure(mock_requests_get):
    # Simulate a failed response from requests.get
    mock_response = requests.Response()
    mock_response.status_code = 404
    mock_requests_get.return_value = mock_response

    # Call the function
    download_file('http://example.com/nonexistentfile', '/fake/directory', 'file.txt')