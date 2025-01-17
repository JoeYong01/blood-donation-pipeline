"""contains any logic to extract data"""
import logging
import os
from typing import IO
import requests

logger = logging.getLogger(os.path.basename(__file__))

def validate_extension(
    filepath: str,
    default_extension: str = ".parquet"
) -> str:
    """
    checks to see if there is a file extension, else append ".parquet"

    Args:
        filefilepathname (str): full path to the file
        default_extension (str, optional): extension tto append. Defaults to ".parquet".

    Returns:
        str: a filepath with an extension
    """
    logging.info("running validate_extension function")
    _, ext = os.path.splitext(filepath)
    logging.info("returning filepath with extension")
    return filepath + default_extension if not ext else filepath

def download_file(
    url: str,
    directory: str,
    filename: str
) -> IO[bytes]:
    """
    downloads a file to a directory using requests.

    Args:
        url (str): url to request file from
        directory (str): directory to download the file to
        filename (str): name of the file

    Returns:
        IO[bytes]: returns a file 
    """
    logger.info("running download_file function")
    response = requests.get(url, timeout=60)
    filepath = validate_extension(os.path.join(directory, filename))
    os.makedirs(directory, exist_ok=True)
    if response.status_code == 200:
        try:
            logger.debug("opening context manager")
            with open(filepath, 'wb') as file:
                logger.info("writing files to directory")
                file.write(response.content)
        except Exception as e:
            logger.exception("exception in download_file: %s", e)
    else:
        logger.error(
            "Failed to download %s. response status code: %s", 
            filename, response.status_code
        )
