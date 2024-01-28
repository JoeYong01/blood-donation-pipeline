"""contains logic to load data to database"""
import logging
import os
from sqlalchemy import create_engine, text
import pandas as pd

logger = logging.getLogger(os.path.basename(__file__))

def prepare_tables_and_conn(
    db_user: str,
    db_password: str,
    db_host: str,
    db_port: str,
    db_schema: str,
    queries: list
) -> str:
    """
    returns a sqlalcehmy conn str & runs a few queries to:
    - prepare tables using correct datatypes
    - create indexes for optimal analytics

    Args:
        db_user (str): database user
        db_password (str): database password
        db_host (str): database host
        db_port (str): database port
        db_schema (str): database schema/database
        query (list): validation queries to run

    Returns:
        str: a sqlalchemy connection string
    """
    logger.info("running prepare_tables_and_conn function")
    mysql_conn_str = (
        f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_schema}"
    )
    engine = create_engine(mysql_conn_str)
    logger.info("creating context manager")
    with engine.connect() as conn:
        logger.info("looping through query list")
        for query in queries:
            conn.execute(text(query))
    logger.info("returning connection string")
    return mysql_conn_str

def upload_data(
    sqlalchemy_conn_str: str,
    df: pd.DataFrame,
    table: str
) -> None:
    """
    uploads (appends) a dataframe to a mysql database

    Args:
        dataframe (pd.DataFrame): dataframe to upload
        table (str): table to upload to
    """
    logger.info("running upload_data function")
    engine = create_engine(sqlalchemy_conn_str)
    logger.info("uploading data to database")
    df.to_sql(table, engine, if_exists='append', index=False, chunksize=10000)
