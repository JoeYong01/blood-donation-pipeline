"""contains any logic to transform/validate/clean data"""
import logging
import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(os.path.basename(__file__))

def get_file_extension(
    filepath: str
) -> str:
    """
    extracts the file extenion type

    Args:
        filepath (str): full path to the file

    Returns:
        str: extension type
    """
    logger.info("running get_file_extension")
    _, file_ext = os.path.splitext(filepath)
    file_type = file_ext.lower()[1:]
    logger.debug("file extension: %s", file_type)
    logger.info("returning file_type")
    return file_type

def get_date_from_db(
    sqlalchemy_conn_str: str,
    query: str
) -> str:
    """
    extracts the latest extracted data from the database.

    Args:
        sqlalchemy_conn_str (str): sqlalchemy connections tring
        query (str): query to run

    Returns:
        str: date
    """
    logger.info("running get_date_from_db function")
    engine = create_engine(sqlalchemy_conn_str)
    logger.info("creating context manager")
    with engine.connect() as conn:
        logger.info("executing query")
        result = conn.execute(text(query))
        row = result.fetchone()
        logger.info("returning date")
        return str(row[0])

def get_latest_data(
    filepath: str,
    date: str,
    date_columns: list
) -> pd.DataFrame:
    """
    extracts yesterday's data from a file

    Args:
        filepath (str): full path to the file in question
        date (str): date from get_date_from_db func
        date_column (list): a list of possible date columns

    Returns:
        pd.DataFrame: pandas dataframe with the latest data
    """
    logger.info("running get_latest_data function")
    logger.debug("obtaining pd.read_* type")
    pandas_reader = {
        'csv': pd.read_csv,
        'parquet': pd.read_parquet
    }
    ext_type = get_file_extension(filepath)
    reader = pandas_reader.get(ext_type)
    if not reader:
        error_message = "get_latest_data only supports csv/parquet"
        logger.exception(error_message)
        raise ValueError(error_message)
    df = reader(filepath)
    for date_column in date_columns:
        if date_column in df.columns:
            logger.debug("filtering latest data from pandas dataframe.")
            latest_data = df[pd.to_datetime(df[date_column]) > pd.to_datetime(date)]
            logger.info("returning pandas dataframe")
            return latest_data
    error_message = "date columns not found in dataframe"
    logger.error(error_message)
    raise ValueError(error_message)

def validate_col(
    ignore_cols: list,
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    validates (replace with NaN) each column that is not in ignore_cols to:
    - ensure it is an int
    - ensure the int value is not negative

    Args:
        ignore_cols (list): colunns to ignore
        df (pd.DataFrame): the pandas dataframe in question

    Returns:
        pd.DataFrame: validated pandas dataframe
    """
    logger.info("running validate_col function")
    for col in df.columns:
        if col not in ignore_cols:
            logger.debug("validating other columns are int dtypes")
            df[col] = pd.to_numeric(df[col], errors='coerce')
            logger.debug("validating int records > 0")
            df[col] = df[col].mask(df[col] < 0)
    logger.info("returning dataframe")
    return df

def call_procedure(
    sqlalchemy_conn_str: str,
    procedure: str
) -> None:
    """
    calls a sql procedure to kick start table creation

    Args:
        sqlalchemy_conn_str (str): sqlalchemy connection string
        procedure (str): mysql procedure to call
    
    Returns:
        None
    """
    logger.info("running call_procedure function.")
    engine = create_engine(sqlalchemy_conn_str)
    logger.info("creating context manager")
    with engine.connect() as conn:
        logger.info("executing procedure")
        conn.execute(text(procedure))

def process_parquet(
    filepath: str
) -> pd.DataFrame:
    """
    original sql statement turned into pandas operation due to /tmp limitation

    Args:
        filepath (str): filepath

    Returns:
        pd.DataFrame: pandas dataframe
    """
    df = pd.read_parquet(filepath)
    df['visit_date'] = pd.to_datetime(df['visit_date'])

    # Step 2: Replicate the donation_details CTE
    df = df.sort_values(['donor_id', 'visit_date'])
    df['next_visit_date'] = df.groupby('donor_id')['visit_date'].shift(-1)

    # Now, 'visit_date' and 'next_visit_date' are datetime objects
    df['is_churned'] = (df['next_visit_date'] - df['visit_date']).dt.days > 180
    df['is_churned'] = df['is_churned'].astype(int)

    # Step 4: Replicate the donor_churn_count CTE
    donor_churn_count = df.groupby('donor_id')['is_churned'].sum().reset_index()
    donor_churn_count.rename(columns={'is_churned': 'visits_before_churn'}, inplace=True)

    # Step 5: Replicate the churn_distribution CTE
    churn_distribution = donor_churn_count.groupby('visits_before_churn').size().reset_index(name='num_donors')

    # Step 6: Replicate the total_donors CTE
    total_donors = len(donor_churn_count['donor_id'].unique())

    # Step 7: Final SELECT query
    result = churn_distribution.copy()
    result['percentage_of_total_donors'] = (result['num_donors'] / total_donors) * 100
    result = result.sort_values('visits_before_churn')
    
    return result