"""contains any logic to transform/validate/clean data"""
from datetime import datetime, timedelta
import pandas as pd

def get_yesterdays_date(
    df: pd.DataFrame,
    date_column: str
) -> pd.DataFrame:
    """
    extracts yesterday's data from a pandas dataframe

    Args:
        df (pd.DataFrame): the pandas dataframe in question
        date_column (str): date column 

    Returns:
        pd.DataFrame: pandas dataframe with the latest data
    """
    datetime_now = datetime.now()
    datetime_ytd = datetime_now - timedelta(days=1)
    date_ytd = datetime_ytd.strftime("%Y-%m-%d")
    yesterdays_data = df[pd.to_datetime(df[f"{date_column}"]) == date_ytd]
    return yesterdays_data

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
    for col in df.columns:
        if col not in ignore_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].mask(df[col] < 0)
    return df
