"""main file"""
import asyncio
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
from src.etl.extract import (
    validate_extension,
    download_file
)
from src.etl.transform import (
    get_date_from_db,
	get_latest_data,
	validate_col,
    call_procedure,
    process_parquet
)
from src.etl.load import (
	prepare_tables_and_conn,
	upload_data,
)
from src.sql import (
    PREP_DATABASE,
	PREP_NEWDONORS_STATE,
	PREP_NEWDONORS_FACILITY,
	PREP_DS_DATA_GRANULAR,
	PREP_DONATIONS_STATE,
	PREP_DONATIONS_FACILITY,
    QUERY_DATE,
    QUESTION_1_PROCEDURE,
    QUESTION_2_PROCEDURE
)
from src.notification import send_telegram_message

load_dotenv()

# misc const/var
DATE_NOW = datetime.now().strftime("%Y/%m/%d")

# logging const/var
LOG_NAME = "blood-donation-pipeline.log"
LOG_DIR = os.path.join("logs", DATE_NOW)
LOG_FILEPATH = os.path.join(LOG_DIR, LOG_NAME)
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger(os.path.basename(__file__))
logging.basicConfig(
    filename=LOG_FILEPATH,
    level=logging.INFO,
    format="%(asctime)s : %(name)s : %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# mysql consts
DB_USER = os.environ.get("db_user")
DB_PASSWORD = os.environ.get("db_password")
DB_HOST = os.environ.get("db_host")
DB_PORT = os.environ.get("db_port")
DB_SCHEMA = os.environ.get("db_schema")

# telegram consts
TELEGRAM_BOT_TOKEN = os.environ.get("telegram_bot_token")
TELEGRAM_GROUP_ID = os.environ.get("telegram_group_id")
TELEGRAM_TEXT = '<a href="https://lookerstudio.google.com/reporting/e20bfcff-b50b-498b-8780-e937f04146da">Dashboard here</a>'

# preperation query
PREPERATION_QUERIES = [
    PREP_DATABASE,
    PREP_NEWDONORS_STATE,
    PREP_NEWDONORS_FACILITY,
    PREP_DS_DATA_GRANULAR,
    PREP_DONATIONS_STATE,
    PREP_DONATIONS_FACILITY
]

# download_file & directory const/var
RAW_DIR = os.path.join("data", "raw", DATE_NOW)
os.makedirs(RAW_DIR, exist_ok=True)
STAGING_DIR = os.path.join("data", "staging", DATE_NOW)
os.makedirs(STAGING_DIR, exist_ok=True)
CLEANED_DIR = os.path.join("data", "cleaned", DATE_NOW)
os.makedirs(CLEANED_DIR, exist_ok=True)
FILE_URLS = [
    "https://raw.githubusercontent.com/MoH-Malaysia/data-darah-public/main/donations_facility.csv",
    "https://raw.githubusercontent.com/MoH-Malaysia/data-darah-public/main/donations_state.csv",
    "https://raw.githubusercontent.com/MoH-Malaysia/data-darah-public/main/newdonors_facility.csv",
    "https://raw.githubusercontent.com/MoH-Malaysia/data-darah-public/main/newdonors_state.csv",
    "https://dub.sh/ds-data-granular"
]

# ignore these columns for int validation
IGNORE_COLS = ['date', 'hospital', 'state', 'donor_id', 'visit_date']
DATE_COLS = ['date', 'visit_date']

async def main():
    """main function"""
    conn_str = prepare_tables_and_conn(
        DB_USER,
        DB_PASSWORD,
        DB_HOST,
        DB_PORT,
        DB_SCHEMA,
        PREPERATION_QUERIES
    )
    logger.info("looping through FILE_URLS")
    for url in FILE_URLS:
        filename_with_ext = validate_extension(url.rsplit('/', maxsplit=1)[-1])
        download_file(url, RAW_DIR, filename_with_ext)
        staging_filepath = os.path.join(STAGING_DIR, filename_with_ext)
        cleaned_filepath = os.path.join(CLEANED_DIR, filename_with_ext)
        filepath = os.path.join(RAW_DIR, filename_with_ext)
        latest_date = get_date_from_db(conn_str, QUERY_DATE)
        df = get_latest_data(filepath, latest_date, DATE_COLS)
        filename, ext = os.path.splitext(filename_with_ext)
        if ext == '.parquet':
            df.to_parquet(staging_filepath, index=False)
            df_cleaned = validate_col(IGNORE_COLS, df)
            df_cleaned.to_parquet(cleaned_filepath, index=False)
            # seprate job to transform the parquet data
            parquet = process_parquet(filepath)
            upload_data(
                conn_str,
                parquet,
                'q2',
                'replace'
            )
        elif ext == '.csv':
            df.to_csv(staging_filepath, index=False)
            df_cleaned = validate_col(IGNORE_COLS, df)
            df_cleaned.to_csv(cleaned_filepath, index=False)
        upload_data(
            conn_str,
            df_cleaned,
            filename.replace("-", '_')
        )
    call_procedure(conn_str, QUESTION_1_PROCEDURE)
    await send_telegram_message(
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_GROUP_ID,
        TELEGRAM_TEXT
    )

if __name__ == "__main__":
    asyncio.run(main())
