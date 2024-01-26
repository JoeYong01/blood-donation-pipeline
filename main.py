"""main file"""
from datetime import datetime
import logging
import os
from src.etl.extract import (
    validate_extension,
    download_file
)
from src.etl.transform import (
	get_yesterdays_data,
	validate_col,
)

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

def main():
    """main function"""
    logger.info("looping through FILE_URLS")
    for url in FILE_URLS:
        # ds-data-granular
        filename = validate_extension(url.rsplit('/', maxsplit=1)[-1])
        download_file(url, RAW_DIR, filename)
        staging_filepath = os.path.join(STAGING_DIR, filename)
        cleaned_filepath = os.path.join(CLEANED_DIR, filename)
        filepath = os.path.join(RAW_DIR, filename)
        df = get_yesterdays_data(filepath, DATE_COLS)
        df.to_csv(staging_filepath, index=False)
        df_cleaned = validate_col(IGNORE_COLS, df)
        df_cleaned.to_csv(cleaned_filepath, index=False)

if __name__ == "__main__":
    main()
