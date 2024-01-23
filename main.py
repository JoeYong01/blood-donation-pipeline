"""main file"""
from datetime import datetime
import logging
import os
from src.etl.extract import download_file

# misc const/var
DATE_NOW = datetime.now().strftime("%Y/%m/%d/")

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

# download_file const/var
DOWNLOAD_DIRECTORY = os.path.join("downloads", DATE_NOW)
FILE_URLS = [
    "https://github.com/MoH-Malaysia/data-darah-public/blob/main/donations_facility.csv",
    "https://github.com/MoH-Malaysia/data-darah-public/blob/main/donations_state.csv",
    "https://github.com/MoH-Malaysia/data-darah-public/blob/main/newdonors_facility.csv",
    "https://github.com/MoH-Malaysia/data-darah-public/blob/main/newdonors_state.csv",
    "https://dub.sh/ds-data-granular"
]

def main():
    """main function"""
    logger.info("looping through FILE_URLS")
    for url in FILE_URLS:
        filename = url.rsplit('/', maxsplit=1)[-1]
        download_file(url, DOWNLOAD_DIRECTORY, filename)

if __name__ == "__main__":
    main()
