import os
from datetime import datetime

from utils import get_json_data_from_bls, save_json, create_parquet_table
from logger_file import logger
from configs import series_id, start_year, end_year, raw_data_dir


def main(series_id=series_id, start_year=start_year, end_year=end_year, raw_data_dir=raw_data_dir):
    try:
        json_data = get_json_data_from_bls(series_id, start_year, end_year)
    except Exception as error:
        logger.error(f"An error occurred while trying to get the data from BLS: {error}")
    else:
        json_file_name = f"raw data {str(datetime.now()).replace(':', '_')}.json"
        raw_file = os.path.join(raw_data_dir, json_file_name)

        try:
            save_json(raw_file, json_data)
        except Exception as error:
            logger.error(f"An error occurred while trying to save the raw json: {error}")
            raw_file = "Doesn't_saved"

        create_parquet_table(json_data, raw_file, received_date=datetime.now().date())


if __name__ == "__main__":
    from logger_file import set_log_file_handler

    # Set log file.
    log_path = f"./logs/log_{datetime.now().date()}.log"
    os.makedirs("./logs", exist_ok=True)
    set_log_file_handler(log_path)

    main()
