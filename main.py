import os
from datetime import datetime
from typing import List
from utils import get_json_data_from_bls, save_json, create_parquet_table
from logger_file import logger
from configs import series_id, start_year, end_year, raw_data_dir


def main(series_id: List[str] = series_id,
         start_year: int = start_year,
         end_year: int = end_year,
         raw_data_dir: str = raw_data_dir):
    """
    Download and store the public unemployment rate data of several local areas in the U.S.
    The data stored in two layers, the raw data - json files of the data retrieved using the BLS API,
    the mirror data - parquet table with the columns : 'seriesID', 'year', 'period', 'periodName', 'value', 'footnotes',
    and with partition by 'seriesID' and 'year'
    :param series_id: A list of series id that presents the local areas for which you want to receive the desired data
    :param start_year: The year from which the data is desired
    :param end_year: The year up to which the data is desired
    :param raw_data_dir: The directory path to store the raw data in.
    """

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
