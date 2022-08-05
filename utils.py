import requests
import json
import pandas as pd
import os

from logger_file import logger


def get_json_data_from_bls(series_id: list, start_year: int, end_year: int):
    """

    :param series_id:
    :param start_year:
    :param end_year:
    :return:
    """

    # Set request variables.
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    headers = {"Content-Type": "application/json"}
    payload = {"seriesid": series_id, "startyear": str(start_year), "endyear": str(end_year)}

    logger.info(f"Send request to BLS for getting json data about series: {series_id}, "
                f"between the years {start_year} and {end_year}.")
    response = requests.post(url, json=payload, headers=headers)
    json_data = response.json()

    if len(json_data['message']) > 0:
        logger.warning(json_data['message'])

    return json_data


def save_json(file_path: str, json_data: json):
    dir_name = os.path.dirname(file_path)
    os.makedirs(dir_name, exist_ok=True)

    with open(file_path, 'w') as file:
        json.dump(json_data, file)


def create_parquet_table(json_data, raw_file, received_date, parquet_dir="./data"):
    # Create an empty DataFrame.
    final_df = pd.DataFrame([])

    # Create DataFrame with data of all series.
    # column names: 'seriesID', 'year', 'period', 'periodName', 'value', 'footnotes'.
    for series in json_data['Results']['series']:

        # Create DataFrame of the current series.
        df2 = pd.DataFrame(series['data'], dtype='str')
        df2['seriesID'] = str(series['seriesID'])

        # Add the current series DataFrame to the end of the final DataFrame.
        final_df = final_df.append(df2, ignore_index=True)

    # Add received_date and raw_file columns to the DataFrame.
    final_df['received_date'] = received_date
    final_df['raw_file'] = raw_file

    # Create parquet from the data with partition by 'seriesID' and 'year'
    final_df.to_parquet(parquet_dir, partition_cols=['seriesID', 'year'])
    logger.info(f"Successfully create a parquet table from the desired data. Directory path: {parquet_dir}")