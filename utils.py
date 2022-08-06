import requests
import json
import pandas as pd
import os

from logger_file import logger


def get_json_data_from_bls(series_id: list, start_year: int, end_year: int) -> dict:
    """
    Using BlS API to get json data of unemployment rates of the given series (that represent local areas)
     between the given years
    :param series_id: A list of series id that presents the local areas for which you want to receive the desired data
    :param start_year: The year from which the data is desired
    :param end_year: The year up to which the data is desired
    :return: json with the desired data.
    """

    # Set request variables.
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    headers = {"Content-Type": "application/json"}
    payload = {"seriesid": series_id, "startyear": str(start_year), "endyear": str(end_year)}

    logger.info(f"Send request to BLS for getting json data about series: {series_id}, "
                f"between the years {start_year} and {end_year}.")
    response = requests.post(url, json=payload, headers=headers)
    json_data = response.json()
    logger.info(json_data['status'])

    if len(json_data['message']) > 0:
        logger.warning(json_data['message'])

    return json_data


def save_json(file_path: str, json_data: dict):
    """
    Save json data into json file in the given path
    :param file_path: The file path to save the data to
    :param json_data: Json with the data to saved
    """

    # If the directory to which the save is made doesn't exist, then create it.
    dir_name = os.path.dirname(file_path)
    os.makedirs(dir_name, exist_ok=True)

    # Save the data to a json file
    with open(file_path, 'w') as file:
        json.dump(json_data, file)


def create_parquet_table(json_data: dict, raw_file: str, received_date: str, parquet_dir: str = "./data"):
    """
    Create parquet table from the json_data with the columns : 'seriesID', 'year', 'period', 'periodName', 'value',
     'footnotes', and with partition by 'seriesID' and 'year'
    :param json_data: The raw json downloaded using the BLS API ,of the unemployment rates of some local areas
    between some years
    :param raw_file: The path of the file that contains the raw json_data
    :param received_date: The date in which the data was downloaded from BLS
    :param parquet_dir: The directory path to store parquet table in.
    """

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
