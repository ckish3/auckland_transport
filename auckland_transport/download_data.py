from collections import defaultdict

import pandas as pd
import requests
import os


def make_realtime_request() -> dict:

    """
    Make a request to the Auckland Transport API's real-time feed, and return
    the response data as a dictionary.

    Returns:
        dict: The real-time data from the feed.
    """
    url = 'https://api.at.govt.nz/realtime/legacy/tripupdates'
    key = os.environ['AUCKLAND_TRANSPORT_API_KEY']
    headers = {
        'Cache-Control': 'no-cache',
        'Ocp-Apim-Subscription-Key': key
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Request failed with status code {response.status_code}")

    data = response.json()
    if data['status'] != 'OK':
        raise Exception(f"Request failed with status {data['status']}")

    return data['response']



def convert_request_to_df(data: dict) -> pd.DataFrame:
    """
    Convert the real-time data from the Auckland Transport API into a pandas
    DataFrame.

    Args:
        data (dict): The real-time data from the Auckland Transport API.

    Returns:
        pd.DataFrame: The real-time data in a pandas DataFrame.
    """

    entities = data['entity']
    formatted_dict = defaultdict(list)

    for e in entities:
        try:
            row = {
                'trip_id': e['trip_update']['trip']['trip_id'],
                'route_id': e['trip_update']['trip']['route_id'],
                'direction_id': e['trip_update']['trip']['direction_id'],
                'start_time': e['trip_update']['trip']['start_time'],
                'start_date': e['trip_update']['trip']['start_date'],
                'stop_id': e['trip_update']['stop_time_update']['stop_id'],
                'stop_time': e['trip_update']['stop_time_update']['arrival']['time'],
                'stop_delay': e['trip_update']['stop_time_update']['arrival']['delay'],
                'stop_uncertainty': e['trip_update']['stop_time_update']['arrival']['uncertainty'],
                'stop_sequence': e['trip_update']['stop_time_update']['stop_sequence'],
                'vehicle_id': e['trip_update']['vehicle']['id'],
                'vehicle_license_plate': e['trip_update']['vehicle']['license_plate'],
                'trip_delay': e['trip_update']['delay'],
                'timestamp': e['trip_update']['timestamp'],
            }

            for k, v in row.items():
                formatted_dict[k].append(v)

        except KeyError as e:
            print(f"Error: {e}")
            continue

    df = pd.DataFrame(formatted_dict)
    return df
