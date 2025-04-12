import pandas as pd
import requests
import os


def make_realtime_request() -> dict:

    """
    Make a request to the Auckland Transport API's real-time feed, and return
    the result as a dictionary.

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
