from dotenv import load_dotenv
from typing import Dict, List
import arrow
import os
import requests

def get_api_key():
    """
    Loads environment variables and retrieves the api key from the environment.

    Returns:
        str: The retrieved API key
    """
    load_dotenv()
    return os.getenv('API_KEY')

def fetch_tide_data(latitude: float, longitude: float) -> Dict:
    """
    Fetches tide data for a given latitude and longitude

    The function queries the Stormglass API to retrieve tide data for the provided geographic coordinates
    The start time for the data is set to the current day at midnight adjusted to PST

    Args:
        latitude (float): The latitude of the location
        longitude (float): The longitude of the location

    Returns:
        dict: A JSON response contianing the tide data

    Raises:
        requests.exceptions.HTTPError: If the HTTP request was unsuccessful
    """
    start = arrow.now().floor('day')
    response = requests.get(
        'https://api.stormglass.io/v2/tide/sea-level/point',
        params={
            'lat': latitude ,
            'lng': longitude,
            'start': start.to('PST').timestamp(), # Convert to PST timestamp
        },
        headers={
            'Authorization': get_api_key()
        }
    )
    response.raise_for_status()
    return response.json()

def fetch_forecast_data(latitude: float, longitude: float, requested_data_points: List[str]) -> Dict:
    """
    Fetches forecast data for a given latitude and longitude

    The function queries the Stormglass API to retrieve the desired forecast data for the provided geographic coordinates
    The start time for the data is set to the current day at midnight adjusted to PST

    Args:
        latitude (float): The latitude of the location
        longitude (float): The longitude of the location
        requested_data_points (List[str]): A list of strings specifying the desired data

    Returns:
        dict: A JSON response contianing the tide data

    Raises:
        requests.exceptions.HTTPError: If the HTTP request was unsuccessful
    """
    start = arrow.now().floor('day')
    response = requests.get(
        'https://api.stormglass.io/v2/weather/point',
        params={
            'lat': latitude,
            'lng': longitude,
            'params': ','.join(requested_data_points),
            'start': start.to('PST').timestamp(),
        },
        headers={
            'Authorization': get_api_key()
        }
    )
    response.raise_for_status()
    return response.json()
