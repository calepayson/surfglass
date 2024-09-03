from dotenv import load_dotenv
from typing import List
import arrow
import os
import requests

def get_api_key():
    load_dotenv()
    return os.getenv('API_KEY')

def fetch_tide_data(latitude: float, longitude: float) -> dict:
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

def fetch_forecast_data(latitude: float, longitude: float, requested_data_points: List[str]) -> dict:
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
