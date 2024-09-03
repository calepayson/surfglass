import pytest
import requests_mock
from surfglass.requests import get_api_key, fetch_tide_data, fetch_forecast_data

def test_get_api_key():
    """Test that the API key was properly imported"""
    assert get_api_key != None

def test_fetch_tide_data():
    """Test that a proper request is used for tide data"""
    with requests_mock.Mocker() as m:
        m.get(
            'https://api.stormglass.io/v2/tide/sea-level/point',
            json={
                'status': 'success',
                'data': 'mocked data',
            }
        )

        response = fetch_tide_data(60.936, -42.69)

        assert response['status'] == 'success'
        assert response['data'] == 'mocked data'
        assert 'lat=60.936&lng=-42.69' in m.last_request.query

def test_fetch_forecast_data():
    """Test that a proper request is used for forecast data"""
    with requests_mock.Mocker() as m:
        m.get(
            'https://api.stormglass.io/v2/weather/point',
            json={
                'status': 'success',
                'data': 'mocked data',
            }
        )

        response = fetch_forecast_data(60.936, -42.69,["weather", "current", "swell"])
        print(m.last_request.query)

        assert response['status'] == 'success'
        assert response['data'] == 'mocked data'
        assert 'lat=60.936&lng=-42.69&params=weather%2ccurrent%2cswell' in m.last_request.query
