import pytest
import requests_mock
from surfglass.coordinates import Coordinates
from surfglass.requests import get_api_key, fetch_tide_data

def test_get_api_key():
    """Test that the API key was properly imported"""
    assert get_api_key != None

def test_fetch_tide_data():
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
