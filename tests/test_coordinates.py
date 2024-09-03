import pytest
from surfglass.coordinates import Coordinates

def test_coordinates_valid():
    """Test that the Coordinates class correctly accepts valid latitude and longitude"""
    coordinates = Coordinates(latitude=-23.4, longitude=73.0)
    assert coordinates.latitude == -23.4
    assert coordinates.longitude == 73.0

def test_coordinates_invalid_latitude():
    """Test that the Coordinates class raises ValueError for invalid latitude"""
    with pytest.raises(ValueError) as execution_info:
        Coordinates(latitude=-91.0, longitude=73.0)
    assert "Invalid latitude" in str(execution_info.value)

def test_coordinates_invalid_longitude():
    """Test that the Coordinates class raises ValueError for invalid longitude"""
    with pytest.raises(ValueError) as execution_info:
        Coordinates(latitude=-23.4, longitude=181.0)
    assert "Invalid longitude" in str(execution_info.value)
