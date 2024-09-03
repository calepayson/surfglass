import pytest
from surfglass.api_key import get_api_key

def test_get_api_key():
    """Test that the API key was properly imported"""
    assert get_api_key != None
