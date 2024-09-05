import pytest
import sqlite3
import surfglass.database as Database

LOCATIONS_TABLE_EXISTS = """
SELECT name 
FROM sqlite_master 
WHERE type='table' AND name='locations';
"""
LOCATIONS_EXPECTED_COLUMNS = ['id', 'name', 'latitude', 'longitude']

UPDATES_TABLE_EXISTS = """
SELECT name 
FROM sqlite_master 
WHERE type='table' AND name='updates';
"""
UPDATES_EXPECTED_COLUMNS = ['id', 'time']

FORECASTS_TABLE_EXISTS = """
SELECT name 
FROM sqlite_master 
WHERE type='table' AND name='forecasts';
"""
FORECASTS_EXPECTED_COLUMNS = [
    'id', 'location_id', 'time', 'tide', 'air_temp', 'cloud_cover', 
    'current_direction', 'current_speed', 'gust', 'swell_direction', 
    'swell_height', 'swell_period', 'secondary_swell_direction', 
    'secondary_swell_height', 'secondary_swell_period', 'visibility', 
    'wave_direction', 'wave_height', 'wave_period', 'wind_wave_direction', 
    'wind_wave_height', 'wind_wave_period', 'wind_direction', 
    'wind_direction1000hpa', 'wind_speed', 'wind_speed1000hpa'
]
LOCATIONS_TEST_DATA = ("Rodeo Beach", 37.83, -122.54)
LOCATIONS_DATA_QUERY = """
SELECT name, latitude, longitude
FROM locations
WHERE name = ?;
"""

def test_create_connection(tmp_path):
    """Test that create connection successfully establishes a connection to the database"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    assert connection is not None, "create_connection() returned 'None'"
    assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1, "Foreign keys not initialized"
    connection.close()

def test_create_locations_table(tmp_path):
    """Test that the locations table is successfully created"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_locations_table(connection)

    # Check if the table exists
    result = connection.execute(LOCATIONS_TABLE_EXISTS)
    assert result.fetchone() is not None, "locations table not found"

    # Check if the table has the right schema
    result = connection.execute("PRAGMA table_info(locations);")
    columns = [row[1] for row in result.fetchall()]
    for column in LOCATIONS_EXPECTED_COLUMNS:
        assert column in columns, f"The locations table is missing a column: {column}"
    connection.close()

def test_create_updates_table(tmp_path):
    """Test that the updates table is successfully created"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_updates_table(connection)

    # Check if the table exists
    result = connection.execute(UPDATES_TABLE_EXISTS)
    assert result.fetchone() is not None, "updates table not found"

    # Check if the table has the right schema
    result = connection.execute("PRAGMA table_info(updates);")
    columns = [row[1] for row in result.fetchall()]
    for column in UPDATES_EXPECTED_COLUMNS:
        assert column in columns, f"The updates table is missing a column: {column}"
    connection.close()

def test_create_forecasts_table(tmp_path):
    """Test that the forecasts table is successfully created"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_locations_table(connection)
    Database.create_forecasts_table(connection)

    # Check if the table exists
    result = connection.execute(FORECASTS_TABLE_EXISTS)
    assert result.fetchone is not None, "forecasts table not found"

    # Check if the table has the right schema
    result = connection.execute("PRAGMA table_info(forecasts);")
    columns = [row[1] for row in result.fetchall()]
    for column in FORECASTS_EXPECTED_COLUMNS:
        assert column in columns, f"The forecasts table is missing a column: {column}"

def test_create_all_tables(tmp_path):
    """Test that all tables are successfully created"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_all_tables(connection)

    # Check if all tables exist
    locations = connection.execute(LOCATIONS_TABLE_EXISTS)
    assert locations.fetchone is not None, "locations table not found"
    updates = connection.execute(UPDATES_TABLE_EXISTS)
    assert updates.fetchone is not None, "updates table not found"
    forecasts = connection.execute(FORECASTS_TABLE_EXISTS)
    assert forecasts.fetchone is not None, "forecasts table not found"

def test_add_location(tmp_path):
    """Test that locations can be successfully added"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_all_tables(connection)

    # Mock a location
    name, latitude, longitude = LOCATIONS_TEST_DATA

    # Add the location to the database
    Database.add_location(connection, name, latitude, longitude)

    # Retrieve the location
    cursor = connection.cursor()
    cursor.execute(LOCATIONS_DATA_QUERY, (name,))
    location = cursor.fetchone()

    # Check that the location was found
    assert location is not None

    # Check that the location matches the input data
    assert location[0] == name, f"Expected name: {name}, Got: {location[0]}"
    assert location[1] == latitude, f"Expected latitude: {latitude}, Got: {location[1]}"
    assert location[2] == longitude, f"Expected longitude: {longitude}, Got: {location[2]}"

def test_get_all_locations(tmp_path):
    """Test that locations can be successfully read"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_all_tables(connection)

    # Mock a location and add it to the database
    name, latitude, longitude = LOCATIONS_TEST_DATA
    Database.add_location(connection, name, latitude, longitude)

    # Read from the database
    locations = Database.get_all_locations(connection)
    returned_name, returned_latitude, returned_longitude = locations[0][1], locations[0][2], locations[0][3]

    # Check that the returned data matches the input data
    assert returned_name == name, f"Expected name: {name}, Got: {returned_name}"
    assert returned_latitude == latitude, f"Expected latitude: {latitude}, Got: {returned_latitude}"
    assert returned_longitude == longitude, f"Expected longitude: {longitude}, Got: {returned_longitude}"

