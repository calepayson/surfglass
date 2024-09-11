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
    'id', 'location_id', 'update_id', 'time', 'tide', 'air_temp', 'cloud_cover', 
    'current_direction', 'current_speed', 'gust', 'swell_direction', 
    'swell_height', 'swell_period', 'secondary_swell_direction', 
    'secondary_swell_height', 'secondary_swell_period', 'visibility', 
    'wave_direction', 'wave_height', 'wave_period', 'wind_wave_direction', 
    'wind_wave_height', 'wind_wave_period', 'wind_direction', 
    'wind_direction1000hpa', 'wind_speed', 'wind_speed1000hpa'
]

LOCATIONS_TEST_DATA = [
    ("Rodeo Beach", 37.83, -122.54),
    ("Ocean Beach", 37.77, -122.51)
]
LOCATIONS_DATA_QUERY = """
SELECT name, latitude, longitude
FROM locations
WHERE name = ?;
"""

UPDATES_TEST_DATA = [
    "2024-09-06",
    "2024-09-07"
]
UPDATES_DATA_QUERY = """
SELECT time
FROM updates
WHERE time = ?;
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
    Database.create_updates_table(connection)
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
    name, latitude, longitude = LOCATIONS_TEST_DATA[0]

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
    Database.create_locations_table(connection)

    # Mock locations and add them to the database
    for location in LOCATIONS_TEST_DATA:
        name, latitude, longitude = location
        Database.add_location(connection, name, latitude, longitude)

    # Check that the database is read correctly
    locations = Database.get_all_locations(connection)
    assert len(locations) == len(LOCATIONS_TEST_DATA), "Number of locations retrieved does not match expected"

    # Check each retrieved location against the test data
    for location in locations:
        assert (location[1], location[2], location[3]) in LOCATIONS_TEST_DATA, f"({locations[1]}, {locations[2]}, {locations[3]}) not found in test data"

def test_get_location_by_name(tmp_path):
    """Test that a location can be retrieved by name"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_locations_table(connection)

    # Mock locations and add them to the database
    for location in LOCATIONS_TEST_DATA:
        name, latitude, longitude = location
        Database.add_location(connection, name, latitude, longitude)

    # Grab the location
    name, latitude, longitude = LOCATIONS_TEST_DATA[0]
    returned_location = Database.get_location_by_name(connection, name)

    # Check that the retrieved location matches the test data
    assert returned_location[0][1] == name, f"Expected name: {name}, Got: {returned_location[0][1]}"
    assert returned_location[0][2] == latitude, f"Expected latitude: {name}, Got: {returned_location[0][2]}"
    assert returned_location[0][3] == longitude, f"Expected longitude: {name}, Got: {returned_location[0][3]}"

def test_delete_location_by_name(tmp_path):
    """Test that a location can be deleted by name"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_locations_table(connection)

    # Mock locations and add them to the database
    for location in LOCATIONS_TEST_DATA:
        name, latitude, longitude = location
        Database.add_location(connection, name, latitude, longitude)

    # Delete a location
    target_name, target_latitude, target_longitude = LOCATIONS_TEST_DATA[0]
    Database.delete_location_by_name(connection, target_name)

    # Grab all the remaining locations
    locations = Database.get_all_locations(connection)

    # Check that the deleted location is not in them
    for location in locations:
        assert target_name != location[1], f"Deleted: {target_name} but found: {location[1]}"
        assert target_latitude != location[2], f"Deleted: {target_latitude} but found: {location[2]}"
        assert target_longitude != location[3], f"Deleted: {target_longitude} but found: {location[3]}"

def test_update_location_by_name(tmp_path):
    """Test that a location can be updated by name"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_locations_table(connection)

    # Mock locations and add them to the database
    for location in LOCATIONS_TEST_DATA:
        name, latitude, longitude = location
        Database.add_location(connection, name, latitude, longitude)

    # Update a location
    name = LOCATIONS_TEST_DATA[0][0]
    new_latitude, new_longitude = 42.0, 69.0
    Database.update_location_by_name(connection, name, new_latitude, new_longitude)

    # Lookup location
    location = Database.get_location_by_name(connection, name)[0]

    # Check that the data was properly updated
    assert location[1] == name, "Name not found"
    assert location[2] == new_latitude, f"Expected: {new_latitude}, Got: {location[2]}"
    assert location[3] == new_longitude, f"Expected: {new_longitude}, Got: {location[3]}"

def test_add_update(tmp_path):
    """Test that an update is successfully added"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_updates_table(connection)

    # Mock updates and add them to the database
    for update in UPDATES_TEST_DATA:
        time = update
        Database.add_update(connection, time)

    # Retrieve the update
    expected_time = UPDATES_TEST_DATA[0]
    cursor = connection.cursor()
    cursor.execute(UPDATES_DATA_QUERY, (expected_time,))
    update = cursor.fetchone()

    # Check that the location was found
    assert update is not None

    # Check that the location matches the input data
    assert update[0] == expected_time, f"Expected time: {expected_time}, Got: {update[0]}"

def test_get_latest_update(tmp_path):
    """Test that the latest update is successfully returned"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_updates_table(connection)

    # Mock updates and add them to the database
    for update in UPDATES_TEST_DATA:
        time = update
        Database.add_update(connection, time)

    # Retrieve the latest update
    expected_update = UPDATES_TEST_DATA[-1]
    latest_update = Database.get_latest_update(connection)

    # Check that an update was found
    assert latest_update is not None

    # Check that the update matches the expected
    assert latest_update[0][1] == expected_update, f"Expected time: {expected_update}, Got {latest_update}"

def test_add_forecast(tmp_path):
    """Test that forecasts can be successfully added"""
    db_file = tmp_path / "test.db"
    connection = Database.create_connection(str(db_file))
    Database.create_all_tables(connection)
    
    # Mock updates and add them to the database
    for update in UPDATES_TEST_DATA:
        time = update
        Database.add_update(connection, time)

    # Mock locations and add them to the database
    for location in LOCATIONS_TEST_DATA:
        name, latitude, longitude = location
        Database.add_location(connection, name, latitude, longitude)

    # Mock a forecast data entry
    forecast_data = (
        1,  # location_id
        1,  # update_id
        "2024-09-06 12:00:00",  # time
        2.5,  # tide
        18.0,  # air_temp
        75.0,  # cloud_cover
        135,  # current_direction
        1.5,  # current_speed
        25.0,  # gust
        220,  # swell_direction
        1.8,  # swell_height
        12,  # swell_period
        190,  # secondary_swell_direction
        1.2,  # secondary_swell_height
        10,  # secondary_swell_period
        10.0,  # visibility
        200,  # wave_direction
        1.5,  # wave_height
        8,  # wave_period
        180,  # wind_wave_direction
        1.0,  # wind_wave_height
        7,  # wind_wave_period
        180,  # wind_direction
        170,  # wind_direction1000hpa
        12.0,  # wind_speed
        10.0  # wind_speed1000hpa
    )

    # Add the forecast to the database
    Database.add_forecast(connection, *forecast_data)

    # Retrieve the forecast
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM forecasts WHERE location_id = ? AND time = ?", (forecast_data[0], forecast_data[2]))
    forecast = cursor.fetchone()

    # Check that the forecast was found
    assert forecast is not None

    # Check that each value matches the input data
    for i, value in enumerate(forecast_data):
        assert forecast[i + 1] == value, f"Expected: {value}, Got: {forecast[i + 1]}"

