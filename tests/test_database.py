import pytest
import sqlite3
import surfglass.database as Database

LOCATIONS_EXPECTED_COLUMNS = ['id', 'name', 'latitude', 'longitude']
UPDATES_EXPECTED_COLUMNS = ['id', 'time']
FORECASTS_EXPECTED_COLUMNS = [
    'id', 'location_id', 'time', 'tide', 'air_temp', 'cloud_cover', 
    'current_direction', 'current_speed', 'gust', 'swell_direction', 
    'swell_height', 'swell_period', 'secondary_swell_direction', 
    'secondary_swell_height', 'secondary_swell_period', 'visibility', 
    'wave_direction', 'wave_height', 'wave_period', 'wind_wave_direction', 
    'wind_wave_height', 'wind_wave_period', 'wind_direction', 
    'wind_direction1000hpa', 'wind_speed', 'wind_speed1000hpa'
]

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
    result = connection.execute("""
        SELECT name 
        FROM sqlite_master 
        WHERE type='table' AND name='locations';
    """)
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
    result = connection.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name='updates';
    """)
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
    result = connection.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name='forecasts';
    """)
    assert result.fetchone is not None, "forecasts table not found"

    # Check if the table has the right schema
    result = connection.execute("PRAGMA table_info(forecasts);")
    columns = [row[1] for row in result.fetchall()]
    for column in FORECASTS_EXPECTED_COLUMNS:
        assert column in columns, f"The forecasts table is missing a column: {column}"

