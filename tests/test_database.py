import pytest
import sqlite3
from surfglass.database import create_connection, create_locations_table, create_updates_table, create_forecasts_table

def test_create_connection(tmp_path):
    """Test that create connection successfully establishes a connection to the database"""
    db_file = tmp_path / "test.db"
    connection = create_connection(str(db_file))
    assert connection is not None, "create_connection() returned 'None'"
    assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1, "Foreign keys not initialized"
    connection.close()

def test_create_locations_table(tmp_path):
    """Test that the locations table is successfully created"""
    db_file = tmp_path / "test.db"
    connection = create_connection(str(db_file))
    create_locations_table(connection)

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
    expected_columns = ['id', 'name', 'latitude', 'longitude']
    for column in expected_columns:
        assert column in columns, f"The locations table is missing a column: {column}"
    connection.close()

def test_create_forecasts_table(tmp_path):
    """Test that the forecasts table is successfully created"""
    db_file = tmp_path / "test.db"
    connection = create_connection(str(db_file))
    create_locations_table(connection)
    create_forecasts_table(connection)

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
    expected_columns = [
        'id', 'location_id', 'time', 'tide', 'air_temp', 'cloud_cover',
        'current_direction', 'current_speed', 'gust', 'swell_direction',
        'swell_height', 'swell_period', 'secondary_swell_direction',
        'secondary_swell_height', 'secondary_swell_period', 'visibility',
        'wave_direction', 'wave_height', 'wave_period', 'wind_wave_direction',
        'wind_wave_height', 'wind_wave_period', 'wind_direction',
        'wind_direction1000hpa', 'wind_speed', 'wind_speed1000hpa'
    ]
    for column in expected_columns:
        assert column in columns, f"The forecasts table is missing a column: {column}"


def test_create_updates_table(tmp_path):
    """Test that the updates table is successfully created"""
    db_file = tmp_path / "test.db"
    connection = create_connection(str(db_file))
    create_updates_table(connection)

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
    expected_columns = ['id', 'time']
    for column in expected_columns:
        assert column in columns, f"The updates table is missing a column: {column}"
    connection.close()
