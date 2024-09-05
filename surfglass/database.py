import sqlite3

CREATE_LOCATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL
);
"""

CREATE_UPDATES_TABLE = """
CREATE TABLE IF NOT EXISTS updates (
        id INTEGER PRIMARY KEY,
        time TEXT NOT NULL
        );
"""

CREATE_FORECASTS_TABLE = """
CREATE TABLE IF NOT EXISTS forecasts (
    id INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL,
    time TEXT NOT NULL,
    tide REAL,
    air_temp REAL,
    cloud_cover REAL,
    current_direction REAL,
    current_speed REAL,
    gust REAL,
    swell_direction REAL,
    swell_height REAL,
    swell_period REAL,
    secondary_swell_direction REAL,
    secondary_swell_height REAL,
    secondary_swell_period REAL,
    visibility REAL,
    wave_direction REAL,
    wave_height REAL,
    wave_period REAL,
    wind_wave_direction REAL,
    wind_wave_height REAL,
    wind_wave_period REAL,
    wind_direction REAL,
    wind_direction1000hpa REAL,
    wind_speed REAL,
    wind_speed1000hpa REAL,
    FOREIGN KEY (location_id) REFERENCES locations (id)
);
"""

def create_connection(db_file):
    """Create a database connection to SQLite database specified by db_file"""
    connection = sqlite3.connect(db_file)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection

def create_locations_table(connection):
    """Create the locations table in the database with the supplied connection"""
    with connection:
        connection.execute(CREATE_LOCATIONS_TABLE)

def create_updates_table(connection):
    """Create the updates table in the database with the supplied connection"""
    with connection:
        connection.execute(CREATE_UPDATES_TABLE)

def create_forecasts_table(connection):
    """Create the forecasts table in the database with the supplied connection"""
    with connection:
        connection.execute(CREATE_FORECASTS_TABLE)

def create_all_tables(connection):
    create_locations_table(connection)
    create_updates_table(connection)
    create_forecasts_table(connection)