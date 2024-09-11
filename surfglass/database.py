import sqlite3

CREATE_LOCATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL
);
"""
ADD_LOCATION = "INSERT INTO locations (name, latitude, longitude) VALUES (?, ?, ?);"
GET_ALL_LOCATIONS = "SELECT * FROM locations;"
GET_LOCATION_BY_NAME = "SELECT * FROM locations WHERE name = ?;"
DELETE_LOCATION_BY_NAME = "DELETE FROM locations WHERE name = ?;"
UPDATE_LOCATION_BY_NAME = """
UPDATE locations
SET latitude = ?, longitude = ?
WHERE name = ?;
"""

CREATE_UPDATES_TABLE = """
CREATE TABLE IF NOT EXISTS updates (
        id INTEGER PRIMARY KEY,
        time TEXT NOT NULL
        );
"""
ADD_UPDATE = "INSERT INTO updates (time) VALUES (?);"
GET_LATEST_UPDATE = """
SELECT *
FROM updates
ORDER BY id DESC
LIMIT 1;
"""

CREATE_FORECASTS_TABLE = """
CREATE TABLE IF NOT EXISTS forecasts (
    id INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL,
    update_id INTEGER NOT NULL,
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
    FOREIGN KEY (update_id) REFERENCES updates (id)
);
"""
ADD_FORECAST = """
INSERT INTO forecasts (
    location_id,
    update_id,
    time,
    tide,
    air_temp,
    cloud_cover,
    current_direction,
    current_speed,
    gust,
    swell_direction,
    swell_height,
    swell_period,
    secondary_swell_direction,
    secondary_swell_height,
    secondary_swell_period,
    visibility,
    wave_direction,
    wave_height,
    wave_period,
    wind_wave_direction,
    wind_wave_height,
    wind_wave_period,
    wind_direction,
    wind_direction1000hpa,
    wind_speed,
    wind_speed1000hpa)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

###############################
# DATABASE AND TABLE CREATION #
###############################

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
    """Create each table if it does not already exist"""
    create_locations_table(connection)
    create_updates_table(connection)
    create_forecasts_table(connection)

#######################
# LOCATION OPERATIONS #
#######################

def add_location(connection, name, latitude, longitude):
    """Add a location to the locations table"""
    with connection:
        connection.execute(ADD_LOCATION, (name, latitude, longitude))

def get_all_locations(connection):
    """Get all the locations from the locations table"""
    with connection:
        return connection.execute(GET_ALL_LOCATIONS).fetchall()

def get_location_by_name(connection, name):
    """Get a location that matches the provided name"""
    with connection:
        return connection.execute(GET_LOCATION_BY_NAME, (name,)).fetchall()

def delete_location_by_name(connection, name):
    """Delete a location that matches the provided name"""
    with connection:
        connection.execute(DELETE_LOCATION_BY_NAME, (name,))

def update_location_by_name(connection, name, latitude, longitude):
    """Update a location that matches the provided name with the provided lat/long"""
    with connection:
        connection.execute(UPDATE_LOCATION_BY_NAME, (latitude, longitude, name))

#####################
# UPDATE OPERATIONS # 
#####################

def add_update(connection, time):
    """Add an update to the updates table"""
    with connection:
        connection.execute(ADD_UPDATE, (time,))

def get_latest_update(connection):
    """Get the latest update"""
    with connection:
        return connection.execute(GET_LATEST_UPDATE).fetchall()

#######################
# FORECAST OPERATIONS #
#######################

def add_forecast(
    connection,
    location_id,
    update_id,
    time,
    tide,
    air_temp,
    cloud_cover,
    current_direction,
    current_speed,
    gust,
    swell_direction,
    swell_height,
    swell_period,
    secondary_swell_direction,
    secondary_swell_height,
    secondary_swell_period,
    visibility,
    wave_direction,
    wave_height,
    wave_period,
    wind_wave_direction,
    wind_wave_height,
    wind_wave_period,
    wind_direction,
    wind_direction1000hpa,
    wind_speed,
    wind_speed1000hpa
):
    """Add a forecast to the forecasts table"""
    with connection:
        connection.execute(
            ADD_FORECAST,
            (
                location_id,
                update_id,
                time,
                tide,
                air_temp,
                cloud_cover,
                current_direction,
                current_speed,
                gust,
                swell_direction,
                swell_height,
                swell_period,
                secondary_swell_direction,
                secondary_swell_height,
                secondary_swell_period,
                visibility,
                wave_direction,
                wave_height,
                wave_period,
                wind_wave_direction,
                wind_wave_height,
                wind_wave_period,
                wind_direction,
                wind_direction1000hpa,
                wind_speed,
                wind_speed1000hpa,
            )
        )

