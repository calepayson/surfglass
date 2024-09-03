class Coordinates:
    """
    Class to represent geographical coordinates

    Attributes:
        latitude (float): Latitude of the coordinate, must be between -90 and 90.
        longitude (float): Longitude of the coordinate, must be between -180 and 180
    """
    def __init__(self, latitude: float, longitude: float):
        """
        Initialize a new Coordinates object, validating the provided latitude and longitude

        Args:
            latitude (float): The latitude value to set.
            longitude (float): The longitude value to set.

        Raises:
            ValueError: If latitude or longitude is out of the valid range
        """
        if not (-90 <= latitude <= 90):
            raise ValueError(f"Invalid latitude: {latitude}. Must be between -90 and 90.")
        if not (-180 <= longitude <= 180):
            raise ValueError(f"Invalid longitude: {longitude}. Must be between -180 and 180.")

        self.latitude = latitude
        self.longitude = longitude
