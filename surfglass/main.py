class SurfBreakLocation:
    """
    Class to represent the location of a surf break

    Attributes:
        name (str): The name of the break
        coordinates (Coordinates): The geographical coordinates of the break
    """
    def __init__(self, name: str, coordinates: Coordinates):
        """
        Initialize a new SurfBreakLocation object

        Args:
            name (str): The name to set
            coordinates (Coordinates): The coordinates to set
        """
        self.name = name
        self.coordinates = coordinates


