from time import time
from uszipcode import SearchEngine
import yaml


def test_table_has_rows(engine, table_name):
    """
    Test whether a table has any rows, raising a ValueError if it does not

    Args:
        engine: psycopg2 engine connected to a database
        table_name: table to test

    Returns:
        True if successful
    """
    cur = engine.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    results = cur.fetchone()
    if results[0] > 0:
        return True
    else:
        raise ValueError(f"Table {table_name} has no data")


def test_table_has_no_rows(engine, table_name):
    """
    Test whether a table has any rows, raising a ValueError if it does not

    Args:
        engine: psycopg2 engine connected to a database
        table_name: table to test

    Returns:
        True if successful
    """
    cur = engine.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    results = cur.fetchone()
    if results[0] == 0:
        return True
    else:
        raise ValueError(f"Table {table_name} has data")


def lat_long_to_zip(latitude, longitude, search=None):
    """
    Converts coordinates of latitude and longitude to zip code using uszipcode package

    Args:
        latitude (float, str):
        longitude (float, str):
        search: uszipcode SearchEngine object (to avoid rebuilding one every query), or None
                (to build one by default)

    Returns:

    """
    latitude = float(latitude)
    longitude = float(longitude)
    if not search:
        search = SearchEngine(simple_zipcode=True)
    return search.by_coordinates(latitude, longitude, returns=1)[0].zipcode


def load_settings(secrets='secrets.yml', data_cfg='data.yml'):
    """
    Loads and returns settings files

    Args:
        secrets (str): Filename for secrets
        data_cfg (str): Filename for data settings (S3 locations, etc.,)

    Returns:
        Tuple of yaml objects for (secrets, data_cfg)
    """
    with open(secrets, 'r') as stream:
        secrets = yaml.safe_load(stream)
    with open(data_cfg, 'r') as stream:
        data_cfg = yaml.safe_load(stream)
    return secrets, data_cfg


class Timer:
    def __init__(self, enter_message=None, exit_message=None):
        """
        Construct a simple timer class.

        This can also be used as a context manager in a with block, eg:
            with Timer():
                pass

        Args:
            enter_message (str): If used as context manager, this message is printed at enter
            exit_message (str): If used as a context manager, this message is prepended to the context exit message
        """
        self.reference_time = None
        self.reset()
        if exit_message is None:
            self.exit_message = ""
        else:
            self.exit_message = str(exit_message) + ": "

        if enter_message is None:
            self.enter_message = None
        else:
            self.enter_message = str(enter_message)

    def elapsed(self):
        """
        Return the time elapsed between when this object was instantiated (or last reset) and now

        Returns:
            (float): Time elapsed in seconds
        """
        return time.perf_counter() - self.reference_time

    def reset(self):
        """
        Reset the reference timepoint to now

        Returns:
            None
        """
        self.reference_time = time.perf_counter()

    def __enter__(self):
        self.reset()
        if self.enter_message:
            print(self.enter_message)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"{self.exit_message}Process took {self.elapsed():.1f}s")


