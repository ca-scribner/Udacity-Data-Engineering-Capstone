from uszipcode import SearchEngine


def test_table_has_rows(engine, table_name):
    cur = engine.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    results = cur.fetchone()
    if results[0] > 0:
        return True
    else:
        raise ValueError(f"Table {table_name} has no data")


def test_table_has_no_rows(engine, table_name):
    cur = engine.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    results = cur.fetchone()
    if results[0] == 0:
        return True
    else:
        raise ValueError(f"Table {table_name} has data")


def lat_long_to_zip(latitude, longitude, search=None):
    latitude = float(latitude)
    longitude = float(longitude)
    if not search:
        search = SearchEngine(simple_zipcode=True)
    return search.by_coordinates(latitude, longitude, returns=1)[0].zipcode
