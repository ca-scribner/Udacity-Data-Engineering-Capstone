import psycopg2
import argparse
import yaml
from uszipcode import SearchEngine

from sql_postgres import insert_table_queries, load_staging_queries, get_station_latitude_longitude, insert_station_zip
from utilities import test_table_has_rows, test_table_has_no_rows, lat_long_to_zip


def load_check_table(engine, table_name, query, check_before=True, check_after=True):
    """
    Executes a table load query, checking the table is empty before and not after

    Args:
        engine: psycopg2 engine connected to postgres database
        table_name (str): Name of destination table
        query (str): Query that will load destination table
        check_before (bool): If true, checks if a table is empty before load
        check_after (bool): If true, checks if a table has data after load
    """
    # Test destination table is empty
    if check_before:
        test_table_has_no_rows(engine, table_name)

    cur = engine.cursor()
    # Insert into table
    cur.execute(query)
        
    # Check for data in destination table
    if check_after:
        test_table_has_rows(engine, table_name)

    engine.commit()

    
def populate_query(q, data_cfg, secrets):
    q_settings = dict(
        source_format=data_cfg["source_format"],
        bucket=data_cfg["bucket"],
        key=data_cfg["key"],
        region=data_cfg["region"],
        access_key=secrets["aws"]["access_key"],
        secret_key=secrets["aws"]["secret_key"],
    )
    return q.format(**q_settings)


# TODO: REMAKE DOCSTRING
# (engine, bucket, key, region, source_format, access_key, secret_key):
def load_check_staging_tables(engine, data_sources, data_cfg, secrets):
    """
    Loads all staging tables from S3 source
    
    Includes checks to ensure tables are empty before load and have content after load
    
    Args:
        engine: psycopg2 engine connected to postgres database
        bucket (str): AWS S3 bucket for source data
        key (str): AWS S3 key for source data
        region (str): AWS S3 region for source data
        source_format (str): Postgres copy command formatted source file format string.  For example: "(FORMAT CSV, HEADER TRUE)"
        access_key (str): AWS access key that can access S3 data
        secret_ke (str): AWS secret key that can access S3 data
    """
    print("Loading and checking staging tables")
    
    # Stage sales
    table_name = "staging_sales"
    print(f"\tLoading table {table_name}")

    q = load_staging_queries[table_name]
    q = populate_query(q, data_cfg[data_sources["sales"]], secrets)
    load_check_table(engine, table_name, q)

    # Stage weather
    table_name = "staging_weather"
    print(f"\tLoading table {table_name}")
    
    # Check the table is empty before, but only check before the partial loads
    test_table_has_no_rows(engine, table_name)
    
    # Load data from all raw weather files
    for key in data_cfg[data_sources["weather"]]['key']:
        print(f"\t\tLoading key {key}")
        this_data_cfg = data_cfg[data_sources["sales"]]
        this_data_cfg['key'] = key
        q = load_staging_queries[table_name]
        q = populate_query(q, data_cfg[data_sources["sales"]], secrets)
        load_check_table(engine, table_name, q, check_before=False)


def insert_check_tables(engine):
    """
    Inserts staged data into production tables
    
    Includes checks to ensure tables are empty before load and have content after load
    
    Args:
        engine: psycopg2 engine connected to postgres database
    """
    print("Loading and checking production tables")

    for table_name, q in insert_table_queries.items():
        print(f"\tLoading table {table_name}")
        print(q)
        load_check_table(engine, table_name, q)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Stage S3 data to Postgres and then insert it into production tables")
    parser.add_argument(
        '-t', '--test', 
        action="store_true", 
        help="If set, use testing subset data rather than production data"
    )
    return parser.parse_args()
    
    
def load_settings():
    with open('secrets.yml', 'r') as stream:
        secrets = yaml.safe_load(stream)
    with open('data.yml', 'r') as stream:
        data_cfg = yaml.safe_load(stream)
    return secrets, data_cfg    


def add_zip_to_weather_stations(engine):
    """
    Adds zip code to all records in weather_stations that have lat/long but not zip codes

    Args:
        engine: psycopg2 engine connected to postgres database
    """
    print("Adding zip code to weather stations")
    cur = engine.cursor()
    cur.execute(get_station_latitude_longitude)
    records = cur.fetchall()

    search = SearchEngine(simple_zipcode=True)
    search = SearchEngine(simple_zipcode=True)
    f = lambda t: (t[0], lat_long_to_zip(t[1], t[2], search))
    zipcode_records = map(f, records)

    # Values in format needed for sql (series of "(stn_id, zip), (stn_id, zip), ...")
    values = ", ".join([str(r) for r in zipcode_records])

    insert_query = insert_station_zip.format(values=values)

    cur.execute(insert_query)
    engine.commit()


if __name__ == "__main__":
    """
    Stage S3 data to Postgres and then insert it into production tables
    """
    args = parse_arguments()

    data_sources = {
        'sales': 'sales_raw',
        'weather': 'weather_raw',
    }
    if args.test:
        data_sources['sales'] = data_sources['sales'] + "_test"
        
    secrets, data_cfg = load_settings()
    
    engine = psycopg2.connect(
        database=secrets["postgres"]["database"],
        user=secrets["postgres"]["user"],
        password=secrets["postgres"]["password"],
        host=secrets["postgres"]["host"],
        port=secrets["postgres"]["port"],
    )
    
    load_check_staging_tables(
        engine,
        data_sources,
        data_cfg,
        secrets
    )

    insert_check_tables(engine)

    add_zip_to_weather_stations(engine)

