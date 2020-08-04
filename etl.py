import psycopg2
import argparse
from uszipcode import SearchEngine
import awswrangler as wr

from sql_queries import insert_table_queries_postgres, get_station_latitude_longitude, insert_station_zip, \
    load_staging_queries_postgres, load_staging_queries_redshift
from utilities import test_table_has_rows, test_table_has_no_rows, lat_long_to_zip, load_settings, Timer


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


def get_load_query(table_name, data_cfg, file_to_stage, secrets, db_type="postgres"):
    if file_to_stage.startswith('s3://'):
        # Strip leading s3://{bucket}/
        file_to_stage = "/".join(file_to_stage.lstrip("s3://").split("/")[1:])

    q_settings = dict(
        source_format=data_cfg[f"source_format_{db_type}"],
        bucket=data_cfg["bucket"],
        key=file_to_stage,
        region=data_cfg["region"],
    )
    if db_type == "postgres":
        q_settings["access_key"] = secrets["aws"]["access_key"]
        q_settings["secret_key"] = secrets["aws"]["secret_key"]
    elif db_type == "redshift":
        q_settings["iam"] = secrets["redshift"]["arn"]
    else:
        raise ValueError(f"Unknown db_type {db_type}")

    staging_query_map = {
        'redshift': load_staging_queries_redshift,
        'postgres': load_staging_queries_postgres
    }
    q = staging_query_map[db_type][table_name]
    return q.format(**q_settings)


# TODO: REMAKE DOCSTRING
# (engine, bucket, key, region, source_format, access_key, secret_key):
def load_check_staging_tables(engine, data_sources, data_cfg, secrets, db_type="postgres", sales_raw_data_type="csv", ):
    """
    TODO: REDO THIS DOCSTRING
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

    # Find staging files
    with Timer(enter_message=f"\tLoading table {table_name}", exit_message=f"\t--> load {table_name} complete"):
        this_data_cfg = data_cfg[data_sources["sales"]][sales_raw_data_type]
        load_check_from_s3_prefix(this_data_cfg, db_type, engine, secrets, table_name)

    # Stage weather
    table_name = "staging_weather"

    # Check the table is empty before loading
    test_table_has_no_rows(engine, table_name)
    
    # Load data from all raw weather files
    with Timer(enter_message=f"\tLoading table {table_name}", exit_message=f"\t--> load {table_name} complete"):
        this_data_cfg = data_cfg[data_sources["weather"]]
        load_check_from_s3_prefix(this_data_cfg, db_type, engine, secrets, table_name)


def load_check_from_s3_prefix(data_cfg, db_type, engine, secrets, table_name):
    """
    Checks if a staging table is empty then loads data from one or more files specified by an S3 location

    TODO: docstrong

    Args:
        data_cfg:
        db_type:
        engine:
        secrets:
        table_name:

    Returns:

    """
    # Check the staging table is empty before loading
    test_table_has_no_rows(engine, table_name)
    # Glob all raw files and stage each separately
    path = f"s3://{data_cfg['bucket']}/{data_cfg['key_base']}"
    files_to_stage = wr.s3.list_objects(path=path)

    if len(files_to_stage) == 0:
        raise ValueError(f"Found no files to load for {table_name} in {path}")

    for file_to_stage in files_to_stage:
        with Timer(enter_message=f"\t\tstaging file .../{file_to_stage.split('/')[-1]}", exit_message="\t\t--> "):
            q = get_load_query(table_name, data_cfg, file_to_stage, secrets, db_type)
            load_check_table(engine, table_name, q, check_before=False)


def insert_check_tables(engine):
    """
    Inserts staged data into production tables
    
    Includes checks to ensure tables are empty before load and have content after load
    
    Args:
        engine: psycopg2 engine connected to postgres database
    """
    print("Loading and checking production tables")

    for table_name, q in insert_table_queries_postgres.items():
        with Timer(enter_message=f"\tInserting into table {table_name}", exit_message=f"\t--> insert into {table_name} complete"):
            load_check_table(engine, table_name, q)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Stage S3 data to Postgres and then insert it into production tables")
    parser.add_argument(
        '-t', '--test', 
        action="store_true", 
        help="If set, use testing subset data rather than production data"
    )
    parser.add_argument(
        '--db',
        action="store",
        default="postgres",
        help="Name of db credentials in secrets.yaml.  Use this to point at different dbs (postgres, redshift, ...)"
    )
    parser.add_argument(
        '--sales_raw_data_type',
        default="csv",
        help="File type to load sales data from.  "
             "Can be 'csv' for postgres, or either of ('csv', 'parquet') for redshift"
    )
    return parser.parse_args()


def add_zip_to_weather_stations(engine):
    """
    Adds zip code to all records in weather_stations that have lat/long but not zip codes

    Args:
        engine: psycopg2 engine connected to postgres database
    """
    cur = engine.cursor()
    cur.execute(get_station_latitude_longitude)
    records = cur.fetchall()

    search = SearchEngine(simple_zipcode=True)
    zip_fun = lambda t: (t[0], lat_long_to_zip(t[1], t[2], search))
    zipcode_records = map(zip_fun, records)

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
        database=secrets[args.db]["database"],
        user=secrets[args.db]["user"],
        password=secrets[args.db]["password"],
        host=secrets[args.db]["host"],
        port=secrets[args.db]["port"],
    )

    with Timer(enter_message="Loading staging tables", exit_message="--> staging table load complete"):
        load_check_staging_tables(
            engine,
            data_sources,
            data_cfg,
            secrets,
            db_type=args.db,
            sales_raw_data_type=args.sales_raw_data_type
        )

    with Timer(enter_message="Inserting data into tables", exit_message="--> table insert complete"):
        insert_check_tables(engine)

    with Timer(enter_message="Adding zip code to weather stations table", exit_message="--> add zip complete"):
        add_zip_to_weather_stations(engine)

