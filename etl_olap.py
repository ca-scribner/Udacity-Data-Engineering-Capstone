import argparse
import psycopg2

from sql_queries import insert_olap_table_queries, drop_olap_table_queries, create_olap_table_queries
from utilities import test_table_has_rows, test_table_has_no_rows, load_settings, Timer, logging_argparse_kwargs, \
    logging_argparse_args, get_logger

logger = get_logger(name=__file__)


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


def insert_check_tables(engine):
    """
    Inserts staged data into production tables
    
    Includes checks to ensure tables are empty before load and have content after load
    
    Args:
        engine: psycopg2 engine connected to postgres database
    """
    with Timer(print_function=logger.info,
               enter_message="Loading and checking OLAP tables",
               exit_message="\t--> load and check complete"):
        for table_name, q in insert_olap_table_queries.items():
            logger.info(f"\tInserting and checking table {table_name}")
            logger.debug(f"\tq = {q}")
            load_check_table(engine, table_name, q)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Stage S3 data to Postgres and then insert it into production tables")
    parser.add_argument(
        '--db',
        action="store",
        default="postgres",
        help="Name of db credentials in secrets.yaml.  Use this to point at different dbs (postgres, redshift, ...)"
    )
    parser.add_argument(
        '--drop_before_load',
        action="store_true",
        help="If set, will drop/receate the target table before loading.  Useful for debugging"
    )
    parser.add_argument(*logging_argparse_args, **logging_argparse_kwargs)
    return parser.parse_args()


def drop_load_table(engine):
    """
    Drops and creates the OLAP tables

    Args:
        engine: psycopg2 engine connected to postgres database
    """
    cur = engine.cursor()

    for name, q in drop_olap_table_queries.items():
        logger.info(f"\tDropping {name}")
    cur.execute(q)

    for name, q in create_olap_table_queries.items():
        logger.info(f"\tCreating {name}")
    cur.execute(q)
    engine.commit()


if __name__ == "__main__":
    """
    Load OLAP tables from existing OLTP database
    """
    args = parse_arguments()

    if args.set_logging_level:
        logger.setLevel(str(args.set_logging_level).upper())

    secrets, data_cfg = load_settings()

    engine = psycopg2.connect(
        database=secrets[args.db]["database"],
        user=secrets[args.db]["user"],
        password=secrets[args.db]["password"],
        host=secrets[args.db]["host"],
        port=secrets[args.db]["port"],
    )

    if args.drop_before_load:
        drop_load_table(engine)

    insert_check_tables(engine)
