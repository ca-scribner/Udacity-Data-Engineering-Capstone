import argparse

import psycopg2

from sql_queries import create_staging_table_queries, create_table_queries, drop_olap_table_queries, \
    create_olap_table_queries
from sql_queries import drop_staging_table_queries, drop_table_queries
from utilities import load_settings, Timer, logging_argparse_kwargs, \
    logging_argparse_args, get_logger

logger = get_logger(name=__file__)


def drop_tables(engine):
    """
    Drops all production and staging tables in database

    Args:
        engine: psycopg2 engine connected to postgres database
    """
    cur = engine.cursor()
    for name, q in drop_staging_table_queries.items():
        logger.info(f"\tDropping {name}")
        _execute_query(cur, q)

    for name, q in drop_olap_table_queries.items():
        logger.info(f"\tDropping {name}")
        _execute_query(cur, q)

    for name, q in drop_table_queries.items():
        logger.info(f"\tDropping {name}")
        _execute_query(cur, q)

    engine.commit()


def create_tables(engine):
    """
    Creates all production and staging tables in database
    
    Args:
        engine: psycopg2 engine connected to postgres database
    """
    cur = engine.cursor()
    for name, q in create_staging_table_queries.items():
        logger.info(f"\tCreating {name}")
        _execute_query(cur, q)

    for name, q in create_table_queries.items():
        logger.info(f"\tCreating {name}")
        _execute_query(cur, q)

    for name, q in create_olap_table_queries.items():
        logger.info(f"\tCreating {name}")
        _execute_query(cur, q)
    engine.commit()


def _execute_query(cur, q):
    logger.debug(f"\t\tquery = \n{q}")
    cur.execute(q)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Drops and creates tables for database")
    parser.add_argument(
        '--db',
        action="store",
        default="postgres",
        help="Name of db credentials in secrets.yaml.  Use this to point at different dbs (postgres, redshift, ...)"
    )
    parser.add_argument(*logging_argparse_args, **logging_argparse_kwargs)

    return parser.parse_args()


if __name__ == "__main__":
    """
    Drops and creates all tables in the postgres database
    """
    args = parse_arguments()

    if args.set_logging_level:
        logger.setLevel(str(args.set_logging_level).upper())

    # Get DB/login information
    secrets, _ = load_settings()

    logger.info(f"Connecting to DB {args.db}")
    engine = psycopg2.connect(
        database=secrets[args.db]["database"],
        user=secrets[args.db]["user"],
        password=secrets[args.db]["password"],
        host=secrets[args.db]["host"],
        port=secrets[args.db]["port"],
    )

    with Timer(enter_message="Dropping existing tables", exit_message="--> drop complete", print_function=logger.info):
        drop_tables(engine)

    with Timer(enter_message="Creating new tables", exit_message="--> table creation complete",
               print_function=logger.info):
        create_tables(engine)
