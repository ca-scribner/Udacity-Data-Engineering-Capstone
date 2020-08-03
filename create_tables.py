import argparse

import psycopg2

from sql_queries import create_staging_table_queries, create_table_queries, drop_olap_table_queries, \
    create_olap_table_queries
from sql_queries import drop_staging_table_queries, drop_table_queries
from utilities import load_settings


def drop_tables(engine):
    """
    Drops all production and staging tables in database

    Args:
        engine: psycopg2 engine connected to postgres database
    """
    print("Dropping existing tables")
    cur = engine.cursor()
    for name, q in drop_staging_table_queries.items():
        print(f"\tDropping {name}")
        cur.execute(q)

    for name, q in drop_olap_table_queries.items():
        print(f"\tDropping {name}")
        cur.execute(q)

    for name, q in drop_table_queries.items():
        print(f"\tDropping {name}")
        cur.execute(q)

    engine.commit()

    
def create_tables(engine):
    """
    Creates all production and staging tables in database
    
    Args:
        engine: psycopg2 engine connected to postgres database
    """
    print("Creating new tables")
    cur = engine.cursor()
    for name, q in create_staging_table_queries.items():
        cur.execute(q)

    for name, q in create_table_queries.items():
        print(f"\tCreating {name}")
        cur.execute(q)

    for name, q in create_olap_table_queries.items():
        print(f"\tCreating {name}")
        cur.execute(q)
    engine.commit()


def parse_arguments():
    parser = argparse.ArgumentParser(description="Drops and creates tables for database")
    parser.add_argument(
        '--db',
        action="store",
        default="postgres",
        help="Name of db credentials in secrets.yaml.  Use this to point at different dbs (postgres, redshift, ...)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    """
    Drops and creates all tables in the postgres database
    """
    args = parse_arguments()

    # Get DB/login information
    secrets, _ = load_settings()

    print(f"Connecting to DB {args.db}")
    engine = psycopg2.connect(
        database=secrets[args.db]["database"],
        user=secrets[args.db]["user"],
        password=secrets[args.db]["password"],
        host=secrets[args.db]["host"],
        port=secrets[args.db]["port"],
    )

    drop_tables(engine)
    create_tables(engine)
