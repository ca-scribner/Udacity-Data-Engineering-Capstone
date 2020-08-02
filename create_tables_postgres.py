import psycopg2
import configparser

from sql_postgres import create_staging_table_queries, create_table_queries, drop_olap_table_queries, \
    create_olap_table_queries
from sql_postgres import drop_staging_table_queries, drop_table_queries


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

    for name, q in drop_table_queries.items():
        print(f"\tDropping {name}")
        cur.execute(q)

    for name, q in drop_olap_table_queries.items():
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
        print(f"\tCreating {name}")
        cur.execute(q)

    for name, q in create_table_queries.items():
        print(f"\tCreating {name}")
        cur.execute(q)

    for name, q in create_olap_table_queries.items():
        print(f"\tCreating {name}")
        cur.execute(q)
    engine.commit()


if __name__ == "__main__":
    """
    Drops and creates all tables in the postgres database
    """
    # Get DB/login information
    secrets = configparser.ConfigParser()
    secrets.read('secrets.cfg')

    print("Connecting to DB")
    engine = psycopg2.connect(
        database=secrets["postgres"]["database"],
        user=secrets["postgres"]["user"],
        password=secrets["postgres"]["password"],
        host=secrets["postgres"]["host"],
        port=secrets["postgres"]["port"],
    )

    drop_tables(engine)
    create_tables(engine)
