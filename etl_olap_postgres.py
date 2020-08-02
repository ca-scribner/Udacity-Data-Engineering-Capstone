import psycopg2

from sql_postgres import insert_olap_table_queries
from utilities import test_table_has_rows, test_table_has_no_rows, load_settings


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
    print("Loading and checking OLAP tables")

    for table_name, q in insert_olap_table_queries.items():
        load_check_table(engine, table_name, q)


if __name__ == "__main__":
    """
    Load OLAP tables from existing OLTP database
    """
    secrets, data_cfg = load_settings()
    
    engine = psycopg2.connect(
        database=secrets["postgres"]["database"],
        user=secrets["postgres"]["user"],
        password=secrets["postgres"]["password"],
        host=secrets["postgres"]["host"],
        port=secrets["postgres"]["port"],
    )

    insert_check_tables(engine)
