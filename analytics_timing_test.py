import argparse

import psycopg2

from sql_queries import insert_olap_table_queries, drop_olap_table_queries, create_olap_table_queries, \
    analytical_queries, discardable_query
from utilities import test_table_has_rows, test_table_has_no_rows, load_settings, Timer


DEFAULT_N = 5


def run_analytical_queries(engine, n):
    """
    TODO
    
    Args:
        engine: psycopg2 engine connected to postgres database
        n (int): Number of iterations per query
    """
    cur = engine.cursor()

    # Discard a query in case there's an initial connect time
    with Timer(enter_message=f"Running a junk query in case there's an initial connect time",
               exit_message="\t--> junk query done"):
        cur.execute(discardable_query)

    for q_name, q in analytical_queries.items():
        with Timer(enter_message=f"Running query {q_name} {n} times", exit_message="\t--> batch run done"):
            for i in range(n):
                with Timer(exit_message=f"\t--> {i}"):
                    cur.execute(q)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Stage S3 data to Postgres and then insert it into production tables")
    parser.add_argument(
        '--db',
        action="store",
        default="postgres",
        help="Name of db credentials in secrets.yaml.  Use this to point at different dbs (postgres, redshift, ...)"
    )
    parser.add_argument(
        "-n",
        default=DEFAULT_N,
        help=f"Number of runs per query.  Default is {DEFAULT_N}"
    )
    return parser.parse_args()


if __name__ == "__main__":
    """
    Test analytical queries on OLAP and OLTP schemas
    """
    args = parse_arguments()

    secrets, data_cfg = load_settings()

    engine = psycopg2.connect(
        database=secrets[args.db]["database"],
        user=secrets[args.db]["user"],
        password=secrets[args.db]["password"],
        host=secrets[args.db]["host"],
        port=secrets[args.db]["port"],
    )

    run_analytical_queries(engine, args.n)
