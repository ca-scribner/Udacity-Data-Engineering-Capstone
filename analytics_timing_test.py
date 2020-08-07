import argparse
import logging
import psycopg2

from sql_queries import analytical_queries, discardable_query
from utilities import load_settings, Timer, logging_datefmt, logging_format, logging_argparse_kwargs, \
    logging_argparse_args

logging.basicConfig(format=logging_format,
                    datefmt=logging_datefmt)
logger = logging.getLogger(__file__)

DEFAULT_N = 5


def run_analytical_queries(engine, n):
    """
    TODO
    
    Args:
        engine: psycopg2 engine connected to postgres database
        n (int): Number of iterations per query
    """
    cur = engine.cursor()

    for q_name, q in analytical_queries.items():
        # Discard a query in case there's an initial connect time
        with Timer(enter_message=f"Running a junk query in case there's an initial connect time",
                   exit_message="\t--> junk query done",
                   print_function=logger.info,
                   ):
            cur.execute(discardable_query)
        with Timer(enter_message=f"Running query {q_name} {n} times", exit_message="\t--> batch run done",
                   print_function=logger.info,
                  ):
            logger.debug(f"query definition:\n{q}")
            for i in range(n):
                with Timer(exit_message=f"\t--> {i}",
                           print_function=logger.info,
                           ):
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
    parser.add_argument(*logging_argparse_args, **logging_argparse_kwargs)

    return parser.parse_args()


if __name__ == "__main__":
    """
    Test analytical queries on OLAP and OLTP schemas
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

    run_analytical_queries(engine, args.n)
