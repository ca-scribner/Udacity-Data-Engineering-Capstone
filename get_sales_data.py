import argparse
import pandas as pd
from sodapy import Socrata
import awswrangler as wr

from utilities import load_settings, Timer
from sql_queries import staging_sales_columns

# Map between the names used in this app and the original data source names
SALES_NAME_MAP_APP_TO_SOURCE = {
    "invoice_id": 'invoice_line_no',
    "date": 'date',
    "store_id": 'store',
    "store_name": 'name',
    "address": 'address',
    "city": 'city',
    "zip": 'zipcode',
    "store_location": 'store_location',
    "county_number": 'county_number',
    "county": 'county',
    "category_id": 'category',
    "category_name": 'category_name',
    "vendor_id": 'vendor_no',
    "vendor_name": 'vendor_name',
    "item_id": 'itemno',
    "item_description": 'im_desc',
    "pack": 'pack',
    "bottle_volume_ml": 'bottle_volume_ml',
    "bottle_cost": 'state_bottle_cost',
    "bottle_retail": 'state_bottle_retail',
    "bottles_sold": 'sale_bottles',
    "total_sale": 'sale_dollars',
    "volume_sold_liters": 'sale_liters',
    "volume_sold_gallons": 'sale_gallons',
}

SALES_NAME_MAP_SOURCE_TO_APP = {v: k for k, v in SALES_NAME_MAP_APP_TO_SOURCE.items()}

def parse_args():
    parser = argparse.ArgumentParser(description="Gets Iowa Liquor Sales data from API and stores to a bucket as "
                                                 "monthly files")

    parser.add_argument(
        'month_start',
        help='Date to start pulling data (inclusive, in YYYY-MM format)'
    )
    parser.add_argument(
        'month_end',
        help='Last month to pull data (exclusive, in YYYY-MM format)'
    )
    parser.add_argument(
        '--data_spec',
        default='sales_raw',
        help='Name of data specifications in data.yml.  This specifies the destination of the data'
    )
    parser.add_argument(
        '--no_csv',
        action='store_true',
        help='If set, will not output csv files to S3',
    )
    parser.add_argument(
        '--no_pq',
        action='store_true',
        help='If set, will not output parquet files to S3',
    )
    parser.add_argument(
        '--limit',
        action='store',
        help='Maximum number of records requested per month'
    )
    parser.add_argument(
        '--action_on_limit',
        action='store',
        default='raise',
        help='Action if record limit reached for a given month.  Options are "raise" or "warn"'
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    start_date = pd.to_datetime(args.month_start)
    end_date = pd.to_datetime(args.month_end)
    download_dates = pd.date_range(start_date, end_date, freq='MS')

    secrets, data_cfg = load_settings()

    source_client = Socrata("data.iowa.gov",
                            secrets['socrata']['access_key']
                            )

    columns = [SALES_NAME_MAP_APP_TO_SOURCE[name] for name in staging_sales_columns]
    select = ", ".join(columns)
    limit = int(args.limit)
    order = "date"
    source_key = "m3tr-qhgy"

    s3_additional_kwargs = dict(
        aws_access_key_id=secrets['aws']['access_key'],
        aws_secret_access_key=secrets['aws']['secret_key']
    )

    for start_date, end_date in zip(download_dates[:-1], download_dates[1:]):

        start_date_formatted = start_date.strftime("%Y-%m")
        end_date_formatted = end_date.strftime("%Y-%m")
        where = f"date >= '{start_date_formatted}' AND date < '{end_date_formatted}'"
        with Timer(enter_message=f"Downloading data where {where}", exit_message="--> download complete"):
            results = source_client.get(source_key,
                                        select=select,
                                        order="date",
                                        where=where,
                                        limit=limit,
                                 )

        # Catch if we maxed our return limit
        if len(results) == limit:
            if args.action_on_limit == 'warn':
                print("WARNING: dowload limit reached")
            elif args.action_on_limit == 'raise':
                raise ValueError("Error: Download limit reached")
            else:
                raise ValueError("Error: Download limit reached and invalid action_on_limit specified "
                                 f"({args.action_on_limit})")
        df = pd.DataFrame(results).rename(columns=SALES_NAME_MAP_SOURCE_TO_APP)
        url_template = f's3://{{bucket}}/{{key_base}}/{start_date.year:02d}/{start_date.month:02d}/{start_date.year:04d}-{start_date.month:02d}{{suffix}}'

        # Save to csv
        if not args.no_csv:
            this_data_cfg = data_cfg[args.data_spec]["csv"]
            with Timer(enter_message=f"Uploading csv data", exit_message="--> upload csv complete"):
                output_url = url_template.format(
                    bucket=this_data_cfg["bucket"],
                    key_base=this_data_cfg["key_base"],
                    suffix=this_data_cfg["suffix"]
                )

                # Could partition this data further (daily files)
                wr.s3.to_csv(
                    df=df,
                    path=output_url,
                    s3_additional_kwargs=s3_additional_kwargs,
                    index=False,  # Do not export empty index column
                )

        # Save to parquet
        if not args.no_pq:
            this_data_cfg = data_cfg[args.data_spec]["parquet"]
            with Timer(enter_message=f"Uploading parquet data", exit_message="--> upload csv complete"):
                output_url = url_template.format(
                    bucket=this_data_cfg["bucket"],
                    key_base=this_data_cfg["key_base"],
                    suffix=this_data_cfg["suffix"]
                )

                # Could partition this data further (daily files)
                wr.s3.to_parquet(
                    df=df,
                    path=output_url,
                    s3_additional_kwargs=s3_additional_kwargs,
                    # compression=this_data_cfd["compression"],  # psycopg2 does not support copy redshift from
                                                                 # compressed parquet
                    index=False,  # Do not export empty index column
                )
