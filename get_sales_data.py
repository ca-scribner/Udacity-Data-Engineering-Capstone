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
        help='Last month to pull data (inclusive, in YYYY-MM format)'
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
    limit = 1000000
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
        with Timer(enter_message=f"Downloading data where {where}", exit_message="download complete"):
            results = source_client.get(source_key,
                                        select=select,
                                        order="date",
                                        where=where,
                                        limit=limit,
                                 )

        # Catch if we maxed our return limit
        if len(results) == limit:
            raise ValueError("Download limit reached - need to handle this better")

        df = pd.DataFrame(results).rename(columns=SALES_NAME_MAP_SOURCE_TO_APP)
        url_template = f's3://{{bucket}}/{{key_base}}/{start_date.year:02d}/{start_date.month:02d}/{start_date.year:04d}-{start_date.month:02d}{{suffix}}'

        # Save to csv
        if not args.no_csv:
            with Timer(enter_message=f"Uploading csv data", exit_message="upload csv complete"):
                output_url = url_template.format(
                    bucket=data_cfg["sales_raw"]["csv"]["bucket"],
                    key_base=data_cfg["sales_raw"]["csv"]["key_base"],
                    suffix=data_cfg["sales_raw"]["csv"]["suffix"]
                )

                # Could partition this data further (daily files)
                wr.s3.to_csv(
                    df=df,
                    path=output_url,
                    s3_additional_kwargs=s3_additional_kwargs,
                )

        # Save to parquet
        if not args.no_pq:
            with Timer(enter_message=f"Uploading parquet data", exit_message="upload csv complete"):
                output_url = url_template.format(
                    bucket=data_cfg["sales_raw"]["parquet"]["bucket"],
                    key_base=data_cfg["sales_raw"]["parquet"]["key_base"],
                    suffix=data_cfg["sales_raw"]["parquet"]["suffix"]
                )

                # Could partition this data further (daily files)
                wr.s3.to_parquet(
                    df=df,
                    path=output_url,
                    s3_additional_kwargs=s3_additional_kwargs,
                    compression=data_cfg["sales_raw"]["parquet"]["compression"],
                )
