staging_sales = "staging_sales"
staging_weather = "staging_weather"

invoices = "invoices"
items = "items"
product_categories = "product_categories"
stores = "stores"
weather = "weather"
weather_stations = "weather_stations"
olap_sales_weather = "fact_weather_sales"

count_rows = """
SELECT COUNT(*) from {table_name}
"""

drop = """
DROP TABLE IF EXISTS {table_name}
"""

drop_invoices = drop.format(table_name=invoices)
drop_items = drop.format(table_name=items)
drop_product_categories = drop.format(table_name=product_categories)
drop_stores = drop.format(table_name=stores)
drop_weather = drop.format(table_name=weather)
drop_weather_stations = drop.format(table_name=weather_stations)

drop_olap_sales_weather = drop.format(table_name=olap_sales_weather)

drop_staging_sales = drop.format(table_name=staging_sales)
drop_staging_weather = drop.format(table_name=staging_weather)

staging_sales_columns = {
    "invoice_id": "VARCHAR NOT NULL",
    "date": "DATE NOT NULL",
    "store_id": "DECIMAL NOT NULL",
    "store_name": "VARCHAR NOT NULL",
    # "address": "VARCHAR NOT NULL",
    # "city": "VARCHAR NOT NULL",
    "zip": "VARCHAR NOT NULL",
    "store_location": "VARCHAR",
    # "county_number": "DECIMAL",
    # "county": "VARCHAR",
    "category_id": "DECIMAL",
    "category_name": "VARCHAR",
    "vendor_id": "DECIMAL NOT NULL",
    "vendor_name": "VARCHAR NOT NULL",
    "item_id": "DECIMAL NOT NULL",
    "item_description": "VARCHAR NOT NULL",
    # "pack": "DECIMAL NOT NULL",
    # "bottle_volume_ml": "DECIMAL NOT NULL",
    "bottle_cost": "DECIMAL NOT NULL",
    "bottle_retail": "DECIMAL NOT NULL",
    "bottles_sold": "DECIMAL NOT NULL",
    "total_sale": "DECIMAL NOT NULL",
    # "volume_sold_liters": "DECIMAL NOT NULL",
    # "volume_sold_gallons": "DECIMAL NOT NULL",
}
create_staging_sales = f"""
CREATE TABLE {staging_sales} (
  {", ".join(f"{name} {spec}" for name, spec in staging_sales_columns.items())}
)
"""

staging_weather_columns = {
    "station_id": "VARCHAR",
    "name": "VARCHAR",
    "latitude": "VARCHAR",
    "longitude": "VARCHAR",
    "elevation": "VARCHAR",
    "date": "DATE",
    "precipitation": "VARCHAR",
    "snowfall": "VARCHAR",
    "temperature_max": "VARCHAR",
    "temperature_min": "VARCHAR",
}

create_staging_weather = f"""
CREATE TABLE {staging_weather} (
  {", ".join(f"{name} {spec}" for name, spec in staging_weather_columns.items())}
)
"""

invoices_columns = {
    "invoice_id": "VARCHAR(16) NOT NULL",
    "store_id": "VARCHAR(4) NOT NULL",
    "item_id": "VARCHAR(6) NOT NULL",
    "date": "DATE NOT NULL",
    "bottle_cost": "DECIMAL(7,3) NOT NULL",
    "bottle_retail": "DECIMAL(7,3) NOT NULL",
    "bottles_sold": "SMALLINT NOT NULL",
    "total_sale": "DECIMAL(8,3) NOT NULL",
}

create_invoices = f"""
CREATE TABLE {invoices} (
  {", ".join(f"{name} {spec}" for name, spec in invoices_columns.items())},
  PRIMARY KEY (invoice_id),
  FOREIGN KEY (store_id) REFERENCES {stores},
  FOREIGN KEY (item_id) REFERENCES {items}
)
"""

stores_columns = {
    "store_id": "VARCHAR(4) NOT NULL",
    "store_name": "VARCHAR(50) NOT NULL",
    "zip": "VARCHAR(5) NOT NULL",
    "store_location": "VARCHAR",
}

create_stores = f"""
CREATE TABLE {stores} (
  {", ".join(f"{name} {spec}" for name, spec in stores_columns.items())},
  PRIMARY KEY (store_id)
)
"""

items_columns = {
    "item_id": "VARCHAR(6) NOT NULL",
    "item_description": "VARCHAR(70) NOT NULL",
    "category_id": "VARCHAR(7)",
    "vendor_id": "VARCHAR(3) NOT NULL",
}

create_items = f"""
CREATE TABLE {items} (
  {", ".join(f"{name} {spec}" for name, spec in items_columns.items())},
  PRIMARY KEY (item_id),
  FOREIGN KEY (category_id) REFERENCES {product_categories}
)
"""

product_categories_columns = {
    "category_id": "VARCHAR(7) NOT NULL",
    "category_name": "VARCHAR(50)",
}

create_product_categories = f"""
CREATE TABLE {product_categories} (
  {", ".join(f"{name} {spec}" for name, spec in product_categories_columns.items())},
  PRIMARY KEY (category_id)
)
"""

weather_columns = {
    "station_id": "VARCHAR(11) NOT NULL",
    "date": "DATE NOT NULL",
    "precipitation": "DECIMAL(5, 3)",
    "snowfall": "DECIMAL(5, 3)",
    "temperature_max": "INTEGER",
    "temperature_min": "INTEGER",
}

create_weather = f"""
CREATE TABLE {weather} (
  {", ".join(f"{name} {spec}" for name, spec in weather_columns.items())},
  PRIMARY KEY (station_id, date),
  FOREIGN KEY (station_id) REFERENCES {weather_stations}
)
"""

weather_stations_columns = {
    "station_id": "VARCHAR(11) NOT NULL",
    "name": "VARCHAR(50) NOT NULL",
    "latitude": "VARCHAR(17) NOT NULL",
    "longitude": "VARCHAR(17) NOT NULL",
    "zip": "VARCHAR(5)",
}

create_weather_stations = f"""
CREATE TABLE {weather_stations} (
  {", ".join(f"{name} {spec}" for name, spec in weather_stations_columns.items())},
  PRIMARY KEY (station_id)
)
"""

create_olap_sales_weather = f"""
CREATE TABLE {olap_sales_weather} (
  invoice_id VARCHAR(16) NOT NULL, 
  date DATE NOT NULL, 
  category_id VARCHAR(7) NOT NULL, 
  store_id VARCHAR(4) NOT NULL, 
  total_sale DECIMAL(8,3) NOT NULL,
  precipitation DECIMAL(5, 3),
  snowfall DECIMAL(5, 3),
  PRIMARY KEY (invoice_id),
  FOREIGN KEY (category_id) REFERENCES {product_categories},
  FOREIGN KEY (store_id) REFERENCES {stores}
)
"""

load_staging_postgres = """
SELECT aws_s3.table_import_from_s3(
    '{table_name}',
    '',
    '{{source_format}}',
    '{{bucket}}', '{{key}}', '{{region}}',
    '{{access_key}}', '{{secret_key}}', ''
)
"""

load_staging_sales_postgres = load_staging_postgres.format(table_name=staging_sales)
load_staging_weather_postgres = load_staging_postgres.format(table_name=staging_weather)

load_staging_redshift = """
COPY {table_name} 
FROM 's3://{{bucket}}/{{key}}' 
IAM_ROLE '{{iam}}' 
{{source_format}} 
REGION '{{region}}' 
COMPUPDATE OFF STATUPDATE OFF
"""

load_staging_sales_redshift = load_staging_redshift.format(table_name=staging_sales)
load_staging_weather_redshift = load_staging_redshift.format(table_name=staging_weather)

# Query template to get a distinct row for each group that is generic across postgres and redshift
select_distinct = """
WITH cte AS
(
    SELECT
        {columns},
        ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_by}) as row_number
        FROM {source_table}
)
SELECT
    {columns}
FROM cte
WHERE row_number = 1
"""

# Query template for inserting distinct rows for each group that is generic across postgres and redshift
insert_distinct = f"""
INSERT INTO {{table_name}} (
{select_distinct}
)
"""


insert_invoices = insert_distinct.format(
    table_name=invoices,
    columns=", ".join(invoices_columns),
    partition_by="invoice_id",
    order_by="date DESC",
    source_table=staging_sales
)

insert_stores = insert_distinct.format(
    table_name=stores,
    columns=", ".join(stores_columns),
    partition_by="store_id",
    order_by="date DESC",
    source_table=staging_sales
)

insert_items = insert_distinct.format(
    table_name=items,
    columns=", ".join(items_columns),
    partition_by="item_id",
    order_by="date DESC",
    source_table=staging_sales
)

# Handle product_categories differently so we ensure we get no null category id's
insert_product_categories = f"""
INSERT INTO {{table_name}} (
{select_distinct} AND category_id IS NOT NULL
)
""".format(
    table_name=product_categories,
    columns=", ".join(product_categories_columns),
    partition_by="category_id",
    order_by="date DESC",
    source_table=staging_sales
)

this_weather_stations_columns = [x for x in weather_stations_columns.keys() if x != "zip"]

insert_weather_stations = insert_distinct.format(
    table_name=weather_stations,
    columns=", ".join(this_weather_stations_columns),
    partition_by="station_id",
    order_by="date DESC",
    source_table=staging_weather
)

# Use NULLIF to catch blank strings as null.  Postgres does not need this, but without it redshift will raise type error
# on cast
insert_weather = f"""
INSERT INTO {weather} (
    SELECT 
        CAST (station_id as VARCHAR),
        CAST (date as DATE),
        CAST (NULLIF(precipitation, '') as DECIMAL),
        CAST (NULLIF(snowfall, '') as DECIMAL),
        CAST (NULLIF(temperature_max, '') as INTEGER),
        CAST (NULLIF(temperature_min, '') as INTEGER)
    FROM {staging_weather}
)
"""


insert_olap_sales_weather = f"""
INSERT INTO {olap_sales_weather} (
    SELECT 
        t.invoice_id as invoice_id,
        t.date as date,
        t.category_id as category_id,
        t.store_id as store_id,
        t.total_sale as total_sale,
        t.precipitation as precipitation,
        t.snowfall as snowfall
    FROM (
        SELECT 
            inv.invoice_id,
            inv.date,
            it.category_id,
            s.store_id,
            inv.total_sale,
            w.precipitation,
            w.snowfall,
            row_number() over (partition by ws.zip, inv.invoice_id
                               order by ws.station_id) as rn
        FROM invoices inv
        JOIN stores s ON (s.store_id = inv.store_id)
        JOIN items it ON (it.item_id = inv.item_id)
        JOIN product_categories pc ON (pc.category_id = it.category_id)
        JOIN weather_stations ws ON (ws.zip = s.zip)
        JOIN weather w ON w.station_id = ws.station_id AND w.date = inv.date
        ORDER BY ws.zip, ws.station_id, inv.invoice_id
    ) t
    WHERE rn = 1
    ORDER BY t.invoice_id
)
"""

create_staging_table_queries = {
    staging_sales: create_staging_sales,
    staging_weather: create_staging_weather,
}

create_table_queries = {
    product_categories: create_product_categories, 
    items: create_items,
    stores: create_stores,
    invoices: create_invoices,
    weather_stations: create_weather_stations,
    weather: create_weather,
}

create_olap_table_queries = {
    olap_sales_weather: create_olap_sales_weather,
}

drop_staging_table_queries = {
    staging_sales: drop_staging_sales,
    staging_weather: drop_staging_weather,
}

drop_table_queries = {
    invoices: drop_invoices,
    stores: drop_stores,
    items: drop_items,
    product_categories: drop_product_categories, 
    weather: drop_weather,
    weather_stations: drop_weather_stations,
}

drop_olap_table_queries = {
    olap_sales_weather: drop_olap_sales_weather,
}

load_staging_queries_postgres = {
    staging_sales: load_staging_sales_postgres,
    staging_weather: load_staging_weather_postgres,
}

load_staging_queries_redshift = {
    staging_sales: load_staging_sales_redshift,
    staging_weather: load_staging_weather_redshift,
}


insert_table_queries_postgres = {
    product_categories: insert_product_categories, 
    items: insert_items,
    stores: insert_stores,
    invoices: insert_invoices,
    weather_stations: insert_weather_stations,
    weather: insert_weather,
}

insert_olap_table_queries = {
    olap_sales_weather: insert_olap_sales_weather,
}

# Other queries
get_station_latitude_longitude = f"""
SELECT station_id, latitude, longitude FROM {weather_stations} 
WHERE 
    latitude IS NOT NULL 
    AND longitude IS NOT NULL 
    AND zip IS NULL 
"""

# # Could use this for postgres, but redshift does not support update from values.  Use common syntax below
# insert_station_zip_POSTGRES = f"""
# UPDATE {weather_stations} AS ws SET
#   zip = new.zip
# FROM (VALUES
#   {{values}}
# ) as new(station_id, zip)
# where ws.station_id = new.station_id
# """

insert_station_zip = f"""
CREATE TEMPORARY TABLE new_zips (
  station_id VARCHAR(11) NOT NULL, 
  zip VARCHAR(5)
);
INSERT INTO new_zips
VALUES
    {{values}}
;
UPDATE {weather_stations} 
SET zip=selected.zip
FROM (
	SELECT station_id
  		 , zip
 	FROM new_zips
) selected
WHERE {weather_stations}.station_id=selected.station_id
"""

