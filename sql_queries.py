invoices = "invoices"
items = "items"
population = "population"
product_categories = "product_categories"
stores = "stores"
weather = "weather"
weather_stations = "weather_stations"

staging_sales = "staging_sales"
staging_weather = f"staging_{weather}"
staging_population = f"staging_{population}"

olap_sales_weather_population = f"fact_sales_{weather}_{population}"
olap_monthly_sales_store = f"fact_monthly_sales_store"
olap_daily_sales_by_category = f"fact_daily_sales_by_category"

count_rows = """
SELECT COUNT(*) from {table_name}
"""

drop = """
DROP TABLE IF EXISTS {table_name} CASCADE
"""

# Should make drop's auto generate from the list of create queries
drop_invoices = drop.format(table_name=invoices)
drop_items = drop.format(table_name=items)
drop_population = drop.format(table_name=population)
drop_product_categories = drop.format(table_name=product_categories)
drop_stores = drop.format(table_name=stores)
drop_weather = drop.format(table_name=weather)
drop_weather_stations = drop.format(table_name=weather_stations)

drop_olap_sales_weather_population = drop.format(table_name=olap_sales_weather_population)
drop_olap_monthly_sales_store = drop.format(table_name=olap_monthly_sales_store)
drop_olap_daily_sales_by_category = drop.format(table_name=olap_daily_sales_by_category)

drop_staging_sales = drop.format(table_name=staging_sales)
drop_staging_weather = drop.format(table_name=staging_weather)
drop_staging_population = drop.format(table_name=staging_population)
staging_sales_columns = {
    "invoice_id": "VARCHAR NOT NULL",
    "date": "DATE NOT NULL",
    "store_id": "DECIMAL NOT NULL",
    "store_name": "VARCHAR NOT NULL",
    # "address": "VARCHAR NOT NULL",
    # "city": "VARCHAR NOT NULL",
    "zipcode": "VARCHAR",  # Some values are malformed/missing
    # "store_location": "VARCHAR",
    # "county_number": "DECIMAL",
    # "county": "VARCHAR",
    "category_id": "DECIMAL",
    "category_name": "VARCHAR",
    "vendor_id": "DECIMAL",
    "vendor_name": "VARCHAR",
    "item_id": "VARCHAR NOT NULL",
    "item_description": "VARCHAR NOT NULL",
    # "pack": "DECIMAL NOT NULL",
    # "bottle_volume_ml": "DECIMAL NOT NULL",
    "bottle_cost": "DECIMAL",
    "bottle_retail": "DECIMAL",
    "bottles_sold": "DECIMAL",
    "total_sale": "DECIMAL",
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

staging_population_columns = {
    "year": "INTEGER",
    "population": "INTEGER",
    "minimum_age": "INTEGER",
    "maximum_age": "INTEGER",
    "gender": "VARCHAR(6)",
    "zipcode": "VARCHAR(5)",
    "geo_id": "VARCHAR(14)",
}

create_staging_population = f"""
CREATE TABLE {staging_population} (
  {", ".join(f"{name} {spec}" for name, spec in staging_population_columns.items())}
)
"""

invoices_columns = {
    "invoice_id": "VARCHAR(16) NOT NULL",
    "store_id": "VARCHAR(4) NOT NULL",
    "item_id": "VARCHAR(8) NOT NULL",
    "date": "DATE NOT NULL",
    "bottle_cost": "DECIMAL(8,3) NOT NULL",
    "bottle_retail": "DECIMAL(8,3) NOT NULL",
    "bottles_sold": "SMALLINT NOT NULL",
    "total_sale": "DECIMAL(9,3) NOT NULL",
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
    "zipcode": "VARCHAR(5)",
}

create_stores = f"""
CREATE TABLE {stores} (
  {", ".join(f"{name} {spec}" for name, spec in stores_columns.items())},
  PRIMARY KEY (store_id)
)
"""

items_columns = {
    "item_id": "VARCHAR(8) NOT NULL",
    "item_description": "VARCHAR(70) NOT NULL",
    "category_id": "VARCHAR(7)",
    "vendor_id": "VARCHAR(3)",
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
    "zipcode": "VARCHAR(5)",
}

create_weather_stations = f"""
CREATE TABLE {weather_stations} (
  {", ".join(f"{name} {spec}" for name, spec in weather_stations_columns.items())},
  PRIMARY KEY (station_id)
)
"""

population_columns = {
    "year": "INTEGER",
    "zipcode": "VARCHAR(5)",
    "population": "INTEGER",
}

create_population = f"""
CREATE TABLE {population} (
  {", ".join(f"{name} {spec}" for name, spec in population_columns.items())},
  PRIMARY KEY (year, zipcode)
)
"""

olap_sales_weather_population_columns = {
    "invoice_id": "VARCHAR(16) NOT NULL",
    "date": "DATE NOT NULL",
    "category_id": "VARCHAR(7) NOT NULL",
    "store_id": "VARCHAR(4) NOT NULL",
    "total_sale": "DECIMAL(9,3) NOT NULL",
    "precipitation": "DECIMAL(5, 3)",
    "snowfall": "DECIMAL(5, 3)",
    "population": "INTEGER",
}

create_olap_sales_weather_population = f"""
CREATE TABLE {olap_sales_weather_population} (
      {", ".join(f"{name} {spec}" for name, spec in olap_sales_weather_population_columns.items())},
  PRIMARY KEY (invoice_id),
  FOREIGN KEY (category_id) REFERENCES {product_categories},
  FOREIGN KEY (store_id) REFERENCES {stores}
)
"""

olap_monthly_sales_store_columns = {
    "year": "INTEGER",
    "month": "INTEGER",
    "store_id": "VARCHAR(4) NOT NULL",
    "total_sale": "DECIMAL(11,3) NOT NULL",
}

create_olap_monthly_sales_store = f"""
CREATE TABLE {olap_monthly_sales_store} (
      {", ".join(f"{name} {spec}" for name, spec in olap_monthly_sales_store_columns.items())},
  PRIMARY KEY (year, month, store_id),
  FOREIGN KEY (store_id) REFERENCES {stores}
)
"""

olap_daily_sales_by_category_columns = {
    "date": "DATE NOT NULL",
    "category_id": "VARCHAR(7) NOT NULL",
    "total_sale": "DECIMAL(11,3) NOT NULL",
}

create_olap_daily_sales_by_category = f"""
CREATE TABLE {olap_daily_sales_by_category} (
      {", ".join(f"{name} {spec}" for name, spec in olap_daily_sales_by_category_columns.items())},
  PRIMARY KEY (date, category_id),
  FOREIGN KEY (category_id) REFERENCES {product_categories}
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
load_staging_population_postgres = load_staging_postgres.format(table_name=staging_population)

load_staging_redshift = """
COPY {table_name} 
FROM 's3://{{bucket}}/{{key}}' 
IAM_ROLE '{{iam}}' 
{{source_format}} 
COMPUPDATE OFF STATUPDATE OFF
"""

load_staging_sales_redshift = load_staging_redshift.format(table_name=staging_sales)
load_staging_weather_redshift = load_staging_redshift.format(table_name=staging_weather)
load_staging_population_redshift = load_staging_redshift.format(table_name=staging_population)

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

# Handle invoices differently so we ensure we get no null price/bottle
insert_invoices = f"""
INSERT INTO {{table_name}} (
{select_distinct} 
AND bottle_cost IS NOT NULL
AND bottle_retail IS NOT NULL
AND bottles_sold IS NOT NULL
AND total_sale IS NOT NULL
)
""".format(
    table_name=invoices,
    columns=", ".join(invoices_columns),
    partition_by="invoice_id",
    order_by="date DESC",
    source_table=staging_sales
)

this_weather_stations_columns = [x for x in weather_stations_columns.keys() if x != "zipcode"]

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

# Use NULLIF to catch blank strings as null.  Postgres does not need this, but without it redshift will not see any
# null gender fields
insert_population = f"""
INSERT INTO {population} (
    SELECT 
        {", ".join(population_columns)}
    FROM {staging_population}
    WHERE minimum_age IS NULL
      AND maximum_age IS NULL 
      AND NULLIF(gender, '') IS NULL
)
"""

select_popoulation_by_year = f"""
SELECT * FROM {population} WHERE year = {{year}}
"""

select_oltp_sales_weather_population = f"""
SELECT 
    t.invoice_id as invoice_id,
    t.date as date,
    t.category_id as category_id,
    t.store_id as store_id,
    t.total_sale as total_sale,
    t.precipitation as precipitation,
    t.snowfall as snowfall,
    t.population as population
FROM (
    SELECT 
        inv.invoice_id,
        inv.date,
        it.category_id,
        s.store_id,
        inv.total_sale,
        w.precipitation,
        w.snowfall,
        p.population,
        row_number() over (partition by ws.zipcode, inv.invoice_id
                           order by ws.station_id) as rn
    FROM {invoices} inv
    JOIN {stores} s ON (s.store_id = inv.store_id)
    JOIN {items} it ON (it.item_id = inv.item_id)
    JOIN {weather_stations} ws ON (ws.zipcode = s.zipcode)
    JOIN {weather} w ON w.station_id = ws.station_id AND w.date = inv.date
    JOIN ({select_popoulation_by_year.format(year=2010)}) p ON p.zipcode = s.zipcode
    WHERE it.category_id IS NOT NULL
    ORDER BY ws.zipcode, ws.station_id, inv.invoice_id
) t
WHERE rn = 1
"""

select_oltp_sales_weather_population_for_year = select_oltp_sales_weather_population + " and EXTRACT (YEAR FROM t.date) = {year}"

select_oltp_monthly_sales_store = f"""
SELECT
    EXTRACT (YEAR FROM inv.date) as year,
    EXTRACT (MONTH FROM inv.date) as month, 
    inv.store_id,
    SUM(inv.total_sale)
FROM invoices inv
GROUP BY year, month, inv.store_id
"""

select_oltp_daily_sales_by_category = f"""
SELECT
    inv.date as date,
    it.category_id as category_id,
    SUM(inv.total_sale)
FROM invoices inv
JOIN items it ON (it.item_id = inv.item_id)
WHERE category_id IS NOT NULL
GROUP BY date, category_id
"""

insert_olap_sales_weather_population = f"""
INSERT INTO {olap_sales_weather_population} (
    {select_oltp_sales_weather_population}
)
"""

insert_olap_monthly_sales_store = f"""
INSERT INTO {olap_monthly_sales_store} (
    {select_oltp_monthly_sales_store}
)
"""

insert_olap_daily_sales_by_category = f"""
INSERT INTO {olap_daily_sales_by_category} (
    {select_oltp_daily_sales_by_category}
)
"""

create_staging_table_queries = {
    staging_sales: create_staging_sales,
    staging_weather: create_staging_weather,
    staging_population: create_staging_population,
}

create_table_queries = {
    product_categories: create_product_categories,
    items: create_items,
    stores: create_stores,
    invoices: create_invoices,
    weather_stations: create_weather_stations,
    weather: create_weather,
    population: create_population,
}

create_olap_table_queries = {
    olap_sales_weather_population: create_olap_sales_weather_population,
    olap_monthly_sales_store: create_olap_monthly_sales_store,
    olap_daily_sales_by_category: create_olap_daily_sales_by_category
}

drop_staging_table_queries = {
    staging_sales: drop_staging_sales,
    staging_weather: drop_staging_weather,
    staging_population: drop_staging_population,
}

drop_table_queries = {
    population: drop_population,
    weather: drop_weather,
    weather_stations: drop_weather_stations,
    invoices: drop_invoices,
    stores: drop_stores,
    items: drop_items,
    product_categories: drop_product_categories,
}

drop_olap_table_queries = {
    olap_sales_weather_population: drop_olap_sales_weather_population,
    olap_monthly_sales_store: drop_olap_monthly_sales_store,
    olap_daily_sales_by_category: drop_olap_daily_sales_by_category,
}

load_staging_queries_postgres = {
    staging_sales: load_staging_sales_postgres,
    staging_weather: load_staging_weather_postgres,
    staging_population: load_staging_population_postgres,
}

load_staging_queries_redshift = {
    staging_sales: load_staging_sales_redshift,
    staging_weather: load_staging_weather_redshift,
    staging_population: load_staging_population_redshift,
}

insert_table_queries_postgres = {
    product_categories: insert_product_categories,
    items: insert_items,
    stores: insert_stores,
    invoices: insert_invoices,
    weather_stations: insert_weather_stations,
    weather: insert_weather,
    population: insert_population,
}

insert_olap_table_queries = {
    olap_monthly_sales_store: insert_olap_monthly_sales_store,
    olap_daily_sales_by_category: insert_olap_daily_sales_by_category,
    olap_sales_weather_population: insert_olap_sales_weather_population,
}

# Other queries
get_station_latitude_longitude = f"""
SELECT station_id, latitude, longitude FROM {weather_stations} 
WHERE 
    latitude IS NOT NULL 
    AND longitude IS NOT NULL 
    AND zipcode IS NULL 
"""

# # Could use this for postgres, but redshift does not support update from values.  Use common syntax below
# insert_station_zipcode_POSTGRES = f"""
# UPDATE {weather_stations} AS ws SET
#   zipcode = new.zipcode
# FROM (VALUES
#   {{values}}
# ) as new(station_id, zipcode)
# where ws.station_id = new.station_id
# """

insert_station_zipcode = f"""
CREATE TEMPORARY TABLE new_zipcodes (
  station_id VARCHAR(11) NOT NULL, 
  zipcode VARCHAR(5)
);
INSERT INTO new_zipcodes
VALUES
    {{values}}
;
UPDATE {weather_stations} 
SET zipcode=selected.zipcode
FROM (
	SELECT station_id
  		 , zipcode
 	FROM new_zipcodes
) selected
WHERE {weather_stations}.station_id=selected.station_id
"""

# Analytical queries
select_olap_sales_weather_population = f"""
SELECT
    invoice_id,
    date,
    category_id,
    store_id,
    total_sale,
    precipitation,
    snowfall,
    population
FROM {olap_sales_weather_population}
"""

select_olap_sales_weather_population_for_year = select_olap_sales_weather_population + \
                                                " WHERE EXTRACT(YEAR from date) = {year}"


select_olap_monthly_sales_store = f"""
SELECT
    *
FROM {olap_monthly_sales_store}
"""

select_olap_daily_sales_by_category = f"""
SELECT
    *
FROM {olap_daily_sales_by_category}
"""


analytical_queries = {
    "OLTP: sales vs weather and population": select_oltp_sales_weather_population,
    "OLAP: sales vs weather and population": select_olap_sales_weather_population,
    "OLTP: sales vs weather and population (2015)": select_oltp_sales_weather_population_for_year.format(year=2015),
    "OLAP: sales vs weather and population (2015)": select_olap_sales_weather_population_for_year.format(year=2015),
    "OLTP: monthly sales by store": select_oltp_monthly_sales_store,
    "OLAP: monthly sales by store": select_olap_monthly_sales_store,
    "OLTP: daily sales by category": select_oltp_daily_sales_by_category,
    "OLAP: daily sales by category": select_olap_daily_sales_by_category,
}

redshift_diststyle = {
    product_categories: " DISTSTYLE ALL",
    items: " DISTSTYLE ALL",
    stores: " DISTSTYLE ALL",
    invoices: " distkey(store_id)",
    weather_stations: " DISTSTYLE ALL",
    weather: " DISTSTYLE ALL",
    population: " DISTSTYLE ALL",
    staging_sales: " DISTSTYLE EVEN",
    staging_weather: " DISTSTYLE EVEN",
    staging_population: " DISTSTYLE EVEN",
    olap_sales_weather_population: "",
    olap_monthly_sales_store: "",
    olap_daily_sales_by_category: ""
}

# Misc helper queries

# very simple query used during performance testing to make sure there's no first-query-lag in timing
discardable_query = f"""SELECT * FROM {staging_sales} LIMIT 1"""
