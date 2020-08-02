staging_sales = "staging_sales"
staging_weather = "staging_weather"

invoices = "invoices"
items = "items"
product_categories = "product_categories"
stores = "stores"
weather = "weather"
weather_stations = "weather_stations"

count_rows = """
SELECT COUNT(*) from {table_name};
"""

drop = """
DROP TABLE IF EXISTS {table_name};
"""

drop_invoices = drop.format(table_name=invoices)
drop_items = drop.format(table_name=items)
drop_product_categories = drop.format(table_name=product_categories)
drop_stores = drop.format(table_name=stores)
drop_weather = drop.format(table_name=weather)
drop_weather_stations = drop.format(table_name=weather_stations)

drop_staging_sales = drop.format(table_name=staging_sales)
drop_staging_weather = drop.format(table_name=staging_weather)

create_staging_sales = f"""
CREATE TABLE {staging_sales} (
  invoice_id VARCHAR NOT NULL, 
  date DATE NOT NULL, 
  store_id DECIMAL NOT NULL, 
  store_name VARCHAR NOT NULL, 
  address VARCHAR NOT NULL, 
  city VARCHAR NOT NULL, 
  zip VARCHAR NOT NULL, 
  store_location VARCHAR, 
  county_number DECIMAL, 
  county VARCHAR, 
  category_id DECIMAL, 
  category_name VARCHAR, 
  vendor_id DECIMAL NOT NULL, 
  vendor_name VARCHAR NOT NULL, 
  item_id DECIMAL NOT NULL, 
  item_description VARCHAR NOT NULL, 
  pack DECIMAL NOT NULL, 
  bottle_volume_ml DECIMAL NOT NULL, 
  bottle_cost DECIMAL NOT NULL, 
  bottle_retail DECIMAL NOT NULL, 
  bottles_sold DECIMAL NOT NULL, 
  total_sale DECIMAL NOT NULL, 
  volume_sold_liters DECIMAL NOT NULL, 
  volume_sold_gallons DECIMAL NOT NULL
);
"""

create_staging_weather = f"""
CREATE TABLE {staging_weather} (
  "station_id" VARCHAR, 
  "name" VARCHAR, 
  "latitude" VARCHAR, 
  "longitude" VARCHAR, 
  "elevation" VARCHAR, 
  "date" DATE, 
  "precipitation" VARCHAR, 
  "snowfall" VARCHAR, 
  "temperature_max" VARCHAR, 
  "temperature_min" VARCHAR
);
"""

create_invoices = f"""
CREATE TABLE {invoices} (
  invoice_id VARCHAR(16) NOT NULL, 
  store_id VARCHAR(4) NOT NULL, 
  date DATE NOT NULL, 
  bottle_cost DECIMAL(7,3) NOT NULL, 
  bottle_retail DECIMAL(7,3) NOT NULL, 
  bottles_sold SMALLINT NOT NULL, 
  total_sale DECIMAL(8,3) NOT NULL,
  PRIMARY KEY (invoice_id),
  FOREIGN KEY (store_id) REFERENCES {stores}
);
"""

create_stores = f"""
CREATE TABLE {stores} (
  store_id VARCHAR(4) NOT NULL, 
  store_name VARCHAR(50) NOT NULL, 
  zip VARCHAR(5) NOT NULL, 
  store_location VARCHAR,
  PRIMARY KEY (store_id)
);
"""

create_items = f"""
CREATE TABLE {items} (
  item_id VARCHAR(6) NOT NULL, 
  item_description VARCHAR(70) NOT NULL, 
  category_id VARCHAR(7), 
  vendor_id VARCHAR(3) NOT NULL,
  PRIMARY KEY (item_id),
  FOREIGN KEY (category_id) REFERENCES {product_categories}
);
"""

create_product_categories = f"""
CREATE TABLE {product_categories} (
  category_id VARCHAR(7) NOT NULL, 
  category_name VARCHAR(50),
  PRIMARY KEY (category_id)
);
"""

create_weather = f"""
CREATE TABLE {weather} (
  station_id VARCHAR(11) NOT NULL, 
  date DATE NOT NULL, 
  precpitation DECIMAL(5, 3),
  snowfall DECIMAL(5, 3),
  temperature_max INTEGER, 
  temperature_min INTEGER,
  PRIMARY KEY (station_id, date),
  FOREIGN KEY (station_id) REFERENCES {weather_stations}
);
"""

create_weather_stations = f"""
CREATE TABLE {weather_stations} (
  station_id VARCHAR(11) NOT NULL, 
  name VARCHAR(50) NOT NULL, 
  latitude VARCHAR(17) NOT NULL, 
  longitude VARCHAR(17) NOT NULL, 
  zip VARCHAR(5),
  PRIMARY KEY (station_id)
);
"""

load_staging = """
SELECT aws_s3.table_import_from_s3(
    '{table_name}',
    '',
    '{{source_format}}',
    '{{bucket}}', '{{key}}', '{{region}}',
    '{{access_key}}', '{{secret_key}}', ''
);
"""

load_staging_sales = load_staging.format(table_name=staging_sales)
load_staging_weather = load_staging.format(table_name=staging_weather)

insert_invoices = f"""
INSERT INTO {invoices} (
    SELECT
        DISTINCT ON (invoice_id) invoice_id,
        store_id,
        date,
        bottle_cost,
        bottle_retail,
        bottles_sold,
        total_sale
    FROM {staging_sales}
    WHERE invoice_id IS NOT NULL
    ORDER BY invoice_id, date
);
"""

insert_stores = f"""
INSERT INTO {stores} (
    SELECT
        DISTINCT ON (store_id) store_id,
        store_name,
        zip
    FROM {staging_sales}
    WHERE store_id IS NOT NULL
    ORDER BY store_id, date
);
"""

insert_items = f"""
INSERT INTO {items} (
    SELECT
        DISTINCT ON (item_id) item_id, 
        item_description, 
        category_id, 
        vendor_id
    FROM {staging_sales}
    WHERE item_id IS NOT NULL
    ORDER BY item_id, date
);
"""

insert_product_categories = f"""
INSERT INTO {product_categories} (
    SELECT 
        DISTINCT ON (category_id) category_id, 
        category_name 
    FROM {staging_sales}
    WHERE category_id IS NOT NULL
    ORDER BY category_id, date    
);
"""

insert_weather = f"""
INSERT INTO {weather} (
    SELECT 
        CAST (station_id as VARCHAR),
        CAST (date as DATE),
        CAST (precipitation as DECIMAL),
        CAST (snowfall as DECIMAL),
        CAST (temperature_max as INTEGER),
        CAST (temperature_min as INTEGER)
    FROM {staging_weather}
);
"""

insert_weather_stations = f"""
INSERT INTO {weather_stations} (
    SELECT
        DISTINCT ON (station_id) station_id,
        name as name,
        latitude as latitude,
        longitude as longitude
    FROM {staging_weather}
    WHERE station_id IS NOT NULL
    ORDER BY station_id, date
);
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

load_staging_queries = {
    staging_sales: load_staging_sales,
    staging_weather: load_staging_weather,
}
insert_table_queries = {
    product_categories: insert_product_categories, 
    items: insert_items,
    stores: insert_stores,
    invoices: insert_invoices,
    weather_stations: insert_weather_stations,
    weather: insert_weather,
}


# Other queries
get_station_latitude_longitude = f"""
SELECT station_id, latitude, longitude FROM {weather_stations} 
WHERE 
    latitude IS NOT NULL 
    AND longitude IS NOT NULL 
    AND zip IS NULL 
;
"""

insert_station_zip = f"""
UPDATE {weather_stations} AS ws SET
  zip = new.zip
FROM (VALUES
  {{values}}
) as new(station_id, zip)
where ws.station_id = new.station_id
;
"""
