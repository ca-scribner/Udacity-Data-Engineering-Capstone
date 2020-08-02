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

drop_olap_sales_weather = drop.format(table_name=olap_sales_weather)

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
  item_id VARCHAR(6) NOT NULL, 
  date DATE NOT NULL, 
  bottle_cost DECIMAL(7,3) NOT NULL, 
  bottle_retail DECIMAL(7,3) NOT NULL, 
  bottles_sold SMALLINT NOT NULL, 
  total_sale DECIMAL(8,3) NOT NULL,
  PRIMARY KEY (invoice_id),
  FOREIGN KEY (store_id) REFERENCES {stores},
  FOREIGN KEY (item_id) REFERENCES {items}
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
  precipitation DECIMAL(5, 3),
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
        item_id,
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
