# Udacity Data Engineering Nanodegree

# Project: Data Warehouse

This project explores the use of AWS' managed Postgres and Redshift database services using [Iowa liquor sales data](https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Sales/m3tr-qhgy) joined with [NOAA weather data](https://www.ncdc.noaa.gov/cdo-web/).  The objectives are to better understand how these two offerings behave for both OLTP (regularly loading new data into a source of truth database) and OLAP (extracting insights that may span multiple tables) workflows.  In general, the objectives with this data are to derive insights regarding the liquor sales data, such as investigating monthly liquor sales by store or the relation between liquor sales and weather patterns (such as snowfall and rainfall).  In particular, this will be explored from the perspective of a user who needs to support both a source of truth (must have access to in-sync data, although this access need not be fully performant and could be infrequent) and analytics workflows (such as for a dashboard or machine learning use cases).

TO ADD: 
* explore and assess data, before schema?

# Source Data

The source data used here are:

* Iowa Liquor Sales:
* National Oceanic and Atmospheric Administration weather data:

The exploration of these datasets is described below

## Iowa Liquor Sales:

The [Iowa Liquor sales data](https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Sales/m3tr-qhgy) includes per-sale invoice records for liquor sales across Iowa, including the item sold, location sold from, and value of sale.  The data spans sales from 2012 to 2018 and is accessible through CSV download or interaction with the Socrata API.  The API has been used here, automated through scripts that request monthly data.  Initial data exploration was done using a subset of the data to estimate types/sizes, then these estimates were used to ingest the entire dataset.  

Key data fields include:

* invoice_id: unique, constant 16 character length, and does not contain nulls
* date: ranges from 2012-01 to 2018-04 and does not contain nulls
* store id: 4 digit ID and does not contain nulls 
* store zip code: 5 digit ID and does not contain nulls
* item category id: 9 digit numeric ID and and contains nulls
* category name: <50 character text field that contains nulls
* bottle cost, bottle retail, and total sale value: numeric and may contain nulls.  Staged as text to avoid rounding and to allow their final destination to choose precision

The staged raw CSV and parquet data are hosted publicly in `udacity-de-capstone-182/raw-data/sales/`.

Note also that some fields which have ID and name components (for example, category id and category name) do not necessarily have the same unique counts.  Their definitions have changed over time (for example, Category ABC might be renamed to Category XYZ, but still use the same ID=1).  Because of this the strategy used here is to resolve any conflicts between ID and name by using the most recent naming convention from the raw data (for example, if in 2012 Category 1 had name ABC but in 2018 Category 1 had name XYZ, we use XYZ).

## National Oceanic and Atmospheric Administration weather data:

The [National Oceanic and Atmospheric Administration weather data](https://www.ncdc.noaa.gov/cdo-web/) includes daily weather summaries from all weather stations in the state of Iowa, including latitude/longitude of the station and the daily precipitation and snowfall totals for that location.  This data was obtained by manual request from the source as yearly CSV files and loaded into S3 manually. 

Key data fields include:
* weather station id: 11 character unique station ID that does not contain nulls
* date: collected for 2012-2018, does not contain nulls
* weather station latitude and longitude: described as a point
* daily precipitation: decimal value, may contain nulls
* daily snowfall: decimal value, may contain nulls

The staged raw CSV data is hosted publicly in `udacity-de-capstone-182/raw-data/weather/`.

# ETL Strategy

The same ETL process was applied for both Postgres and Redshift implementations.  This process took the following steps:

* Store raw data:
    * (by human interaction): Transfer NOAA Iowa weather data for 2012-2018 to AWS S3, stored as yearly CSV files
    * (`get_sales_data.py`): Collect Iowa Liquor Sales data from Socrata API and transfer to AWS S3, grabbed and stored as monthly CSV and parquet files
* Stage raw data in staging tables:
    * (`create_tables.py`): (Drop and) Create all staging, OLTP, and OLAP tables
    * (`etl.py`): Load staging data from S3 CSV to database
* Copy staged data to OLTP tables:
    * (`etl.py`): Select/insert data from staging tables into OLTP tables in the schema described below
    * (`etl.py`): Enrich weather station data by adding zip code, computed using the `uszipcode` zip code search engine
* (`etl.py`): Copy OLTP data to OLAP schemas:
 
# Analytics objectives

In general, the objectives here are to flexibly derive insights from the sales, weather, and population data provided.  As an example of these goals, three primary use cases were examined.  These were chosen to investigate different workloads (some spanning the entire database, others aggregating small portions of the data).  The use cases are: 

1. Aggregating sales per category and relating it to weather and population data (for example, how do the per-capita sales of liquor category X compare to category Y on days with high snowfall).
1. Aggregating monthly sales per store
1. Aggregating daily sales per category across all stores

# Database Schema

## OLTP Schema

The OLTP database had the following schema:

![OLTP Database Schema](./images/oltp.png)

The database centers on a fact table which includes transaction data (`bottle_cost`, `total_sale`) and references to other tables for store and item details.  The goal here is a Third Normal Form schema with no redundancy, but this causes complications when trying to relate properties like an item category with the store that sells it as they require multiple joins.  In the case of aggregating liquor sales by category and comparing to weather data, five joins are required.

## OLAP Schemas

To optimize performance for the target workloads, the following OLAP schemas were also investigated. 

### Per Category Sales vs Weather and Population

This use case spans the entire OLTP, relating product category to weather and population, and is thus expected to benefit significantly from a denormalized OLAP table.  For example, in the OLTP schema relating invoices to weather requires four joins and relating invoices to product categories requires two joins in the opposite direction.  To aggregate per-category sales and compare them to weather and population in the OLTP schema requires five joins.  

An OLAP schema around a single denormalized fact table was investigated as shown below.  This schema would let a user view per-category_id sales vs weather and population without a single join.

![OLAP Sales vs Weather and Population](./images/olap_sales_weather_population.png)

### Daily Sales per Category

This use case requires a single `join` and `group by` to be performed when using the OLTP database directly.  It is expected that an OLAP table could improve on the responsiveness of this query by storing these results.  An example of the schema is shown below. 

![OLAP Daily Sales per Category](./images/olap_daily_sales_by_category.png)

### Monthly Aggregate Sales per Store

This use case is requires no `join`s but does require a `group by` over a large table.  It is expected that the use case could benefit from an aggregation OLAP table that caches the work of the `group by` operation, although perhaps less so than use cases that require many joins.  An example of the schema is shown below. 

![OLAP Monthly Sales per Store](./images/olap_monthly_sales_store.png)

# Table Optimization

TODO?
**NOTES: make suer you check these for full db**

* add indices for key table/query?  For example:
    * `explain analyze select * from population order by zipcode;`: 
        * No index: 260ms
        * `create index population_ind_zipcode ON population(zipcode);`: 94ms
        * `create index population_ind_zipcode ON population(zipcode, population);`: 94ms  <-this way we don't need to scan to get population
    * `olap_fact_sales_weather_population_by_year`:
        * No index: 17ms
        * `CREATE INDEX olap_swp_year ON fact_sales_weather_population(EXTRACT (YEAR FROM date));`: 0.06ms

                         
    ``

This implementation does not use any distribution or sorting within the tables because:

the desired analytical workload was not specified. Depending on the desired workload, setting a distribution strategy for the songplay table that is focused on the primary actions might be adventageous.
as we are examining a subset of the data, the distribution of the real data is unclear. The balance between number of records of users vs artists appears to be different from what we'd expect in practice.
Without foreknowledge about the workflow and data, Redshift's default optimization is a good option.

If more detail on the potential data/workflow was known, a distribution strategy might be available that co-locates commonly joined information. For example, assuming the songplay and user tables are both expected to be large and often joined, while the song, and artist tables are expected to be small (and edited less often), it could make sense to distribute the songplay and user tables by key with user_id and the song and artist tables by all. Similarly, sorting could be defined based on expected workloads.

# Repository Contents and Run Instructions

**TODO**: Do APIs match current definition?

Included in the repository are:

* `get_sales_data.py`: Fetches monthly sales data from public API and store in S3
    *  see `-h` for more details
    *  example: `python get_sales_data.py 2012-01 2018-12 --data_spec sales_raw`
* `create_tables.py`: (Drops and) Creates tables for the database
    *  see `-h` for more details
    *  example: `python create_tables.py --db postgres`
* `etl.py`: Performs ETL from raw --> staged --> OLTP database
    *  see `-h` for more details
    *  example: `python etl.py --db postgres`
* `etl_olap.py`: Performs ETL from OLTP --> OLAP
    *  see `-h` for more details
    *  example: `python etl_olap.py --db postgres`
* `sql_queries.py`: Definitions of all SQL queries 
* `utilities.py`: Shared utilities used throughout the code
* `data.yml`: Definition of metadata for the data sources
* `secrets.yml`: Not included in the repository, but should contain data of the form:
    ````yaml
    postgres:
        database:
        host:
        port:
        user:
        password:
    
    aws:
        access_key:
        secret_key:
    
    redshift:
        database:
        host:
        port:
        user:
        password:
        arn:
    
    socrata:
        access_key:
        secret_key:
    ````

# Analytics Comparisons

* Compare between OLTP and OLAP for load and select
* Compare between pg and rs

# Comparison between Postgres and Redshift
s
(for this use case)

# Project Questions:

## Clearly state the rationale for the choice of tools and technologies for the project.

TODO

## Propose how often the data should be updated and why.

Given the nature of the data sources, the current system need only be updated whenever new liquor sales data is added to the database (current dissemination of data lags by ~2 years).  If this was an in-house data source that had frequent updates, the answer to this would depend on what the usage of the data required.  If this was used to provide frequent summary of the sales, etc., ETL of new data would need to be at least as frequent as the analyses (if analysis is daily, ETL needs to be <= daily).    

## Write a description of how you would approach the problem differently under the following scenarios:

TODO

### The data was increased by 100x.

TODO

### The data populates a dashboard that must be updated on a daily basis by 7am every day.

TODO

### The database needed to be accessed by 100+ people.

TODO

# Recommendations and Conclusions

TODO:
