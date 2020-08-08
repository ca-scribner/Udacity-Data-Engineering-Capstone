# # install csvutils
# pip install csvkit

# # install aws cli
# curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
# unzip awscliv2.zip
# sudo ./aws/install

# Get a subset of this data in S3
aws s3api select-object-content --bucket udacity-de-capstone-182 \
   --key raw-data/iowa_liquor_sales.csv \
   --expression "select * from s3object limit 10000" \
   --expression-type \'SQL\' \
   --input-serialization \'{"CSV": {}, "CompressionType": "NONE"}\' \
   --output-serialization \'{"CSV": {}}\' "subset.csv"

csvsql subset.csv | tee subset_csvstat.csv

# Returns:
# CREATE TABLE subset (
# 	"Invoice/Item Number" VARCHAR NOT NULL, 
# 	"Date" DATE NOT NULL, 
# 	"Store Number" DECIMAL NOT NULL, 
# 	"Store Name" VARCHAR NOT NULL, 
# 	"Address" VARCHAR NOT NULL, 
# 	"City" VARCHAR NOT NULL, 
# 	"Zip Code" VARCHAR NOT NULL, 
# 	"Store Location" VARCHAR, 
# 	"County Number" DECIMAL, 
# 	"County" VARCHAR, 
# 	"Category" DECIMAL, 
# 	"Category Name" VARCHAR, 
# 	"Vendor Number" DECIMAL NOT NULL, 
# 	"Vendor Name" VARCHAR NOT NULL, 
# 	"Item Number" DECIMAL NOT NULL, 
# 	"Item Description" VARCHAR NOT NULL, 
# 	"Pack" DECIMAL NOT NULL, 
# 	"Bottle Volume (ml)" DECIMAL NOT NULL, 
# 	"State Bottle Cost" DECIMAL NOT NULL, 
# 	"State Bottle Retail" DECIMAL NOT NULL, 
# 	"Bottles Sold" DECIMAL NOT NULL, 
# 	"Sale (Dollars)" DECIMAL NOT NULL, 
# 	"Volume Sold (Liters)" DECIMAL NOT NULL, 
# 	"Volume Sold (Gallons)" DECIMAL NOT NULL
# );


csvstat subset.csv | tee subset_csvstat.csv
# Returns:
# 1. "Invoice/Item Number"

# 	Type of data:          Text
# 	Contains null values:  False
# 	Unique values:         9999
# 	Longest value:         16 characters
# 	Most common values:    S15295000128 (1x)
# 	                       S16288500003 (1x)
# 	                       S09399500011 (1x)
# 	                       S04545700011 (1x)
# 	                       S16464700042 (1x)

#   2. "Date"

# 	Type of data:          Date
# 	Contains null values:  False
# 	Unique values:         832
# 	Smallest value:        2012-01-03
# 	Largest value:         2018-04-11
# 	Most common values:    2015-10-19 (27x)
# 	                       2013-05-21 (26x)
# 	                       2018-04-10 (26x)
# 	                       2015-04-15 (26x)
# 	                       2014-11-05 (24x)

# ...

