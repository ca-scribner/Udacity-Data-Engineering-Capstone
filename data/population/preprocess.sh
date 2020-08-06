# Select columns (so they're in the same order - raw files have different orders) and add a year column to each file
csvcut -c population,minimum_age,maximum_age,gender,zipcode,geo_id population_by_zip_2000.csv | csvstack -n year -g 2000 - > population_by_zip_2000_with_year.csv 
csvcut -c population,minimum_age,maximum_age,gender,zipcode,geo_id population_by_zip_2010.csv | csvstack -n year -g 2010 - > population_by_zip_2010_with_year.csv 
