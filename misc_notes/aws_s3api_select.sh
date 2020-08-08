aws s3api select-object-content --bucket udacity-de-capstone-182 \
	--key raw-data/iowa_liquor_sales_h10000.csv \
	--expression "select * from s3object limit 100" \
	--expression-type 'SQL' \
	--input-serialization '{"CSV": {}, "CompressionType": "NONE"}' \
	--output-serialization '{"CSV": {}}' "output.csv"
