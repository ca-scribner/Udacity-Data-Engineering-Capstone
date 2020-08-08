# Ingestion by EC2:

-	start EC2
	-	in same region as target bucket
	-	with roles that can access S3
	-	in a security group that allows port 22 (ssh)
	-	base ubuntu image is fine, but doesn't have aws cli
-	ssh to ec2:
	- can copy/paste from the GUI
	-	ssh -i "mypemfile.pem" ubuntu@myec2endpoint.compute.amazonaws.com
-	install cli:
	-	sudo apt install unzip
	-	curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
	-	unzip awscliv2.zip
	-	sudo ./aws/install
-	curl to s3
	-	curl "https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv?accessType=DOWNLOAD" | aws s3 cp - s3://udacity-de-capstone-182/iowa_liquor_sales_.csv

# Ingestion by Lambda:

-	Doable and probably easier overall, but hadn't figured out how to stream from urllib to boto3 yet