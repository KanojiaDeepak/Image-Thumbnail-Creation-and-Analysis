-- DDL for image table
CREATE TABLE datasetname.tablename(
	unique_id STRING,
	keyword STRING,
	image_id STRING,
	image_name STRING,
	thumbnail_url STRING,
	tags STRING,
	processing_datetime DATETIME,
)
PARTITION BY DATE(processing_datetime)