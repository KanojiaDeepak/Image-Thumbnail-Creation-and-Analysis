import functions_framework
from google.cloud import storage,bigquery
from PIL import Image
import json
import os
import logging

logging.basicConfig(level="INFO")

def thumbnail(source_filename, destination_filename):
    thumbnail_size = os.environ.get('THUMBNAIL_SIZE')
    thumbnail_size=eval(thumbnail_size)
    with Image.open(source_filename) as image:
        image=image.resize(thumbnail_size)
        image.save(destination_filename)

def write_to_bq(url,image_id):
    bigquery_client=bigquery.Client()
    dataset_id = os.environ.get('DATASET_ID')
    table_id = os.environ.get('TABLE_ID')

    query = f"""
        UPDATE `{dataset_id}.{table_id}` SET thumbnail_url="{url}"
        WHERE image_id="{image_id}"
        """
    try:
        # Execute the query
        query_job = bigquery_client.query(query)
        query_job.result()
        logging.info("Row inserted successfully in bigquery table.")
    except Exception as e:
        logging.error("Row insertion in bigquery table failed.")

@functions_framework.cloud_event    
def generate_thumbnail(cloud_event):
    print(cloud_event)
    data = cloud_event.data

    # source bucket and object name from the event data
    source_bucket_name = data['bucket']
    source_object_name = data['name']

    # destination bucket and object name for the thumbnail image
    destination_bucket_name = os.environ.get('DESTINATION_BUCKET_NAME')
    destination_object_name = f'thumbnail_{source_object_name}'

    try:
        # Download the source image to a local file
        storage_client = storage.Client()
        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_object_name)
        image_id = data['metadata']['image_id']
        # Created_at=data['updated']
        # source_url = source_blob.public_url
        source_filename = '/tmp/source.jpeg'
        source_blob.download_to_filename(source_filename)

        # Generate the thumbnail image
        destination_filename = '/tmp/thumbnail.jpeg'
        thumbnail(source_filename, destination_filename)

        # Upload the thumbnail image to the destination bucket
        destination_bucket = storage_client.bucket(destination_bucket_name)
        destination_blob = destination_bucket.blob(destination_object_name)
        with open(destination_filename, 'rb') as thumbnail_file:
            destination_blob.upload_from_string(thumbnail_file.read(), content_type="image/jpeg")
        logging.info(f"Thumbnail file {destination_object_name} uploaded to bucket successfully.")
        
        #updates the thumbnail url in bigquery table
        write_to_bq(destination_blob.public_url,image_id)
    except Exception as e:
        logging.error(f"Error:{e}")
