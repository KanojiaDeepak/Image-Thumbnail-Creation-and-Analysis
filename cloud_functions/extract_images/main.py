import base64
import functions_framework
import requests
import json
import os
from google.cloud import storage,bigquery,secretmanager
from datetime import datetime
import logging
logging.basicConfig(level="INFO")

project_id = os.environ.get('PROJECT_ID')
dataset_id = os.environ.get('DATASET_ID')
table_id=os.environ.get('TABLE_ID')

bq_client = bigquery.Client()
bucket_name=os.environ.get('CLOUD_STORAGE_BUCKET_NAME')
storage_client=storage.Client()
bucket = storage_client.bucket(bucket_name)

# Get Unsplash API credentials
secret_client = secretmanager.SecretManagerServiceClient()
project_number=os.environ.get('PROJECT_NUMBER')
secret_name = f"projects/{project_number}/secrets/UNSPLASH_ACCESS_KEY/versions/latest"
response = secret_client.access_secret_version(name=secret_name)
unsplash_access_key = response.payload.data.decode("UTF-8")

def write_to_bq(unique_id,user_input,image_name,image_id,tags):
     # Get the current datetime
     current_datetime = datetime.now()

     # Convert datetime to string in the required format
     processing_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')

     query = f"""
     INSERT INTO `{dataset_id}.{table_id}` (unique_id, keyword, image_id, image_name, tags, processing_datetime) 
     VALUES ("{unique_id}", "{user_input}","{image_id}", "{image_name}","{tags}",DATETIME("{processing_datetime}"))
     """
     try:
          # Execute the query
          query_job = bq_client.query(query)
          query_job.result()
          logging.info("Row inserted successfully in bigquery table.")
     except Exception as e:
          logging.info("Row insertion in bigquery table failed.")
          logging.info(f"Error:{e}")

def upload_image_to_gcs(image_name,image_data,metadata):
     # Upload the image to Cloud Storage
     blob = bucket.blob(image_name)
     blob.metadata=metadata
     blob.upload_from_string(image_data, content_type="image/jpeg")
     logging.info(f"Image {image_name} uploaded to Cloud Storage.")

@functions_framework.cloud_event
def extract_images(cloud_event):
     user_input=base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
     unique_id=cloud_event.data["message"]["messageId"]

     logging.info(f"user_input->{user_input}")

     number_of_images=10

     # Set the API endpoint URL
     url = f'https://api.unsplash.com/search/photos?query={user_input}&per_page={number_of_images}'

     # Set the headers with the API access key
     headers = {
     'Accept-Version': 'v1',
     'Authorization': f'Client-ID {unsplash_access_key}'
     }

     try:
          # Send GET request to the Unsplash API
          response = requests.get(url, headers=headers)

          # Check if the request was successful
          if response.status_code == 200:
               # Parse the JSON response
               data = json.loads(response.content)
               # Extract the list of images from the response
               images = data['results']
               # Loop through the list of images
               for image in images:
                    image_id=image["id"]
                    #checks if this image is already present in the table
                    query_job = bq_client.query(f"SELECT * FROM `{dataset_id}.{table_id}` WHERE image_id = '{image_id}'")
                    result = query_job.result()
                    if len(list(result))>0:
                         continue
                    image_name=image["alt_description"]
                    if image_name==None:
                         image_name=user_input+str(images.index(image))
                    tagslist=[]
                    for t in image["tags"]:
                         tagslist.append(t["title"])
                    tags=",".join(tagslist)
                    logging.info(f"tags->{tags}")
                    metadata={'unique_id':unique_id,'input':user_input,'tags':tags,'image_id':image_id}
                    write_to_bq(unique_id,user_input,image_name,image_id,tags)
                    image_url=image['urls']['regular']
                    # Fetch the image data
                    image_data = requests.get(image_url).content  
                    upload_image_to_gcs(image_name,image_data,metadata)                  
          else:
               logging.error(f"Error: {response.status_code}, {response.text}")
     except Exception as e:
          logging.error(f"Error:{e}")
