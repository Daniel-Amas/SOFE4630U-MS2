import os
import json
import glob
import csv
from google.cloud import pubsub_v1


# Search the current directory for the JSON file (including the service account key)
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

project_id = "practical-now-448920-i6"
topic_id = "csv_data"

csv_file_path = 'dataset/Labels.csv'

# Create a Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

with open(csv_file_path, mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Convert row to dictionary
        record = dict(row)
        # Serialize dictionary to JSON
        message = json.dumps(record).encode('utf-8')
        # Publish message to Pub/Sub topic
        print("Producing a record: {}".format(message))  
        future = publisher.publish(topic_path, message)
        
        future.result()

print("All records published")


