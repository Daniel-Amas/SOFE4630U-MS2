from google.cloud import pubsub_v1      
import glob                             
import json
import os 

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="practical-now-448920-i6"
topic_name = "csv_data";   
subscription_id = "csv_data-sub";   

# create a subscriber to the subscriber for the project using the subscription_id
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = 'projects/{}/topics/{}'.format(project_id,topic_name);

print(f"Listening for messages on {subscription_path}..\n")

# A callback function for handling received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # convert from bytes to string (deserialization)
    message_data = json.loads(message.data.decode('utf-8'))
    
    print("Consumed record with value : {}" .format(message_data))
   
   # Report To Google Pub/Sub the successful processed of the received messages
    message.ack()
    
with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        
