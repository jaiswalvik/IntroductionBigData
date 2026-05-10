from google.cloud import pubsub_v1
import json
from processor import process_file

PROJECT_ID = "prefab-galaxy-305717"
SUBSCRIPTION_ID = "sub-vm"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    
    file_name = data['file_name']
    bucket_name = data['bucket']

    result = process_file(bucket_name, file_name)
    print(result)

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print("Listening for messages...")

streaming_pull_future.result()
