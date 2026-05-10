import functions_framework
import json
from google.cloud import pubsub_v1

PROJECT_ID = "prefab-galaxy-305717"
TOPIC_ID = "longest-line-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@functions_framework.cloud_event
def publish_file_metadata(event):
    try:
        data = event.data

        file_name = data.get('name')
        bucket_name = data.get('bucket')

        if not file_name or not bucket_name:
            print(f"Invalid event data: {data}")
            return
         
        if not file_name.startswith("input/"):
            print(f"Ignoring file outside input/: {file_name}")
            return

        message = {
            "file_name": file_name,
            "bucket": bucket_name
        }

        # Convert to bytes
        encoded_data = json.dumps(message).encode("utf-8")

        # Publish to Pub/Sub
        future = publisher.publish(topic_path, data=encoded_data)
        future.result()  # Optional: wait for publish confirmation

        print(f"Published message for {file_name} from bucket {bucket_name}")

    except Exception as e:
        print(f"Error: {str(e)}")