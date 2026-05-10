from google.cloud import pubsub_v1,storage
import json
import time


PROJECT_ID = "prefab-galaxy-305717"
SUBSCRIPTION_ID = "sub-local"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def process_file(bucket_name, file_name):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Download file
        content = blob.download_as_text()
        lines = content.splitlines()

        if not lines:
            return f"File {file_name} is empty."

        # Find longest lines
        max_length = max(len(line) for line in lines)
        longest_lines = [line for line in lines if len(line) == max_length]

        output = (
            f"Input File: {file_name}\n"
            f"Max Length: {max_length}\n"
            f"Longest Lines:\n"
            + "\n".join(longest_lines)
        )
        clean_name = file_name.split("/")[-1]    
        # Save result back to GCS
        output_blob = bucket.blob(f"output/local_{clean_name}")
        output_blob.upload_from_string(output)

        return f"Processed {file_name}, output saved as output/local_{clean_name}"

    except Exception as e:
        return f"Error in process_file: {str(e)}"
    
def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    
    file_name = data['file_name']
    bucket_name = data['bucket']

    result = process_file(bucket_name, file_name)
    print(result)

    message.ack()

subscriber.subscribe(subscription_path, callback=callback)

print("Listening locally...")

while True:
    time.sleep(60)

