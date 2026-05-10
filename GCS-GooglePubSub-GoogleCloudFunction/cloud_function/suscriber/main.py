import functions_framework
import base64
import json
from google.cloud import storage

@functions_framework.cloud_event
def subscribe_pubsub(event):
    try:
        pubsub_message = event.data.get("message", {})
        data = pubsub_message.get("data")

        if not data:
            print(f"No data found in event: {event.data}")
            return

        # Decode message
        decoded_data = base64.b64decode(data).decode("utf-8")
        message = json.loads(decoded_data)

        file_name = message.get("file_name")
        bucket_name = message.get("bucket")

        if not file_name or not bucket_name:
            print(f"Invalid message: {message}")
            return

        if not file_name.startswith("input/"):
            print(f"Ignoring non-input file: {file_name}")
            return

        # Process file
        result = process_file(bucket_name, file_name)
        print(result)

    except Exception as e:
        print(f"Error processing Pub/Sub message: {str(e)}")


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
        output_blob = bucket.blob(f"output/cf_{clean_name}")
        output_blob.upload_from_string(output)

        return f"Processed {file_name}, output saved as output_{file_name}"

    except Exception as e:
        return f"Error in process_file: {str(e)}"