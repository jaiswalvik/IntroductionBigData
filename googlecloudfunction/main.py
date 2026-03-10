import functions_framework
from google.cloud import storage
import os


@functions_framework.cloud_event
def find_longest_lines(cloud_event):
    """Triggered by a file upload to GCS."""

    print("Function triggered")

    data = cloud_event.data

    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"Processing file: {file_name} from bucket: {bucket_name}")

    # Ignore output files to prevent infinite loop
    if file_name.startswith("output_"):
        print("Skipping output file to avoid re-triggering.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download file content
    content = blob.download_as_text()

    lines = content.splitlines()

    if not lines:
        print("File is empty.")
        return

    # Find longest line length
    max_length = max(len(line) for line in lines)

    # Get all lines with max length
    longest_lines = [line for line in lines if len(line) == max_length]

    print(f"Maximum line length: {max_length}")
    print("Longest line(s):")

    for line in longest_lines:
        print(line)

    # Prepare output content
    output_content = f"Maximum Line Length: {max_length}\n\nLongest Line(s):\n"
    output_content += "\n".join(longest_lines)

    # Save result back to same bucket
    output_blob = bucket.blob(f"output_{file_name}")
    output_blob.upload_from_string(output_content)

    print(f"Output saved as: output_{file_name}")
    print("Event ID:", cloud_event["id"])