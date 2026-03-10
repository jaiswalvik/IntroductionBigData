from google.cloud import storage

# Configuration
BUCKET_NAME = "longest-line-bucket"
INPUT_FILE = "test.txt"
OUTPUT_FILE = "longest_lines_output.txt"


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Downloaded {source_blob_name} to {destination_file_name}")


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"Uploaded {source_file_name} to {destination_blob_name}")


def find_longest_lines(file_path):
    """Finds the longest line(s) in a file."""
    longest_lines = []
    max_length = 0

    with open(file_path, "r") as file:
        for line in file:
            stripped_line = line.rstrip('\n')
            length = len(stripped_line)

            if length > max_length:
                max_length = length
                longest_lines = [stripped_line]
            elif length == max_length:
                longest_lines.append(stripped_line)

    return max_length, longest_lines


def main():
    local_input = "downloaded_test.txt"
    local_output = OUTPUT_FILE

    # Step 1: Download file from GCS
    download_blob(BUCKET_NAME, INPUT_FILE, local_input)

    # Step 2: Find longest line(s)
    max_length, longest_lines = find_longest_lines(local_input)

    # Step 3: Print results
    print("\nLongest Line Length:", max_length)
    print("Longest Line(s):")
    for line in longest_lines:
        print(line)

    # Step 4: Save results to output file
    with open(local_output, "w") as f:
        f.write(f"Longest Line Length: {max_length}\n\n")
        f.write("Longest Line(s):\n")
        for line in longest_lines:
            f.write(line + "\n")

    # Step 5: Upload result back to GCS
    upload_blob(BUCKET_NAME, local_output, OUTPUT_FILE)


if __name__ == "__main__":
    main()
