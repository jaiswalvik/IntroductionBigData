from kafka import KafkaProducer
from google.cloud import storage
import pandas as pd
import time
import json
from io import StringIO

# ==============================
# CONFIGURATION
# ==============================
KAFKA_SERVER = "10.128.0.18:9092"   
TOPIC = "streaming-topic"

BUCKET_NAME = "data-streaming-bucket"
FILE_NAME = "data1.csv"

# ==============================
# INIT KAFKA PRODUCER
# ==============================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ==============================
# READ FILE FROM GCS BUCKET
# ==============================
def read_csv_from_gcs(bucket_name, file_name):
    print("Connecting to GCS...")

    client = storage.Client(project="prefab-galaxy-305717")   # Uses VM service account automatically
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    print(f"Downloading {file_name} from bucket {bucket_name}...")

    # Download file content as string
    data = blob.download_as_text()

    print("File downloaded successfully!")

    # Convert string to DataFrame
    df = pd.read_csv(StringIO(data))

    print(f"Total rows loaded: {len(df)}")
    return df


# ==============================
# MAIN LOGIC
# ==============================
def run_producer():
    df = read_csv_from_gcs(BUCKET_NAME, FILE_NAME)

    total_records = min(1000, len(df))  # safety

    print("Starting Producer 1 streaming...")

    for i in range(0, total_records, 10):
        batch = df.iloc[i:i+10].to_dict(orient="records")

        for record in batch:
            record["producer"] = "P1"   # tag for identification
            producer.send(TOPIC, record)

        print(f"Sent batch {i//10 + 1} (records {i} to {i+10})")

        time.sleep(10)   # 10 seconds gap

    producer.flush()
    producer.close()

    print("Producer 1 finished sending 1000 records.")


# ==============================
# ENTRY POINT
# ==============================
if __name__ == "__main__":
    run_producer()
