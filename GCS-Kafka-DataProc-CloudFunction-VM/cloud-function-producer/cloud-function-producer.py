from google.cloud import storage
from kafka import KafkaProducer
import pandas as pd
import json
import time
from io import StringIO

# =========================
# CONFIG
# =========================
KAFKA_SERVER = "136.111.192.28:9092"
TOPIC = "streaming-topic"
BUCKET_NAME = "data-streaming-bucket"
FILE_NAME = "data2.csv"


# =========================
# READ FROM GCS
# =========================
def read_from_gcs():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME)

    data = blob.download_as_text()
    df = pd.read_csv(StringIO(data))

    return df


# =========================
# CLOUD FUNCTION ENTRY POINT
# =========================
def producer2(request):
    """
    HTTP Cloud Function entry point
    request: flask.Request
    """

    print("Cloud Function Producer 2 started...")

    try:
        df = read_from_gcs()

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        total_records = min(1000, len(df))

        for i in range(0, total_records, 5):
            batch = df.iloc[i:i+5].to_dict(orient="records")

            for record in batch:
                record["producer"] = "P2"
                producer.send(TOPIC, record)

            print(f"Sent batch {i//5 + 1}")

            time.sleep(5)  # 5 sec delay

        producer.flush()
        producer.close()

        return ("Data published successfully from Producer 2!", 200)

    except Exception as e:
        print(f"Error: {str(e)}")
        return (f"Error: {str(e)}", 500)