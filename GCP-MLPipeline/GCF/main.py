import functions_framework
import subprocess
from google.cloud import dataproc_v1
from pathlib import Path

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def trigger_dataproc(cloud_event):
    data = cloud_event.data
    file_path = data['name']
    bucket = data['bucket']
    event_type = cloud_event["type"]
    
    if event_type != "google.cloud.storage.object.v1.finalized":
        print("Skipping non-finalize event")
        return

    if "test_data" not in file_path or not file_path.endswith(".csv"):   
        print("Skipping non-target file")
        return

    test_gcs_path = f"gs://{bucket}/{file_path}"
    client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": "us-central1-dataproc.googleapis.com:443"}
    )

    project_id = "prefab-galaxy-305717"
    region = "us-central1"
    cluster_name = "cluster-4d82"

    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": "gs://spark_mllib/code/inference.py",
            "properties": {
                "spark.test.path": test_gcs_path
            }
        } 
    }

    response = client.submit_job(
        request={
            "project_id": project_id,
            "region": region,
            "job": job,
        }
    )

    print("Job submitted:", response.reference.job_id)
