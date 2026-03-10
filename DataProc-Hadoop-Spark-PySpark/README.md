## Project Description

This project uses Google Cloud Platform (GCP) and PySpark in Dataproc cluster to:

1. Download a text file from Google Cloud Storage (GCS)

2. Separate the data to given time bins.

3. Display and save the results back to GCS.

4. Use both RDD and dataframe to store the output to GCS.

## Files Included

- `Dataproc_RDD_DataFrame.py` → PySpark script used in Dataproc cluster  

- `output_rdd_output_part-00001` & `output_rdd_output_part-00000`  → Output file containing rdd output

- `output_df_output_part-00000-2b3e6ca8-b8c7-4814-be7a-fe65c0d06e4c-c000`  → Output file containing dataframe output

- `README.md` → Documentation file