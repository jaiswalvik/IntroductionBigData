## Project Description

This project uses Google Cloud Platform (GCP) - GCS, Google Cloud Function, PySpark Cluster:

1. Read iris dataset from pyspark cluster and store it in train_data folder on GCS.
2. Train the model based on this training data on PySpark cluster and store it in models folder on GCS.  .
3. Upload test iris data in cvs format and run infrence on PySpark Cluster using this file.  
4. The predition is stored in a folder on GCS



## Files Included

* Input files -> train_data,test_data
* Input file generator, model generator & prediction generator python program -> create_data.py,training_tuning.py,inference.py
* Cloud Function Producer -> GCF/main.py
* Screenshot -> GCF/log_screenshot
* output-files -> outputfiles from train,test,model & prediction 
* README.md → Documentation file

