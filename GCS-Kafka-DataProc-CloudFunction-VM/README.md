## Project Description

This project uses Google Cloud Platform (GCP) - GCS, Kafka, Google Cloud Function, PySpark Cluster & VM:

1. Read text file data1.csv from GCS and write to kafka topic via the VM in 10 second interval and write 10 records.
2. Read text file data2.csv from GCS and write to kafka topic via the Cloud Function in 5 second interval and write 5 records.
3. Consume from the kafka topic and print the count in 10 seconds window using spark dataproc cluster
4. Kafka cluster to be setup on a VM.



## Files Included

* Input files -> Data1.csv & Data2.csv
* Input file generator python program -> data-creator.py
* Cloud Function Producer -> cloud-function-producer/cloud-function-producer.py
* VM Producer -> vm-producer/vm-producer.py
* Spark Consumer -> spark-consumer/kafka-consumer.py
* Screenshot -> spark-job & kafka-record-consumer
* README.md → Documentation file

