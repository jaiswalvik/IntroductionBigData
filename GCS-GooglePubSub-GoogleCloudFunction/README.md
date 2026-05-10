## Project Description

This project uses Google Cloud Platform (GCP) - GCS,Google PUB/Sub & Google Cloud Function:

1. Read a text file from Google Cloud Storage (GCS) that is used to calculate longest line.
2. Perform the same operation from cloud function, vm & local machine.
3. Display and save the results back to GCS.
4. Use google pub/sub to store file metadata that is used by various clients to read/write to GCS.

## Files Included
* Test -> input file used by all the processes
* Output files -> Output files from the various operations starting witn output_cf_,output_local_ & output_vm_
* vm -> folder having code that is ran on VM.
* local -> floder that has the code to run code locally
* cloud_function -> that has the code to run code in cloud function
* README.md → Documentation file