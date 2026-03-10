## Project Description



This project uses Google Cloud Platform (GCP) to:



1. Download a text file from Google Cloud Storage (GCS)

2. Identify the longest line(s) in the file

3. Display and save the results back to GCP



If multiple lines share the maximum length, all are reported.





## Files Included



- `longest_line_gcs.py` → Python script used to perform analysis  

- `longest_lines_output.txt` → Output file containing longest line(s)  

- `README.md` → Documentation file



## How to Run



### Step 1: Start GCP VM

Create and SSH into a Compute Engine VM instance.



### Step 2: Create GCS bucket

Create a bucket in GCS and upload a file 'test.txt' 



### Step 3: Install Dependencies

python3 -m venv env

source env/bin/activate

pip install google-cloud-storage



### Step 3: Run the Program

python3 longest_line_gcs.py  





