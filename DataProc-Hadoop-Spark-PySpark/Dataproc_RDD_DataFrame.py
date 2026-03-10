from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.master("yarn").appName("bigdata-Week4-assignment").getOrCreate()

sc = spark.sparkContext
storage_bucket= "gs://time-bin"
inputdata="input"
datafile="click_file.txt"
url=storage_bucket+"/"+inputdata+"/"+datafile

clickFile = sc.textFile(url)
header = clickFile.first()
data = clickFile.filter(lambda line: line != header)

# RDD APPROACH
from datetime import datetime

def get_time_bin(line):
    fields = line.split(",")
    timestamp = fields[1]
    hour_value = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").hour
    
    if 0 <= hour_value < 6:
        return ("0-6", 1)
    elif 6 <= hour_value < 12:
        return ("6-12", 1)
    elif 12 <= hour_value < 18:
        return ("12-18", 1)
    else:
        return ("18-24", 1)

# Map and reduce
result = data.map(get_time_bin).reduceByKey(lambda a, b: a + b).sortByKey()

# Collect and write to bucket
outputdata="output"
filename="rdd_output"
rdd_output_path = storage_bucket+"/"+outputdata+"/"+filename

result.map(lambda x: f"{x[0]}-->{x[1]}").saveAsTextFile(rdd_output_path)

# Read CSV
df = spark.read.csv(url, header=True, inferSchema=True)

from pyspark.sql.functions import hour, col, when
# Extract hour
df_with_hour = df.withColumn("hour", hour(col("timestamp")))

# Create time bin column
df_with_bins = df_with_hour.withColumn(
    "time_bin",
    when((col("hour") >= 0) & (col("hour") < 6), "0-6")
    .when((col("hour") >= 6) & (col("hour") < 12), "6-12")
    .when((col("hour") >= 12) & (col("hour") < 18), "12-18")
    .otherwise("18-24")
)

# Group and count
result = df_with_bins.groupBy("time_bin").count().orderBy("time_bin")

outputdata="output"
filename="df_output"
df_output_path = storage_bucket+"/"+outputdata+"/"+filename

result.write.mode("overwrite").csv(df_output_path, header=True)


sc.stop()