from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "136.111.192.28:9092") \
    .option("subscribe", "streaming-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Use Kafka timestamp (IMPORTANT FIX)
df = df.selectExpr(
    "CAST(value AS STRING)",
    "timestamp"
)

df = df.withWatermark("timestamp", "20 seconds")

windowed = df.groupBy(
    window(col("timestamp"), "10 seconds", "5 seconds")
).count()

query = windowed.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()