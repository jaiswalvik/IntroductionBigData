from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

spark = SparkSession.builder.appName("DT_Inference").getOrCreate()

model_path = "gs://spark_mllib/models/final_dt_model"
test_path = spark.conf.get("spark.test.path")

# Load model
model = PipelineModel.load(model_path)

# Load test data
test_df = spark.read.csv(test_path, header=True, inferSchema=True)

# Predict
predictions = model.transform(test_df)

# Save predictions
output_path = "gs://spark_mllib/predictions/output"
predictions.select("prediction").write.mode("overwrite").csv(output_path)

spark.stop()