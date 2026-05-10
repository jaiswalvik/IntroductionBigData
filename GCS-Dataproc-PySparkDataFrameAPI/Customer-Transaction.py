from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------------------
# Spark Session
# -----------------------------------
spark = SparkSession.builder.appName("CustomerTransactionAnalysis").getOrCreate()

# -----------------------------------
# GCS Paths
# -----------------------------------
customer_path = "gs://customer-transaction/input/customer_dataset.csv"
transaction_path = "gs://customer-transaction/input/transaction_dataset.csv"

output_cleaned = "gs://customer-transaction/output/cleaned_data"
output_invalid = "gs://customer-transaction/output/invalid_rows"
output_joined = "gs://customer-transaction/output/joined_data"
output_aggregates = "gs://customer-transaction/output/aggregates"

# -----------------------------------
# Load Data
# -----------------------------------
customers = spark.read.option("header",True).csv(customer_path)
transactions = spark.read.option("header",True).csv(transaction_path)


print("Customer Schema")
customers.printSchema()

print("Transaction Schema")
transactions.printSchema()

customers.show(5)
transactions.show(5)

# -----------------------------------
# Standardize city names
# -----------------------------------

customers = customers.withColumn(
    "city",
    initcap(trim(col("city")))
)

customers = customers.withColumn(
    "city",
    when(col("city").isin("Cochin"), "Kochi")
    .otherwise(col("city"))
)

# -----------------------------------
# Convert transaction_date
# -----------------------------------
transactions = transactions.withColumn(
    "transaction_date",
    to_date(col("transaction_date"),"yyyy-MM-dd")
)

# -----------------------------------
# Cast numeric fields
# -----------------------------------
transactions = transactions.withColumn(
    "transaction_amount",
    col("transaction_amount").cast("double")
)

# -----------------------------------
# Column Transformations
# -----------------------------------
transactions = transactions.withColumn(
    "amount_category",
    when(col("transaction_amount") < 100,"Low")
    .when(col("transaction_amount") < 500,"Medium")
    .otherwise("High")
)
transactions = transactions.withColumn(
    "transaction_month",
    month(col("transaction_date"))
)

# -----------------------------------
# 3. Data Cleaning
# -----------------------------------

# Identify invalid customer rows
invalid_customers = customers.filter(
    col("customer_id").isNull() |
    col("city").isNull() |
    col("status").isNull()
)

# Clean customer data
clean_customers = customers.subtract(invalid_customers)

# Remove duplicates
clean_customers = clean_customers.dropDuplicates()

# -----------------------------------
# Invalid transaction rows
# -----------------------------------
invalid_transactions = transactions.filter(
    col("transaction_id").isNull() |
    col("customer_id").isNull() |
    (col("transaction_amount") <= 0) |
    col("transaction_date").isNull()
)

# Clean transaction data
clean_transactions = transactions.subtract(invalid_transactions)

# Remove duplicates
clean_transactions = clean_transactions.dropDuplicates()

# -----------------------------------
# Referential Integrity Check
# -----------------------------------
valid_transactions = clean_transactions.join(
    clean_customers,
    "customer_id",
    "inner"
).select(clean_transactions["*"])

invalid_ref_transactions = clean_transactions.join(
    clean_customers,
    "customer_id",
    "left_anti"
)

invalid_transactions = invalid_transactions.union(invalid_ref_transactions)

# -----------------------------------
# Combine invalid rows
# -----------------------------------
invalid_rows = invalid_customers.unionByName(invalid_transactions, allowMissingColumns=True)

# -----------------------------------
# Save cleaned datasets
# -----------------------------------
clean_customers.write.mode("overwrite").csv(output_cleaned + "/customers", header=True)
valid_transactions.write.mode("overwrite").csv(output_cleaned + "/transactions", header=True)

invalid_rows.write.mode("overwrite").csv(output_invalid, header=True)

print("Invalid Rows Sample")
invalid_rows.show(5)

print("Cleaned Customers")
clean_customers.show(5)

print("Cleaned Transactions")
valid_transactions.show(5)

# -----------------------------------
# Join
# -----------------------------------
joined_df = valid_transactions.join(
    clean_customers,
    "customer_id",
    "inner"
)
joined_df.write.mode("overwrite").csv(output_joined, header=True)

print("Joined Data Sample")
joined_df.show(5)

# -----------------------------------
# 5. Aggregations
# -----------------------------------
agg_customer = joined_df.groupBy("customer_id","customer_name").agg(
    sum("transaction_amount").alias("total_amount"),
    avg("transaction_amount").alias("avg_amount")
)

agg_city = joined_df.groupBy("city").agg(
    sum("transaction_amount").alias("city_total")
)

top_customers = agg_customer.orderBy(
    col("total_amount").desc()
).limit(3)

# Save aggregates
agg_customer.write.mode("overwrite").csv(output_aggregates + "/customer_stats", header=True)

agg_city.write.mode("overwrite").csv(output_aggregates + "/city_stats", header=True)

top_customers.write.mode("overwrite").csv(output_aggregates + "/top_customers", header=True)

print("Customer Aggregates")
agg_customer.show()

print("City Aggregates")
agg_city.show()

print("Top Customers")
top_customers.show()

spark.stop()

