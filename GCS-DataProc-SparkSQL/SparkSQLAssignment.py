from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SCD_Analysis").getOrCreate()

# -------------------------------
# Load CSVs into temporary views
# -------------------------------

spark.read.option("header", "true").csv("gs://customer-transaction/input/customer_master.csv").createOrReplaceTempView("customers")

spark.read.option("header", "true").csv("gs://customer-transaction/input/customer_updates.csv").createOrReplaceTempView("customer_updates")

spark.read.option("header", "true").csv("gs://customer-transaction/input/transactions.csv").createOrReplaceTempView("transactions")

# Cast numeric columns
spark.sql("""
CREATE OR REPLACE TEMP VIEW transactions_cast AS
SELECT
    transaction_id,
    customer_id,
    transaction_date,
    CAST(transaction_amount AS DOUBLE) AS amount,
    payment_mode,
    city
FROM transactions
""")
# -------------------------------
# 1. Aggregation Queries
# -------------------------------

# Top 5 customers by total transaction amount
top5 = spark.sql("""
SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM transactions_cast
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 5
""")

top5.show()

# Number of customers per city
customers_per_city = spark.sql("""
SELECT
    city,
    COUNT(*) AS num_customers
FROM customers
GROUP BY city
""")

customers_per_city.show()

# Average transaction value
avg_txn = spark.sql("""
SELECT
    AVG(amount) AS avg_transaction_value
FROM transactions_cast
""")

avg_txn.show()
# -------------------------------
# 2. Save outputs to GCS
# -------------------------------

top5.write.mode("overwrite").option("header", "true") \
    .csv("gs://customer-transaction/output/top_customers")

customers_per_city.write.mode("overwrite").option("header", "true") \
    .csv("gs://customer-transaction/output/customers_per_city")

avg_txn.write.mode("overwrite").option("header", "true") \
    .csv("gs://customer-transaction/output/avg_transaction")
# -------------------------------
# 3. SCD Type I (Overwrite)
# -------------------------------

# BEFORE
print("=== BEFORE SCD TYPE I ===")
spark.sql("SELECT * FROM customers").show()

scd_type1 = spark.sql("""
SELECT
    m.customer_id,
    COALESCE(u.customer_name, m.customer_name) AS customer_name,
    COALESCE(u.city, m.city) AS city,
    COALESCE(u.dob, m.dob) AS dob,
    m.effective_date,
    m.expiry_date,
    m.current_flag
FROM customers m
LEFT JOIN customer_updates u
ON m.customer_id = u.customer_id
""")

print("=== AFTER SCD TYPE I ===")
scd_type1.show()

scd_type1.write.mode("overwrite").option("header", "true") \
    .csv("gs://customer-transaction/output/scd_type1")
# -------------------------------
# 4. SCD Type II (History tracking)
# -------------------------------

print("=== BEFORE SCD TYPE II ===")
spark.sql("SELECT * FROM customers").show()

# Step 1: Expire old records
expired = spark.sql("""
SELECT
    m.customer_id,
    m.customer_name,
    m.city,
    m.dob,
    m.effective_date,
    u.change_date AS expiry_date,
    0 AS current_flag
FROM customers m
JOIN customer_updates u
ON m.customer_id = u.customer_id
WHERE m.current_flag = 1
""")

# Step 2: Insert new records
new_records = spark.sql("""
SELECT
    u.customer_id,
    u.customer_name,
    u.city,
    u.dob,
    u.change_date AS effective_date,
    NULL AS expiry_date,
    1 AS current_flag
FROM customer_updates u
""")

# Step 3: Keep unchanged records
unchanged = spark.sql("""
SELECT *
FROM customers
WHERE customer_id NOT IN (
    SELECT customer_id FROM customer_updates
)
""")


# Step 4: Combine all
scd_type2 = expired.union(new_records).union(unchanged)

print("=== AFTER SCD TYPE II ===")
scd_type2.show()

scd_type2.write.mode("overwrite").option("header", "true") \
    .csv("gs://customer-transaction/output/scd_type2")
spark.stop()
