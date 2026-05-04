# Databricks notebook source
customers_df = spark.read.table("capstone_project.bronze.customers")

# COMMAND ----------

customers_df.display()
customers_df.printSchema()
customers_df.count()

# COMMAND ----------

customers_df = customers_df.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import col

customers_df = customers_df.dropna(subset=["age"])

# COMMAND ----------

customers_df = customers_df.fillna({
    "job": "unknown",
    "marital": "unknown",
    "education": "unknown",
    "default": "unknown",
    "housing": "unknown",
    "loan": "unknown",
    "contact": "unknown",
    "poutcome": "unknown"
})

# COMMAND ----------

from pyspark.sql.functions import trim

customers_df = customers_df.withColumn("job", trim(col("job")))

# COMMAND ----------

customers_df.count()

# COMMAND ----------

customers_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.silver.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transactions

# COMMAND ----------

transactions_df = spark.read.table("capstone_project.bronze.transactions")

# COMMAND ----------

transactions_df.display()
transactions_df.printSchema()
transactions_df.count()

# COMMAND ----------

transactions_df = transactions_df.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import col

transactions_df = transactions_df.dropna(subset=["transaction_id", "customer_id"])

# COMMAND ----------

transactions_df = transactions_df.withColumn(
    "amount",
    col("amount").cast("double")
)

# COMMAND ----------

from pyspark.sql.functions import when

transactions_df = transactions_df.withColumn(
    "amount_category",
    when(col("amount") < 1000, "Low")
    .when(col("amount") < 5000, "Medium")
    .otherwise("High")
)

# COMMAND ----------

transactions_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.silver.transactions")

# COMMAND ----------

transactions_df.display()
transactions_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### branch_master

# COMMAND ----------

branch_df = spark.read.table("capstone_project.bronze.branch_master")

# COMMAND ----------

branch_df.display()
branch_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, trim
# Remove duplicates
branch_df = branch_df.dropDuplicates()

# COMMAND ----------

# Drop null branch_id (important key)
branch_df = branch_df.dropna(subset=["branch_id"])

# COMMAND ----------

# Trim text columns
branch_df = branch_df.withColumn("branch_name", trim(col("branch_name")))

# COMMAND ----------

branch_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.silver.branch_master")

# COMMAND ----------

branch_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### complaints

# COMMAND ----------

complaints_df = spark.read.table("capstone_project.bronze.complaints")

# COMMAND ----------

complaints_df.display()
complaints_df.printSchema()


# COMMAND ----------

# Remove duplicates
complaints_df = complaints_df.dropDuplicates()


# COMMAND ----------

# Drop critical nulls
complaints_df = complaints_df.dropna(subset=["complaint_id", "customer_id"])


# COMMAND ----------

complaints_df = complaints_df.fillna({
    "category": "unknown",
    "status": "unknown",
    "priority": "unknown",
    "channel": "unknown"
})

# COMMAND ----------

complaints_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.silver.complaints")

# COMMAND ----------

# MAGIC %md
# MAGIC ### digital_logins

# COMMAND ----------

logins_df = spark.read.table("capstone_project.bronze.digital_logins")

# COMMAND ----------

logins_df.display()
logins_df.printSchema()

# COMMAND ----------

# Remove duplicates
logins_df = logins_df.dropDuplicates()

# COMMAND ----------

# Drop critical nulls
logins_df = logins_df.dropna(subset=["login_id", "customer_id"])

# COMMAND ----------

# Fill optional columns
logins_df = logins_df.fillna({
    "device_type": "unknown",
    "status": "unknown"
})

# COMMAND ----------

logins_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.silver.digital_logins")

# COMMAND ----------

