# Databricks notebook source
customers_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .load("/Volumes/capstone_project/bronze/raw_data/customers.csv")

# COMMAND ----------

customers_df.display()
customers_df.printSchema()
customers_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

customers_df = customers_df \
    .withColumn("load_time", current_timestamp()) \
    .withColumn("source_file", lit("customers.csv"))

# COMMAND ----------

customers_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.bronze.customers")

# COMMAND ----------

transactions_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .load("/Volumes/capstone_project/bronze/raw_data/transactions.csv")

# COMMAND ----------

transactions_df.display()
transactions_df.printSchema()
transactions_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

transactions_df = transactions_df \
    .withColumn("load_time", current_timestamp()) \
    .withColumn("source_file", lit("transactions.csv"))

# COMMAND ----------

transactions_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.bronze.transactions")

# COMMAND ----------

branch_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .load("/Volumes/capstone_project/bronze/raw_data/branch_master.csv")

# COMMAND ----------

branch_df.display()
branch_df.printSchema()
branch_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

branch_df = branch_df \
    .withColumn("load_time", current_timestamp()) \
    .withColumn("source_file", lit("branch_master.csv"))

# COMMAND ----------

branch_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.bronze.branch_master")

# COMMAND ----------

complaints_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .load("/Volumes/capstone_project/bronze/raw_data/complaints.csv")

# COMMAND ----------

complaints_df.display()
complaints_df.printSchema()
complaints_df.count()

# COMMAND ----------

complaints_df = complaints_df \
    .withColumn("load_time", current_timestamp()) \
    .withColumn("source_file", lit("complaints.csv"))

# COMMAND ----------

complaints_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.bronze.complaints")

# COMMAND ----------

logins_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .load("/Volumes/capstone_project/bronze/raw_data/digital_logins.csv")

# COMMAND ----------

logins_df.display()
logins_df.printSchema()
logins_df.count()

# COMMAND ----------

logins_df = logins_df \
    .withColumn("load_time", current_timestamp()) \
    .withColumn("source_file", lit("digital_logins.csv"))

# COMMAND ----------

logins_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.bronze.digital_logins")

# COMMAND ----------

