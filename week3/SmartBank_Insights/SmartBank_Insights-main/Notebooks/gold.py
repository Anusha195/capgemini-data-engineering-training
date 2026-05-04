# Databricks notebook source
# MAGIC %md
# MAGIC ### KPI 1.Customer Transaction Summary

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg

txn_df = spark.read.table("capstone_project.silver.transactions")

gold_customer_txn = txn_df.groupBy("customer_id").agg(
    count("*").alias("txn_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount")
)

# COMMAND ----------

gold_customer_txn.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.gold.customer_transaction_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI 2: Branch Performance

# COMMAND ----------

branch_df = spark.read.table("capstone_project.silver.branch_master")

gold_branch_perf = txn_df.join(
    branch_df,
    on="branch_id",
    how="inner"
).groupBy("branch_id", "branch_name").agg(
    count("*").alias("total_txns"),
    sum("amount").alias("total_amount")
)

# COMMAND ----------

gold_branch_perf.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.gold.branch_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI 3: Complaint Analysis

# COMMAND ----------

complaints_df = spark.read.table("capstone_project.silver.complaints")

gold_complaints = complaints_df.groupBy("category").agg(
    count("*").alias("complaint_count")
)

# COMMAND ----------

gold_complaints.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.gold.complaint_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI 4: Digital Usage

# COMMAND ----------

logins_df = spark.read.table("capstone_project.silver.digital_logins")

gold_digital = logins_df.groupBy("device_type").agg(
    count("*").alias("login_count")
)

# COMMAND ----------

gold_digital.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.gold.digital_usage")

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI 5. Customer Segmentation

# COMMAND ----------

from pyspark.sql.functions import ntile, col, when
from pyspark.sql.window import Window

window_spec = Window.orderBy(col("total_amount").desc())

customer_segmentation = gold_customer_txn.withColumn(
    "segment_rank",
    ntile(3).over(window_spec)
).withColumn(
    "segment",
    when(col("segment_rank") == 1, "High")
    .when(col("segment_rank") == 2, "Medium")
    .otherwise("Low")
)

# COMMAND ----------

txn_df.printSchema()

# COMMAND ----------

customer_segmentation.select("customer_id", "total_amount", "segment").write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.gold.customer_segmentation")

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI 6. Customer Activity Status

# COMMAND ----------

from pyspark.sql.functions import max, datediff, lit

max_date = txn_df.select(max("txn_date")).collect()[0][0]

activity_df = txn_df.groupBy("customer_id").agg(
    max("txn_date").alias("last_txn_date")
)

customer_activity = activity_df.withColumn(
    "days_inactive",
    datediff(lit(max_date), col("last_txn_date"))
).withColumn(
    "activity_status",
    when(col("days_inactive") <= 30, "Active")
    .when(col("days_inactive") <= 90, "Inactive")
    .otherwise("Dormant")
)

# COMMAND ----------

customer_activity.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.gold.customer_activity")

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI 7:Create Monthly Revenue

# COMMAND ----------

from pyspark.sql.functions import year, month, sum, col

txn_df = spark.read.table("capstone_project.silver.transactions")

monthly_revenue = txn_df.groupBy(
    year("txn_date").alias("year"),
    month("txn_date").alias("month")
).agg(
    sum("amount").alias("monthly_revenue")
).orderBy("year", "month")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag

window_spec = Window.orderBy("year", "month")

monthly_revenue = monthly_revenue.withColumn(
    "prev_month_revenue",
    lag("monthly_revenue").over(window_spec)
)

monthly_revenue = monthly_revenue.withColumn(
    "growth_rate",
    ((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100
)

# COMMAND ----------

monthly_revenue.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("capstone_project.gold.monthly_revenue_trend")

# COMMAND ----------

