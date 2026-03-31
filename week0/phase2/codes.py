from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSV Handling in PySpark").getOrCreate()

customers = spark.read.option("header", "true").csv("/samples/customers.csv")
sales = spark.read.option("header", "true").csv("/samples/sales.csv")


from pyspark.sql.functions import sum, avg, count

#1. Total order amount for each customer

sales.groupBy("customer_id").agg(sum("total_amount").alias("total_amount")).show()

#2. Top 3 customers by total spend

sales.groupBy("customer_id").agg(sum("total_amount").alias("total_amount")).orderBy("total_amount", ascending=False).limit(3).show()

#3. Customers with no orders

customers.join(sales, "customer_id", "left").filter(sales.customer_id.isNull()).show()

#4. City-wise total revenue
customers.join(sales, "customer_id").groupBy("city").agg(sum("total_amount").alias("total_revenue")).show()

#5. Average order amount per customer

sales.groupBy("customer_id").agg(avg("total_amount").alias("avg_amount")).show()

#6. Customers with more than one order

sales.groupBy("customer_id").agg(count("*").alias("order_count")).filter("order_count > 1").show()

#7. Sort customers by total spend descending

sales.groupBy("customer_id").agg(sum("total_amount").alias("total_spend")).orderBy("total_spend", ascending=False).show()

