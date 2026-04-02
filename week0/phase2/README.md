### Phase 2 – SQL to PySpark (Revised Bridge Pack)

Objective
The objective of this phase was to bridge the gap between basic SQL knowledge and practical PySpark usage by performing real-world data transformations, including joins, filtering, and aggregations.

## Problem Summary
We worked with sample datasets like customers and sales (used instead of orders). The tasks involved:
Performing aggregations such as total and average spending
Identifying top customers based on spending
Finding customers with no transactions
Generating city-wise revenue insights
Filtering and sorting data for meaningful analysis

## Approach

1. Loaded datasets into PySpark DataFrames
2. Performed initial data exploration using show() and printSchema()
3. Applied basic data cleaning by removing rows with null customer_id
4. Wrote SQL queries for each problem
5. Converted SQL queries into equivalent PySpark transformations
6. Executed both SQL and PySpark queries and compared outputs

## Key Transformations Used
groupBy() → for grouping data
agg() → for calculating sum, average, and count
join() → to combine customer and sales data
filter() → to apply conditions on data
orderBy() → to sort results
dropna() → to handle missing values

## Output / Results
The following outputs were generated:
Total spending per customer
Top 3 customers based on total spend
Customers with no sales
City-wise revenue
Average spending per customer
Customers with multiple transactions
Sorted list of customers by total spend

All outputs are stored in the outputs/ folder.

## Data Engineering Considerations
Removed null values to ensure accurate aggregations
Adjusted queries based on dataset differences (sales instead of orders)
Verified results by comparing SQL and PySpark outputs

Challenges Faced
Translating SQL queries into PySpark syntax
Understanding join operations in PySpark
Handling dataset differences and column mappings

 ## Learnings
Mapping SQL operations to PySpark functions
Importance of data cleaning before performing transformations
Performing joins and aggregations on real datasets
Writing efficient and readable PySpark code

## Files in this Folder
codes.py → Contains PySpark implementations, 
outputs/ → Contains query output screenshots
