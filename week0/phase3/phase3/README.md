## Phase 3 – ETL & Pipeline using PySpark

## Objective

In this phase, the goal is to build a complete ETL (Extract, Transform, Load) pipeline using PySpark. This includes reading data, cleaning it, applying transformations, and generating final outputs.

## Problem Summary

We were given datasets (customers and sales).
The task was to:
Read and inspect data from files
Clean missing or invalid data
Perform transformations and aggregations
Generate meaningful business insights
Build a reusable ETL pipeline

## Approach

1. Loaded datasets into PySpark DataFrames
2. Performed data cleaning:
    Removed null values using dropna()
    Filtered invalid records using filter()
3. Joined datasets using customer_id
4. Applied transformations:
    groupBy() and aggregations
    Window function using row_number()
5. Generated final reporting dataset
6. Saved output to files

## Key Transformations Used

read() - to load data
dropna() - to handle missing values
filter() - to remove invalid data
join() - to combine datasets
groupBy() - for aggregation
agg() - to calculate metrics like sum, count
row_number() - to rank data within groups
write() - to store final output

## Output / Results

The following outputs were generated:
Daily sales summary
City-wise revenue
Top customer in each city
Final reporting dataset (customer-wise spend and order count)
Outputs/screenshots are available in the `outputs/` folder.

### Data Engineering Considerations

Handled missing values to ensure clean data
Used proper joins to avoid duplicate or incorrect data
Applied transformations in ETL sequence (Extract → Transform → Load)
Ensured reusability by building a pipeline

### Challenges Faced

Understanding ETL flow and combining multiple steps
Handling errors related to functions and imports
Using window functions like `row_number()`

### Learnings

How to build an end-to-end ETL pipeline in PySpark
Importance of data cleaning before processing
Use of joins, aggregations, and window functions
Thinking like a data engineer instead of writing isolated queries

## Files in this Folder

pyspark_codes.py - PySpark implementation,
pyspark_phase3_problem_statement - has details about this phase3, 
outputs/ - Output screenshots
