# Phase 1 – SQL & PySpark Basics

## Objective
In this phase, the goal is to understand basic data querying using SQL and apply the same operations using PySpark DataFrames.


## Problem Summary
We were given a dataset (customers table) to perform basic operations.

The task was to:
Retrieve all records  
Filter data based on conditions  
Select specific columns  
Perform basic aggregation  


## Approach
1. Created tables and inserted sample data using SQL  
2. Loaded the same data into PySpark DataFrames  
3. Performed basic operations:  
   o Selecting columns  
   o Filtering data  
4. Applied aggregation using groupBy  
5. Displayed results using show()  


## Key Transformations Used
select() → to choose required columns  
filter() → to apply conditions  
groupBy() → to group data  
agg() → to perform aggregations  
count() → to count records  


## Output / Results
The following outputs were generated:
All customer records  
Filtered customer data (based on city and age)  
Selected columns (customer_name, city)  
Count of customers city-wise  

Screenshots of outputs are available in the outputs/ folder.


## Data Engineering Considerations
Ensured correct data while creating tables  
Verified results between SQL and PySpark outputs  
Maintained consistency between both implementations  


## Challenges Faced
Understanding PySpark syntax initially  
Mapping SQL queries to PySpark operations  


## Learnings
Basics of SQL querying (SELECT, WHERE, GROUP BY)  
How to perform the same operations using PySpark  
Understanding difference between SQL queries and DataFrame operations  


## Files in this Folder
sql starter code.sql → SQL queries and table setup  
pyspark starter code.py → PySpark setup implementation
pyspark_codes.py → pyspark task solution scodes
sql_codes.txt → sql task solution queries
outputs/ → Output screenshots for both sql and pyspark
