# Phase 6 – Spark Playground Exit Sprint (Advanced Practice Lab)

## Objective  
In this phase, the goal is to build strong hands-on experience in PySpark by working on a real-world style dataset.  
This includes performing data cleaning, validation, joins, aggregations, and applying window functions to generate meaningful insights.

---

## Problem Summary  
We were given two datasets:  
- Customers dataset (with missing and inconsistent values)  
- Orders dataset (with nulls, invalid values, and incorrect relationships)  

The task was to:  
- Clean and validate the data  
- Remove invalid and inconsistent records  
- Join datasets properly  
- Perform aggregations like total spend and order count  
- Apply window functions to rank customers  
- Generate a final output dataset  

---

## Work Done  

### Data Cleaning  
- Removed rows with null values in important columns (name, email, amount)  
- Trimmed extra spaces in customer names  
- Filtered out negative and invalid order amounts  
- Removed duplicate records  

### Data Validation  
- Identified invalid customer IDs using left anti join  
- Ensured referential integrity between customers and orders  

### Data Joining  
- Joined customers and orders using inner join  
- Ensed only valid and clean data is used for further processing  

### Data Transformation  
- Calculated total spend per customer  
- Counted number of orders per customer  
- Grouped data using groupBy and aggregation functions  

### Window Functions  
- Applied ranking to customers based on total spend  
- Used window functions to analyze data without reducing rows  

### Data Output  
- Saved final processed dataset into CSV format  
- Used overwrite mode to ensure updated output  

---

## Learnings  

- Real-world data is often messy and requires proper cleaning  
- Data validation is important to avoid incorrect analysis  
- Joins help in combining multiple datasets meaningfully  
- Aggregations provide useful insights like totals and counts  
- Window functions are powerful for ranking and advanced analysis  

---

## Reflection  

This phase helped in understanding the complete data pipeline from raw data to final insights using PySpark.  
It improved confidence in handling real-world datasets and applying advanced transformations effectively.