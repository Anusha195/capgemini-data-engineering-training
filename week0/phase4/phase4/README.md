# Phase 4 – Business Pipeline and Analytics

## Objective
Build an end-to-end data pipeline using PySpark to clean data, perform transformations, and generate meaningful business insights.

## Problem Summary
We were given datasets such as customers and sales.

The tasks included:
- Cleaning the data  
- Performing aggregations  
- Generating business insights  
- Creating a final reporting table  

## Approach
1. Loaded datasets from the /samples/ folder into PySpark DataFrames  
2. Performed data cleaning:
   - Removed null values in key columns  
   - Removed duplicate records  
   - Filtered invalid data (for example, negative amounts)  
3. Joined datasets using customer_id  
4. Applied transformations:
   - Calculated daily sales  
   - Computed city-wise revenue  
   - Identified top customers  
   - Found repeat customers  
5. Created customer segments:
   - Gold → total_spend > 10000  
   - Silver → 5000–10000  
   - Bronze → < 5000  
6. Combined all insights into a final reporting table  
7. Saved output to a writable path  

## Key Transformations Used
- dropna() to remove null values  
- dropDuplicates() to remove duplicate records  
- filter() to remove invalid data  
- join() to combine datasets  
- groupBy() for aggregations  
- agg() to calculate sum and count  
- withColumn() to create new columns  

## Output / Results
The following outputs were generated:
- Daily sales report  
- City-wise revenue  
- Top 5 customers  
- Repeat customers  
- Customer segmentation  
- Final reporting table  

## Data Engineering Considerations
- Cleaned data before joins to ensure accuracy  
- Handled null values carefully  
- Avoided duplicate records after joins  
- Used writable paths for saving output instead of /samples  

## Challenges Faced
- Column name mismatches  
- Syntax errors in PySpark transformations  
- Understanding join logic  
- Handling file write errors due to read-only paths  

## Learnings
- Built an end-to-end data pipeline using PySpark  
- Understood importance of data cleaning  
- Learned how to generate business insights  
- Improved debugging and error handling skills  

## Project Structure
Phase-4  
|-- pyspark_codes.py  
|-- phase4_problem_statement.pdf  
|-- outputs/  
|-- README.md  

## Conclusion
This phase helped in understanding how raw data is processed and transformed into useful business insights using PySpark.