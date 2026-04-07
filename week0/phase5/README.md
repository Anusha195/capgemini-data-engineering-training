# Phase 5 – Databricks + Olist: End-to-End Data Engineering Pipeline  

## Objective  
In this phase, the goal is to work with a real-world multi-table dataset in Databricks using PySpark.  
This includes data ingestion, validation, modeling, advanced analytics using window functions, and building a final reporting dataset.  

---

## Problem Summary  
We were given the **Olist Brazilian E-commerce dataset**, which contains multiple related tables such as customers, orders, products, and payments.  

The task was to:  
- Load and manage multiple datasets in Databricks  
- Perform joins across different tables  
- Apply aggregations and window functions  
- Generate analytical insights  
- Build a final reporting table  

---

## Approach  
1. Loaded all CSV datasets into PySpark DataFrames from FileStore  
2. Performed data validation:  
   - Checked schema and data types  
   - Verified data consistency across tables  
3. Joined multiple tables using appropriate keys (`customer_id`, `order_id`, `product_id`)  
4. Applied transformations:  
   - `groupBy()` and aggregations  
   - Window functions (`rank`, `running total`)  
   - Filtering and conditional logic  
5. Created final analytical and reporting datasets  

---

## Key Transformations Used  
- `join()` → to combine multiple tables  
- `groupBy()` → for aggregation  
- `agg()` → to calculate metrics like sum and count  
- `filter()` → to refine data  
- Window functions → for ranking and running totals  
- `withColumn()` → to create new columns (e.g., segmentation)  

---

## Output / Results  
The following outputs were generated:  
- Top 3 customers per city  
- Daily sales with running total  
- Top products per category  
- Customer lifetime value (CLV)  
- Customer segmentation (Gold, Silver, Bronze)  
- Final reporting dataset with customer insights  

Screenshots of outputs are available in the `outputs/` folder.  

---

## Data Engineering Considerations  
- Ensured correct joins to avoid duplicate or incorrect records  
- Validated data before and after transformations  
- Handled schema consistency across datasets  
- Used window functions efficiently for analytical queries  

---

## Challenges Faced  
- Understanding relationships between multiple tables  
- Writing correct join conditions across datasets  
- Applying window functions properly  
- Managing multiple transformations in a single pipeline  

---

## Learnings  
- How to build an end-to-end data pipeline in Databricks  
- Practical use of window functions in analytics  
- Importance of data validation and correct joins  
- Working with real-world multi-table datasets  

---

## Files in this Folder  
- `solution.py` → PySpark implementation  
- `phase5_problem_statement.pdf` → Problem description  
- `outputs/` → Output screenshots  

---

## Summary  
Built a complete end-to-end data pipeline using Databricks and PySpark on a real-world dataset.  
Learned advanced transformations including joins, aggregations, and window functions for analytics.  