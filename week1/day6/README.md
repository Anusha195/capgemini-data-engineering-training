# Advanced Car Sales Pipeline (SQL + PySpark)

## Objective

To build an end-to-end data pipeline using PySpark and SQL for analyzing car sales data and generating business insights, including customer behavior, brand performance, and dealer analytics.

---

## Dataset Overview

The pipeline uses the following tables:

* `customers` (customer_id, name, city)
* `cars` (car_id, brand, model, price)
* `sales` (sale_id, customer_id, car_id, sale_date, quantity)
* `dealers` (dealer_id, name, city)
* `sales_dealer` (sale_id, dealer_id)

---

## Phase 1: Data Understanding

* Loaded all datasets into PySpark DataFrames
* Verified schema and row counts
* Checked for null values across all tables
* Identified duplicate records using primary keys
* Detected invalid values such as negative prices and inconsistent strings

---

## Phase 2: Data Cleaning

* Removed negative price values from `cars` table
* Trimmed customer names to remove trailing spaces
* Eliminated invalid foreign key records using inner joins
* Ensured clean and consistent data for downstream processing

---

## Phase 3: Data Validation

* Used `left_anti` joins to identify invalid foreign key relationships
* Generated validation checks for:

  * Invalid customer_id in sales
  * Invalid car_id in sales
  * Invalid dealer mappings
* Ensured referential integrity across all tables

---

## Phase 4: Transformations

* Created revenue column using: `price * quantity`
* Generated key metrics:

  * Customer Revenue: total revenue per customer
  * Brand-wise Sales: revenue grouped by car brand
  * City-wise Revenue: revenue aggregated by customer city

---

## Phase 5: Dealer Analytics

* Calculated total revenue per dealer
* Identified top-performing dealers based on revenue
* Computed dealer-city contribution to analyze regional performance

---

## Phase 6: SQL Analysis

* Registered DataFrame as temporary SQL view using `createOrReplaceTempView`
* Performed SQL-based analysis:

  * Top 3 customers per city using window functions
  * Monthly revenue trends using date aggregation
  * Repeat customers based on purchase frequency

---

## Phase 7: Output

* Saved final transformed datasets as CSV files
* Outputs include:

  * Customer revenue
  * Brand-wise sales
  * City-wise revenue
  * SQL analysis results

---

## Key Learnings

* Importance of data cleaning before analysis
* Handling invalid foreign keys using joins
* Use of window functions for ranking problems
* Integration of PySpark and SQL for scalable data processing
* Ensuring data integrity for accurate business insights

---

## Conclusion

This pipeline demonstrates a complete data engineering workflow from raw data processing to business analytics. It highlights best practices in data cleaning, validation, transformation, and SQL-based analysis using PySpark.
