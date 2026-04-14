# Week 2 Day 2 – PySpark Pipeline & Delta Lake

## Objective
Build a data pipeline using PySpark and understand Delta Lake operations like MERGE, schema evolution, and time travel.

---

## Dataset

### PySpark Dataset
- order_id, customer_id, product, amount, updated_date  
- Issues: NULLs, string types, incremental updates

### Delta Dataset
- id, customer_id, product, amount  

---

## Tasks Performed

### PySpark Pipeline
- Handled NULLs (amount → 1000)
- Casted columns (amount → int, date → date)
- Created derived columns (bonus, category)
- Created amount_bucket using UDF
- Full load using overwrite
- Incremental load using:
  - date filter
  - row_number() for deduplication
- Parameterized pipeline function

---

### Delta Lake
- Created Delta table
- Inserted new data
- Updated and deleted records
- Performed MERGE (upsert)
- Added new column (schema evolution)
- Used time travel (version history)

---

## Key Concepts

- Incremental Load:
  - Filter new data using date
  - Deduplicate using window function

- MERGE:
  - Update if record exists
  - Insert if new

---

## Issues Faced

- NULL handling failed due to type mismatch
- Duplicates caused by append mode
- Parquet does not support update/delete

---

## Key Learnings

- Difference between append, overwrite, and merge
- Window functions for deduplication
- Importance of schema consistency
- Delta Lake enables update, delete, and upsert

---

## Conclusion

This task covers building a PySpark pipeline and improving it using Delta Lake for efficient and reliable data processing.