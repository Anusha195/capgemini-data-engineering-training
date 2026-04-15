# Week 2 Day 3 – Medallion Architecture Pipeline

## Objective
Build an end-to-end data pipeline using **Medallion Architecture (Bronze, Silver, Gold)** to transform raw data into clean, analytics-ready datasets.

---

## Tasks Completed

### 1. Flipkart Data Pipeline
- Implemented Bronze, Silver, Gold layers
- Processed transactional dataset with real-world issues
- Generated business insights for analysis :contentReference[oaicite:0]{index=0}  

### 2. Synthetic Data Generation
- Generated 20,000+ records using PySpark
- Introduced:
  - NULL values
  - Duplicate records
  - Negative values
  - Incremental updates
- Saved as CSV for pipeline input :contentReference[oaicite:1]{index=1}  

---

## Medallion Architecture

### Bronze Layer (Raw)
- Loaded CSV data
- Stored in Delta format
- No transformations (append-only)

### Silver Layer (Cleaned)
- Handled NULL values (`amount`, `city`)
- Converted data types (amount → int, date → date)
- Removed duplicates (based on `order_id`)
- Kept latest records (handled updates)
- Removed negative values

### Gold Layer (Business)
- Aggregations:
  - Sales by product, category, city
- Customer metrics:
  - Orders count
  - Total spending
- Identified top products and customers

---

## Data Issues Handled
- NULL values
- Duplicate records
- Negative amounts
- Incremental updates

---

## Key Learnings
- Importance of layered architecture
- Data cleaning and validation techniques
- Handling real-world data issues
- Building scalable data pipelines

---

## Tools Used
- PySpark  
- Delta Lake  
- Python  

---

## Outcome
- Built a structured pipeline from raw → clean → insights  
- Created dashboard-ready datasets