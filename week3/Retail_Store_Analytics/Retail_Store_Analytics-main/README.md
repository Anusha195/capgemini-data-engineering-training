# Retail Store Sales Lakehouse Project (Databricks DLT)

## 📌 Overview

This project implements an **end-to-end Lakehouse data pipeline** using **Databricks Delta Live Tables (DLT)** following the **Medallion Architecture (Bronze → Silver → Gold)**.

It processes retail data from multiple sources, performs cleaning and transformations, and generates **business-ready insights, KPIs, and alerts**.

---

## 🎯 Objectives

* Build scalable data pipelines using **DLT**
* Implement **data quality checks & transformations**
* Apply **Slowly Changing Dimensions (SCD Type 2)**
* Create **fact & dimension tables**
* Generate **business KPIs & dashboards**
* Implement **inventory alert system**
* Apply **optimization techniques (OPTIMIZE & ZORDER)**

---

## 🏗️ Architecture

```
Raw Data (S3)
     ↓
Bronze Layer (Ingestion)
     ↓
Silver Layer (Cleaning + SCD)
     ↓
Gold Layer (Analytics + KPIs)
     ↓
Dashboard / Alerts
```

---

## 🥉 Bronze Layer

* Ingests raw data using **Auto Loader (cloudFiles)**
* Sources:

  * Customers
  * Products
  * Sales
  * Stores
  * Inventory Events
* Adds metadata:

  * ingestion timestamp
  * source file
  * source system

---

## 🥈 Silver Layer

* Data cleaning & validation
* Type casting and normalization
* Duplicate removal
* Data quality checks using:

  * `@dlt.expect`
  * `@dlt.expect_or_drop`
* Implements:

  * **SCD Type 2** for:

    * Customers
    * Products
    * Stores
* Generates **Surrogate Keys (SK)** using SHA-256

---

## 🥇 Gold Layer

### ⭐ Fact Table

* **fact_sales**

  * Contains transactional data
  * Uses surrogate keys:

    * customer_sk
    * product_sk
    * store_sk

---

### 📊 Dimension Tables

* dim_customer
* dim_product
* dim_store

---

### 📈 Analytical Tables / KPIs

#### 🔹 Revenue & Sales

* monthly_revenue_trend
* daily_store_sales
* top_products

#### 🔹 Customer Analytics

* customer_360
* customer segmentation (Platinum, Gold, Silver, Bronze)

#### 🔹 Discount Analysis

* discount_impact_analysis

  * before vs after discount revenue
  * discount loss %

#### 🔹 Payment Insights

* payment_analysis

#### 🔹 Inventory Monitoring

* inventory_risk
* store_inventory_alerts

---

## 🚨 Alert System

* Detects:

  * Out of stock
  * Low stock
  * Critical stock levels
* Sends alerts via:

  * Email (SMTP)
* Implemented using a **separate notebook after pipeline**

---

## Optimization Strategy

Since DLT creates views, optimization is applied on **materialized tables**:

### Steps:

1. Convert DLT views → Delta tables
2. Apply:

   * `OPTIMIZE` → file compaction
   * `ZORDER` → query performance

### Example:

```sql
CREATE OR REPLACE TABLE gold.fact_sales_physical AS
SELECT * FROM gold.fact_sales;

OPTIMIZE gold.fact_sales_physical;

OPTIMIZE gold.fact_sales_physical
ZORDER BY (customer_sk, product_sk, store_sk);
```

---

## Dashboard Insights

Key business insights visualized:

* Revenue by Category
* Top Products
* Store Performance
* Customer Segmentation
* Payment Distribution
* Discount Impact
* Inventory Alerts

---

## Technologies Used

* Databricks
* Delta Live Tables (DLT)
* PySpark
* Delta Lake
* SQL
* AWS S3
* SMTP (Email Alerts)

---

## Limitations (Free Databricks)

* Limited support for:

  * OPTIMIZE
  * ZORDER
* No Photon engine
* Limited job scheduling features

---

## Future Enhancements

* Real-time streaming dashboards
* Integration with:

  * Power BI / Tableau
* Advanced ML models:

  * Demand forecasting
  * Customer churn prediction
* Automated alerting via:

  * Slack / Teams

---

## Key Learnings

* End-to-end Lakehouse pipeline design
* Handling SCD Type 2 in streaming
* Data quality enforcement in DLT
* Performance optimization strategies
* Business-driven analytics

---

## Summary

> “This project demonstrates a complete Lakehouse pipeline using Databricks DLT, implementing medallion architecture, SCD Type 2, and generating business insights such as customer segmentation, discount analysis, and inventory alerts, with performance optimization using Delta Lake features.”

---
