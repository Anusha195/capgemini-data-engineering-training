# SmartBank Lakehouse Analytics

## Project Overview

SmartBank Lakehouse Analytics is a data engineering and analytics project built on the Databricks Lakehouse platform. The objective is to design an end-to-end pipeline that ingests raw banking data, processes it through structured layers, and generates business insights for decision-making.

The project follows the Medallion Architecture (Bronze, Silver, Gold) to ensure scalability, data quality, and performance.

Key scale of the project:

* Customers: 45,000+
* Transactions: 5,000
* Branches: 10
* Complaints: 500+ 

---

## Dataset Description

The project uses multiple datasets representing different banking domains:

* transactions.csv
  Contains transaction-level data including transaction ID, customer ID, amount, channel, and branch.

* customers.csv
  Contains customer demographic and financial details such as age, job, balance, and loan status.

* branch_master.csv
  Contains branch metadata including branch ID, city, state, and type.

* complaints.csv
  Contains customer complaints with category, priority, status, and channel.

* digital_logins.csv
  Contains login activity such as device type, login time, and session duration.

All datasets are stored in AWS S3 and ingested into Databricks.

---

## Architecture

The project follows a layered Lakehouse architecture:

Source Files (CSV/JSON)
→ AWS S3 (Raw Storage)
→ Bronze Layer (Raw Delta Tables)
→ Silver Layer (Cleaned and Enriched Data)
→ Gold Layer (Aggregated KPI Tables)
→ Dashboards (Databricks SQL)

---

## Bronze Layer

The Bronze layer handles raw data ingestion.

* Data is ingested using Databricks Auto Loader.
* Raw schema is preserved without transformations.
* Data is stored in Delta format.
* Metadata columns are added, including source_file, load_time, and batch_id.
* Supports incremental loading and append-only operations.

This layer ensures traceability and auditability of raw data.

---

## Silver Layer

The Silver layer performs data cleaning, validation, and enrichment.

Transformations include:

* Handling missing values
* Standardizing categorical fields such as channel and branch names
* Converting and normalizing date formats
* Masking or tokenizing sensitive data (PII)
* Creating surrogate keys
* Joining datasets to enrich transactions with customer and branch data
* Deriving additional metrics such as monthly activity

Data quality rules enforced:

* Mandatory presence of customer_id and account_id
* Transaction amount must be greater than zero
* Duplicate records are removed
* branch_id must exist in the branch_master table
* Row count validation between source and target

---

## Gold Layer

The Gold layer provides business-level aggregated tables for analytics and reporting.

Key tables include:

* gold_branch_performance
* gold_campaign_conversion
* gold_customer_segments
* gold_monthly_deposits
* gold_digital_adoption
* gold_complaint_trends
* gold_risk_alerts

These tables are optimized for dashboarding and reporting.

---

## Key Insights

* Digital usage is evenly distributed across devices
* Customer segments are balanced, enabling targeted marketing
* A significant portion of customers are inactive
* Monthly revenue shows stable trends with periodic growth
* Complaints highlight service improvement opportunities

---

## Technologies Used

* Databricks Lakehouse Platform
* Apache Spark (PySpark)
* Delta Lake
* AWS S3
* Databricks SQL
* Auto Loader
* Unity Catalog

---

## Features Implemented

* Incremental data ingestion using Auto Loader
* Delta Lake features such as ACID transactions and time travel
* Data validation and quality checks
* Data transformation and enrichment
* KPI generation for dashboards

---

## Security and Governance

* PII masking implemented in the Silver layer
* Role-based access control using Unity Catalog
* Support for row-level and column-level security

---

## Business Value

The project enables:

* Identification of high-value and at-risk customers
* Monitoring of branch performance
* Evaluation of campaign effectiveness
* Tracking complaint trends
* Detection of anomalies and risks

---

## Workflow Automation

* Automated data ingestion and transformation pipelines
* Incremental data processing
* Scalable architecture for growing datasets

---

## Conclusion

SmartBank Lakehouse Analytics demonstrates a scalable and structured approach to building a modern data platform using Databricks. It ensures reliable data processing, governance, and actionable insights for business decision-making.

---

## Team Members

* Nallagangula Anusha
* Kamparam Joshna Gayatri
* Ijjagani Namratha Sri
* Thatha Bhagavathi Vyshnavi
* Akula Vamsi Krishna
* Alladi Chandu Sai
