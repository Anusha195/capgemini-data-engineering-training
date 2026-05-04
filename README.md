# Databricks Foundations – Learning README

## Project Title  
Databricks Data Engineering Learning Journey (Capgemini Training Program)

---

## Objective  
This repository documents my end-to-end learning of Databricks, SQL, PySpark, and Delta Lake through a structured 3-week hands-on training program.

The goal was to become job-ready for Data Engineering roles by building real-world data pipelines using industry practices like Medallion Architecture (Bronze → Silver → Gold).

---

## What I Have Learned (Overall)

By completing this training, I gained:

- Strong understanding of Databricks workspace (clusters, notebooks, DBFS, catalog)
- Ability to write production-level SQL (joins, window functions, CTEs, deduplication)
- Hands-on experience with PySpark DataFrame transformations
- Knowledge of Delta Lake features (ACID, MERGE, Time Travel, Schema Evolution)
- Ability to design end-to-end data pipelines
- Understanding of data quality checks and debugging
- Experience in jobs, workflows, and scheduling
- Awareness of Unity Catalog, Git integration, and cloud storage fundamentals

---

## Learning Schedule & Topics Covered

### Week 1: Foundations (SQL + PySpark Basics)

Key Focus: Data understanding and strong SQL foundation

#### Topics Covered:
- Data Engineering fundamentals (ETL vs ELT, Lakehouse concept)
- SQL:
  - Joins (Inner, Left, Outer)
  - Aggregations, GROUP BY, HAVING
  - Window Functions (ROW_NUMBER, RANK, LAG/LEAD)
  - Deduplication and Incremental Logic
- PySpark:
  - DataFrames (select, filter, join, groupBy)
  - Transformations vs Actions
  - Handling NULLs and type casting
- Data ingestion:
  - Full load vs Incremental load
  - Parameterized notebooks

#### Key Learning:
- Writing job-ready SQL queries
- Translating SQL logic into PySpark code
- Understanding real-world data issues (duplicates, nulls, inconsistencies)

---

### Week 2: Core Databricks & Delta Lake

Key Focus: Pipeline building and performance optimization

#### Topics Covered:
- Delta Lake:
  - ACID Transactions
  - MERGE (Upserts)
  - Time Travel
  - Schema Evolution
- Medallion Architecture:
  - Bronze (Raw data)
  - Silver (Cleaned data)
  - Gold (Business layer)
- Performance Optimization:
  - Partitioning
  - Small files problem
  - OPTIMIZE and ZORDER
  - Broadcast joins
- Incremental ingestion:
  - Auto Loader (cloudFiles concept)
- Databricks SQL and serving layer
- Workflows and scheduling:
  - Jobs
  - Multi-task pipelines
  - Parameters and retry logic

#### Key Learning:
- Building end-to-end pipelines
- Applying performance tuning techniques
- Handling incremental data processing

---

### Week 3: Integration, Debugging & Capstone

Key Focus: Real-world debugging and project implementation

#### Topics Covered:
- Debugging:
  - Reading logs and Spark UI
  - Fixing schema mismatches, null errors, permission issues
- Governance:
  - Unity Catalog
  - Access control basics
- Git integration in Databricks
- Cloud storage concepts
- Capstone Project:
  - Designing data model
  - Implementing Bronze → Silver → Gold pipeline
  - Adding data quality checks
  - Creating workflows
  - Running incremental pipelines

#### Key Learning:
- Debugging real pipeline failures
- Building a complete data engineering project
- Explaining architecture clearly for interviews

---

## Dataset Experience

Worked on realistic datasets such as:

- E-commerce (orders, customers, products)
- IoT sensor data
- Reference/master data

### Key Challenges Solved:
- Handling duplicates  
- Managing missing values  
- Working with multiple related tables  
- Handling late-arriving data  
- Managing schema inconsistencies  

---

## Tools & Technologies Used

- Databricks
- PySpark
- SQL
- Delta Lake
- Git

---

## Key Concepts Implemented

- Medallion Architecture (Bronze → Silver → Gold)
- Incremental Data Processing
- Delta MERGE (Upserts)
- Data Quality Checks
- Partitioning and Optimization
- Workflow Scheduling
- Debugging and Error Handling

---

## What Makes This Project Strong

- Based on real-world datasets and scenarios  
- Focus on problem-solving, not just syntax  
- Covers end-to-end pipeline development  
- Includes performance tuning and debugging  
- Aligns with industry-level data engineering roles  

---

## Future Scope

- Implement streaming pipelines (Structured Streaming, Kafka concepts)
- Add advanced optimization techniques (data skew handling, adaptive execution)
- Integrate CI/CD pipelines
- Use orchestration tools like Airflow
- Build dashboards for analytics
- Expand data governance practices

---

## Conclusion

This training helped me transition from basic SQL knowledge to building production-ready data pipelines using Databricks.

I am now confident in:
- Designing data architectures  
- Writing optimized queries  
- Handling real-world data challenges  
- Building scalable data engineering solutions  
