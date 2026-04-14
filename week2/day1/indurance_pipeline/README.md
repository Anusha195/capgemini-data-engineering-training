# Insurance Data Engineering Pipeline (PySpark)

## Overview

This project focuses on building a data engineering pipeline for an insurance domain using PySpark. The pipeline processes customer, policy, claim, and agent data to generate meaningful business insights such as premium collection, claim analysis, and risk evaluation.

---

## Dataset Description

* **customers**: Contains customer details (id, name, age, city)
* **policies**: Policy information linked to customers
* **claims**: Claims raised against policies
* **agents**: Agent details managing policies
* **policy_agent**: Mapping between policies and agents

---

## Phase 1: Data Understanding

* Loaded all datasets using PySpark DataFrames

* Checked schema using `printSchema()`

* Verified row counts for each table

* Identified key issues:

  * Negative values in `premium` and `claim_amount`
  * Invalid foreign keys (claims with non-existing policies)
  * Date columns stored as strings

* Understood relationships:

  * Customer → Policy → Claim
  * Policy → Agent

---

## Phase 2: Data Cleaning

The following cleaning steps were applied:

* Converted date columns (`start_date`, `claim_date`) from string to DateType
* Handled negative values:

  * Marked negative premium and claim values as NULL
  * Removed invalid rows using `dropna()`
* Trimmed and standardized string columns (e.g., city names)
* Ensured correct data types for all columns

---

## Phase 3: Data Validation

* Used **left_anti joins** to detect invalid records:

  * Claims with no matching policy
* Removed invalid records to ensure referential integrity
* Compared row counts before and after validation
* Created a validation approach to ensure only consistent data flows into transformations

---

## Phase 4: Data Transformation

* Avoided direct joins to prevent duplication issues
* Aggregated data before joining:

  * Total premium per customer
  * Total claim per customer
* Joined aggregated datasets safely
* Computed:

  * **Risk Score = total_claim / total_premium**
* Generated city-wise premium and claim distribution

---

## Phase 5: Advanced SQL (CTE)**

* Created temporary views from DataFrames
* Used **Common Table Expressions (CTE)** to structure queries
* Broke logic into steps:

  * Aggregate premium
  * Aggregate claims
  * Compute risk score
* Derived insights such as:

  * Risk per customer
  * Customers with increasing claims

---

## Phase 6: Window Functions

* Applied window functions for ranking:

  * Ranked customers based on risk score (partitioned by city)
  * Ranked agents based on total premium handled
* Used functions like:

  * `ROW_NUMBER()`
  * `RANK()`
  * `DENSE_RANK()`

---

## Phase 7: Final Output

* Generated final datasets:

  * Customer risk metrics
  * City-wise distribution
  * Agent performance ranking

* Saved outputs to files using:

  ```python
  df.write.mode("overwrite").csv("output_path")
  ```

---

## Key Insights

* Some customers have significantly higher claim-to-premium ratios, indicating high risk
* Certain cities show higher overall claim ratios, suggesting regional risk patterns
* A small number of agents contribute to a large portion of premium collection
* Data quality issues (negative values, invalid keys) can significantly affect analysis if not handled properly

---

## Conclusion

This project demonstrates a structured data engineering pipeline involving data cleaning, validation, transformation, and analysis. Emphasis was placed on avoiding common pitfalls such as duplication during joins and ensuring data quality before deriving insights.

---
