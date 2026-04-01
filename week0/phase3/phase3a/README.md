# Phase 3A – Data Quality & Cleaning

## Overview
In this phase, I worked with a messy dataset and applied data cleaning techniques using PySpark. The objective was to understand the importance of cleaning data before performing any analysis.

## What I Did
- Created a DataFrame with messy data
- Identified issues like null values, duplicates, and invalid data
- Cleaned the dataset step by step
- Validated data before and after cleaning
- Performed aggregation to get insights

## Data Issues Identified
- Null values in customer_id, name, and city
- Duplicate records
- Invalid age values (negative age)

## Cleaning Steps
- Removed rows where customer_id is null
- Replaced missing name and city with "Unknown"
- Removed duplicate rows using dropDuplicates()
- Filtered invalid age values using filter()

## Validation
- Checked row count before cleaning
- Checked row count after cleaning

## Aggregation
- Calculated number of customers per city using groupBy() and count()

## Project Structure
phase3/
├── pyspark_codes.py
├── outputs/
└── README.md

## Key Learnings
- Real-world data is often messy
- Cleaning is important before processing
- Invalid data can lead to wrong results
- Validation ensures accuracy

## Reflection
- Skipping cleaning can give incorrect results
- Data quality affects business decisions
- A proper cleaning process is necessary