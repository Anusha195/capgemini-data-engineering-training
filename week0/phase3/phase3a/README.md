## Phase 3A – Data Cleaning

## Objective  
In this phase the goal is to clean messy data using PySpark and understand its importance before analysis.

## Problem Summary  
The dataset contained null values duplicate records and invalid data.  
The task was to identify these issues clean the data and generate simple insights.

## Approach  
Created DataFrame with given data  
Identified null values duplicates and wrong data  
Removed null values using dropna  
Handled missing values using fillna  
Removed duplicate records using dropDuplicates  
Filtered invalid data using filter  
Validated data before and after cleaning  
Performed aggregation using groupBy and count  

## Key Methods Used  
dropna to remove null values  
fillna to handle missing data  
dropDuplicates to remove duplicates  
filter to remove invalid data  
groupBy and count for aggregation  

## Output Results  
Cleaned dataset  
Customer count per city  
Outputs are available in the outputs folder  

## Data Engineering Considerations  
Handled missing and invalid data before analysis  
Ensured no duplicate records in final data  
Validated results after cleaning  

## Challenges Faced  
Identifying which data to remove  
Handling missing values correctly  

## Learnings  
Real data is messy  
Cleaning is necessary  
Bad data gives wrong results  

## Reflection  
Cleaning data is important for correct analysis and better decisions  

## Files in this Folder  
pyspark_codes.py contains code, 
pyspark_phase3a_problem_statement contains details of this phase like tasks to be done, 
outputs folder contains results  
