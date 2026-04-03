# Phase 5 – Advanced Analytics and Segmentation using PySpark

## Objective
In this phase, the focus was on understanding customer behavior using advanced analytics techniques in PySpark.  
We explored different ways to segment customers based on their spending patterns and compared how each method works.


## Problem Summary
We were provided with datasets like customers and sales.  
The main goal was to:
- Analyze how much each customer spends
- Group customers into meaningful segments
- Apply multiple segmentation techniques
- Understand which method works best in different scenarios


## Approach

First, the datasets were loaded into PySpark DataFrames.  
Then, both datasets were joined using customer_id to bring all relevant information together.

A new column called `customer_name` was created by combining first and last names.

After that, data was aggregated at the customer level to calculate:
- Total spend of each customer
- Number of orders placed

Once the base data was ready, different segmentation techniques were applied:
- Conditional logic (Gold, Silver, Bronze)
- Quantile-based segmentation
- Bucketizer (fixed ranges)
- Window-based ranking

Finally, results from all methods were compared.


## Key Transformations Used

- `join()` → to combine customer and sales datasets  
- `groupBy()` → to group data by customer  
- `agg()` → to calculate total spend and order count  
- `withColumn()` → to create new columns  
- `when()` → to apply conditions for segmentation  
- `approxQuantile()` → to divide data into equal parts  
- `Bucketizer` → to create fixed buckets  
- `Window` + `percent_rank()` → to rank customers  


## Output / Results

The final output includes customer-level insights with segmentation applied.

Each result contains:
- customer_id  
- customer_name  
- city  
- total_spend  
- order_count  
- segment  

Different segmentation columns were created for comparison:
- quantile_seg  
- bucket_seg  
- rank_seg  

These outputs help in understanding customer distribution and behavior.


## Data Engineering Considerations

- Made sure joins were correct to avoid duplicate records  
- Performed aggregation before segmentation for accuracy  
- Kept output structure consistent across all methods  
- Used appropriate methods depending on the use case  


## Challenges Faced

- Understanding differences between segmentation methods  
- Choosing correct bucket ranges  
- Interpreting quantile values properly  
- Applying window functions correctly  


## Learnings

- Learned how real-world customer segmentation works  
- Understood the difference between fixed rules and data-driven approaches  
- Gained hands-on experience with PySpark transformations  
- Learned how ranking helps in deeper analysis  


## Files in this Folder

- `solution.py` → contains complete PySpark implementation  
- `phase5_problem_statement.pdf` → problem description  
- `outputs/` → screenshots of results  


## Final Thoughts

This phase helped in understanding how raw data can be converted into meaningful business insights.  
Different segmentation techniques provide different perspectives, and choosing the right one depends on the problem.