# Day 2 – SQL Practice (GROUP BY & JOINS)

## Objective
To strengthen SQL skills through hands-on practice by working with GROUP BY operations and different types of JOINS using real-world datasets.

---

## Dataset Used

### Employee Table
- emp_id  
- emp_name  
- department  
- salary  
- joining_date  

### Sales Table
- sales_id  
- emp_id  
- product  
- amount  
- sale_date  

### Additional Tables (Join Practice)
- Employees (with manager_id, dept_id)  
- Departments (dept_id, dept_name)  
- Projects (project_id, project_name, emp_id)  

---

## Topics Covered

### GROUP BY

#### Basic Aggregations
- Total salary by department  
- Employee count per department  
- Average, maximum, and minimum salary  

#### GROUP BY with Conditions
- Applied conditions on aggregated results  
- Filtered departments based on salary thresholds  

#### GROUP BY with HAVING
- Used HAVING to filter grouped results  
- Worked with conditions like:
  - departments with more employees  
  - salary range filtering  

#### Advanced GROUP BY
- Total salary and total sales per employee  
- Unique product count per employee  
- Highest sales amount per employee  
- Product-wise sales analysis  

---

### JOINS

#### INNER JOIN
- Retrieved matching records between tables  
- Combined employee and department data  

#### LEFT JOIN
- Included all employees  
- Handled missing department values using NULL  

#### RIGHT JOIN
- Included all departments  
- Displayed departments without employees  

#### FULL OUTER JOIN
- Combined all records from both tables  
- Included unmatched data from both sides  

---

## Practice Work

- Solved 30 GROUP BY problems  
- Practiced multiple JOIN scenarios  
- Combined Employee and Sales data for analysis  
- Worked on employee-manager relationships  
- Handled NULL values and missing data  
- Implemented real-world SQL use cases  

---

## Key Concepts Learned

- GROUP BY for aggregation  
- HAVING vs WHERE clause  
- Different types of JOINS  
- Combining GROUP BY with JOINS  
- Handling relational data effectively  

---

## Skills Gained

- Writing efficient SQL queries  
- Strong understanding of data relationships  
- Ability to perform data analysis using SQL  
- Solving complex query-based problems  

---

## Outcome

- Improved SQL proficiency  
- Gained hands-on experience with real datasets  
- Built confidence in solving real-world SQL problems  