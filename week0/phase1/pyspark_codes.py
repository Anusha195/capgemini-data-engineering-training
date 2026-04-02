#Show all customers 
customers.show()

#Show customers from Chennai
customers.filter(customers.city == "Chennai").show()

#Show customers with age > 25 
customers.filter(customers.age > 25).show() 

#Show only customer_name and city
customers.select("customer_name", "city").show()

#Count customers city-wise 
customers.groupBy("city").agg(count("*").alias("total_customers")).show()