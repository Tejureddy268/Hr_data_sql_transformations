# Databricks_Transformations
multiple domains of data



https://github.com/Tejureddy268/Databricks_Transformations.git


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder.appName("PySpark Transformations").getOrCreate()

# Sample Employee Salary Data
employee_data = [
    (1, "Alice", 50000),
    (2, "Bob", 60000),
    (3, "Charlie", 70000),
    (4, "David", 60000),
    (5, "Eve", 90000),
]
employee_columns = ["employee_id", "name", "salary"]
df_employees = spark.createDataFrame(employee_data, employee_columns)

# Sample Sales Data
sales_data = [
    (1, "Laptop", 1000, "North"),
    (2, "Phone", 800, "East"),
    (3, "Tablet", 400, "West"),
    (4, "Monitor", 600, "South"),
    (5, "Laptop", 1200, "North"),
]
sales_columns = ["sales_id", "product", "sales", "region"]
df_sales = spark.createDataFrame(sales_data, sales_columns)

# Sample Stock Price Data
stock_data = [
    (datetime(2024, 3, 1), "AAPL", 145.3),
    (datetime(2024, 3, 2), "AAPL", 146.7),
    (datetime(2024, 3, 3), "AAPL", 144.5),
    (datetime(2024, 3, 4), "AAPL", 147.2),
    (datetime(2024, 3, 5), "AAPL", 149.0),
]
stock_columns = ["date", "ticker", "price"]
df_stocks = spark.createDataFrame(stock_data, stock_columns)

# Sample Customer Orders Data
customer_data = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Charlie", "charlie@example.com"),
]
customer_columns = ["customer_id", "name", "email"]
df_customers = spark.createDataFrame(customer_data, customer_columns)

order_data = [
    (101, 1, "Laptop", 1200),
    (102, 2, "Phone", 800),
    (103, 1, "Monitor", 600),
    (104, 3, "Tablet", 400),
]
order_columns = ["order_id", "customer_id", "product", "amount"]
df_orders = spark.createDataFrame(order_data, order_columns)

# Sample Employee Hierarchy Data
hierarchy_data = [
    (1, "Alice", None),
    (2, "Bob", 1),
    (3, "Charlie", 1),
    (4, "David", 2),
    (5, "Eve", 3),
]
hierarchy_columns = ["employee_id", "employee_name", "manager_id"]
df_hierarchy = spark.createDataFrame(hierarchy_data, hierarchy_columns)

# Sample JSON Data
json_data = [
    (1, '{"name": "Alice", "age": "30"}'),
    (2, '{"name": "Bob", "age": "35"}'),
]
json_columns = ["id", "json_column"]
df_json = spark.createDataFrame(json_data, json_columns)

# Sample CSV Read (Assume file exists in the specified path)
df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv("path_to_csv")

# Display sample data
df_employees.show()
df_sales.show()
df_stocks.show()
df_customers.show()
df_orders.show()
df_hierarchy.show()
df_json.show()
