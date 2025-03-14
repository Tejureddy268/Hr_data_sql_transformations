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




These are great PySpark interview questions! Let me give you detailed answers for each:

### **1️⃣ How does PySpark handle data partitioning, and why is it important for performance optimization?**
   - **Data partitioning** in PySpark helps distribute large datasets across multiple nodes in a cluster. It divides data into smaller chunks (partitions) that can be processed in parallel.
   - **Default Partitioning:** PySpark automatically partitions data based on the number of cores available.
   - **Custom Partitioning:** You can explicitly define partitioning using `repartition()` (reshuffles data) and `coalesce()` (reduces the number of partitions efficiently).
   - **Importance for performance optimization:**
     - Reduces data shuffling across nodes.
     - Improves parallelism and resource utilization.
     - Optimizes execution plans by minimizing data movement.

---

### **2️⃣ Explain the concept of lineage graphs in PySpark. How do they contribute to fault tolerance?**
   - A **lineage graph** (Directed Acyclic Graph - DAG) represents the sequence of transformations applied to an RDD.
   - It **tracks the dependencies** between transformations and source data.
   - **Fault Tolerance:** If a node crashes, Spark can **recompute lost partitions** by replaying transformations from the lineage graph instead of restarting the job.

---

### **3️⃣ What are the different types of joins in PySpark, and how do they differ from traditional SQL joins?**
   PySpark supports several types of joins, similar to SQL:
   - **Inner Join (`df1.join(df2, "key", "inner")`)** – Returns only matching records.
   - **Left Outer Join (`left`)** – Keeps all records from the left DataFrame.
   - **Right Outer Join (`right`)** – Keeps all records from the right DataFrame.
   - **Full Outer Join (`outer`)** – Combines both DataFrames, filling missing values with `NULL`.
   - **Cross Join (`cross`)** – Returns the Cartesian product.
   - **Semi Join (`left_semi`)** – Returns only the rows from the left DataFrame that have a match in the right DataFrame.
   - **Anti Join (`left_anti`)** – Returns only the rows from the left DataFrame that do **not** have a match in the right DataFrame.

   **Difference from SQL:** PySpark **optimizes joins** using broadcast joins (`broadcast(df2)`) when one dataset is small, reducing shuffle overhead.

---

### **4️⃣ How can you integrate PySpark with other big data tools like Hadoop or Hive?**
   - **With Hadoop:** PySpark can read/write data from HDFS (`hdfs://path/to/data`).
   - **With Hive:** Use `enableHiveSupport()` in SparkSession to connect with Hive Metastore:
     ```python
     from pyspark.sql import SparkSession
     spark = SparkSession.builder.appName("HiveIntegration").enableHiveSupport().getOrCreate()
     df = spark.sql("SELECT * FROM hive_table")
     ```
   - **With Kafka:** Stream real-time data using Spark Structured Streaming.

---

### **5️⃣ Describe the role of the Catalyst optimizer in PySpark's SQL engine. How does it improve query performance?**
   - **Catalyst optimizer** is Spark SQL’s query optimizer that improves execution plans dynamically.
   - It applies **logical optimizations** (constant folding, predicate pushdown) and **physical optimizations** (reordering joins, selecting efficient join strategies).
   - Improves query performance by:
     - Reducing unnecessary computations.
     - Optimizing execution plans.
     - Choosing efficient data access strategies.

---

### **6️⃣ What is the purpose of the 'groupByKey' and 'reduceByKey' transformations in PySpark? When would you use one over the other?**
   - **`groupByKey()`**: Groups data by key **without** applying an aggregation. It **shuffles all data**, leading to performance overhead.
   - **`reduceByKey()`**: Performs **aggregation during shuffling**, reducing data transfer across nodes.

   **When to use which?**
   - Use **`reduceByKey()`** for aggregations (sum, count, etc.), as it minimizes data movement.
   - Use **`groupByKey()`** only when you need to access all values for a key.

---

### **7️⃣ How do you handle skewed data in PySpark to ensure efficient processing?**
   - **Salting keys:** Add random prefixes to keys to distribute data more evenly.
   - **Repartitioning:** Use `repartition()` to redistribute data before heavy computations.
   - **Broadcast join:** Use `broadcast()` for small lookup tables to avoid shuffling.
   - **Skew join optimization:** Use `skewHint()` in Spark 3.0+ to optimize joins.

---

### **8️⃣ Can you explain the concept of 'checkpointing' in PySpark Streaming? Why is it necessary?**
   - **Checkpointing** saves metadata and intermediate results to prevent data loss in streaming applications.
   - It helps with:
     - **Fault tolerance:** Recovers from node failures.
     - **Stateful processing:** Required for streaming state management.
   - Enable it using:
     ```python
     df.writeStream.format("console").option("checkpointLocation", "hdfs://path/to/checkpoint").start()
     ```

---

### **9️⃣ What are window functions in PySpark, and how are they used in data processing?**
   - **Window functions** perform calculations over a group of rows **without collapsing them into one**.
   - Used for **running totals, ranking, moving averages, etc.**
   - Example:
     ```python
     from pyspark.sql.window import Window
     from pyspark.sql.functions import row_number

     windowSpec = Window.partitionBy("department").orderBy("salary")
     df.withColumn("rank", row_number().over(windowSpec)).show()
     ```

---

### **1️⃣ 0️⃣ How do you monitor and debug PySpark applications in a production environment?**
   - **Spark UI:** Check DAG, job stages, and task execution.
   - **Logging:** Enable logging in `log4j.properties`.
   - **Check executor logs:** Use `yarn logs -applicationId <app_id>` for cluster logs.
   - **Use Metrics:** Enable Ganglia or Prometheus for monitoring.

---

These answers should help in your Cognizant Data Engineer interview! Let me know if you want further clarifications or more examples. 🚀