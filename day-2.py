from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, sum, count, when, expr, to_date, current_date, current_timestamp, coalesce, lit, desc, asc, max

spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)
   
   
emp_data_1 = [
    ["001", "101", "John Doe", "30", "Male", "50000", "2015-01-01"],
    ["002", "102", "Jane Smith", "25", "Female", "55000", "2016-01-01"],
    ["003", "103", "Bob Brown", "26", "Female", "40000", "2016-01-01"],
    ["004", "104", "Alice Lee", "40", "Male", "50000", "2017-01-01"],
    ["005", "105", "Kate Kate", "45", "Male", "60000", "2019-01-01"],
    ["006", "106", "Nancy Lee", "29", "Female", "65000", "2014-01-01"],
    ["007", "107", "David Perk", "28", "Male", "70000", "2013-01-01"],
]

emp_data_2 = [
    ["008", "108", "Michel Lee", "27", "Male", "25000", "2025-01-01"],
    ["009", "109", "Gorge Wang", "26", "Female", "80000", "2013-01-01"],
    ["010", "110", "Ram Stha", "31", "Female", "50000", "2015-01-01"],
    ["011", "111", "Jana Hub", "30", "Female", "53000", "2017-01-01"],
    ["012", "112", "Sita Lee", "33", "Male", "50000", "2019-01-01"],
    ["013", "113", "Sony Nee", "34", "male", "48000", "2018-01-01"],
    ["014", "114", "Shyam Lee", "35", "Female", "35000", "2015-11-01"],
]

emp_schema = StructType([
    StructField("employee_id", StringType(), True),
    StructField("department_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True),
    StructField("hire_date", StringType(), True),
])

emp_data_1_df = spark.createDataFrame(data=emp_data_1, schema=emp_schema)
emp_data_2_df = spark.createDataFrame(data=emp_data_2, schema=emp_schema)

# emp_data_1_df.show()
# emp_data_2_df.show()

# emp = emp_data_1_df.union(emp_data_1_df)
emp = emp_data_1_df.unionAll(emp_data_2_df)
# emp.show()

# emp_sorted = emp.orderBy(col("salary").desc())
# emp_sorted = emp.orderBy(col("salary").asc())
# emp_sorted.show()

# emp_count = emp_sorted.groupBy("department_id").agg(count("employee_id").alias("total_dept_count"))
# emp_count.show()

# emp_sum = emp_sorted.groupBy('department_id').agg(sum("salary").alias("total_dept_salary"))
# emp_sum.show()

# emp_unique = emp.distinct()
emp_dept_unique = emp.select("department_id").distinct()
# emp_dept_unique.show()

window_spec = Window.partitionBy(col("department_id")).orderBy(col("salary").desc())
max_func = max(col("salary")).over(window_spec)
emp_1 = emp.withColumn("max_salary", max_func)
emp_1.show()