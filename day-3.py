from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import spark_partition_id

spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)
   

emp_data = [
    ["001", "101", "John Doe", "30", "Male", "50000", "2015-01-01"],
    ["002", "102", "Jane Smith", "25", "Female", "55000", "2016-01-01"],
    ["003", "103", "Bob Brown", "26", "Female", "40000", "2016-01-01"],
    ["004", "104", "Alice Lee", "40", "Male", "50000", "2017-01-01"],
    ["005", "105", "Kate Kate", "45", "Male", "60000", "2019-01-01"],
    ["006", "106", "Nancy Lee", "29", "Female", "65000", "2014-01-01"],
    ["007", "107", "David Perk", "28", "Male", "70000", "2013-01-01"],
    ["008", "107", "Michel Lee", "27", "Male", "25000", "2025-01-01"],
    ["009", "106", "Gorge Wang", "26", "Female", "80000", "2013-01-01"],
    ["010", "105", "Ram Stha", "31", "Female", "50000", "2015-01-01"],
    ["011", "104", "Jana Hub", "30", "Female", "53000", "2017-01-01"],
    ["012", "101", "Sita Lee", "33", "Male", "50000", "2019-01-01"],
    ["013", "102", "Sony Nee", "34", "male", "48000", "2018-01-01"],
    ["014", "101", "Shyam Lee", "35", "Female", "35000", "2015-11-01"],
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

dept_data = [
    ['101', 'Sales', 'NYC', 'US', "1000000"],
    ['102', 'Marketing', 'LA', 'US', "9000000"],
    ['103', 'Finance', 'London', 'UK', "12000000"],
    ['104', 'Engineering', 'Beijing', 'China', "1500000"],
    ['105', 'Human Resource', 'Tokyo', 'Japan', "8000000"],
    ['106', 'Research and Development', 'Perth', 'Australia', "11000000"],
    ['107', 'Customer Service', 'Sydney', 'Australia', "9500000"],
]

dept_schema = StructType([
    StructField("department_id", StringType(), True),
    StructField("department_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("budget", StringType(), True),
])

emp = spark.createDataFrame(data=emp_data, schema=emp_schema)
dept = spark.createDataFrame(data=dept_data, schema=dept_schema)

# emp.show()
# dept.show()
# emp_partition = emp.repartition(4)
# emp_partition = emp.coalesce(4) #can decrease partiton not increase reduce suffling between executers
# emp_partition = emp.repartition(4, "department_id")
# emp_1 = emp.repartition(4, "department_id").withColumn("partition_num", spark_partition_id())
# emp_1.show()

# print(emp_partition.rdd.getNumPartitions())

# df_jointed = emp.join(dept, how="inner", on=emp.department_id == dept.department_id)
# df_jointed.select(emp.name, dept.department_id, dept.department_name).show()

# df_jointed = emp.join(dept, how="left_outer", on=emp.department_id == dept.department_id)
# df_jointed.show()


df_final = emp.join(dept, how="left_outer", on=(emp.department_id == dept.department_id) & ((emp.department_id == "101") | (emp.department_id == "102")))
df_final.show()