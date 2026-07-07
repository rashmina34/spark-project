from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, avg, count, when, expr, to_date, current_date, current_timestamp, coalesce, lit


spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)
    
employee_data = [
    ["001", "101", "John Doe", "30", "Male", "50000", "2015-01-01"],
    ["002", "102", "Jane Smith", "25", "Female", "55000", "2016-01-01"],
    ["003", "103", "Bob Brown", "26", "Female", "40000", "2016-01-01"],
    ["004", "104", "Alice Lee", "40", "Male", "50000", "2017-01-01"],
    ["005", "105", "Kate Kate", "45", "Male", "60000", "2019-01-01"],
    ["006", "106", "Nancy Lee", "29", "Female", "65000", "2014-01-01"],
    ["007", "107", "David Perk", "28", "Male", "70000", "2013-01-01"],
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

# emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

emp_df = spark.createDataFrame(data=employee_data, schema=emp_schema)

# emp_df.rdd.getNumPartitions()

# print("Partitions:", emp_df.rdd.getNumPartitions())

# emp_df.show()

# emp_salary_df = emp_df.where("salary > 50000")
# emp_salary_df.write.format("csv").save("/Users/rashrestha/Documents/sparkProject/emp.csv")

# emp_salary_df.show()

emp_gender_fixed = emp_df.withColumn("new_gender", when(col("gender") == "Male", "M")
                                     .when(col("gender") == "Female", "F")
                                     .otherwise(None))

# emp_gender_fixed_1 = emp_df.withColumn("new_gender", expr("case when gender = 'Male' then 'M' when gender ='Female' then 'F' else null end"))

# emp_gender_fixed_1.show()

# emp_date_fix = emp_df.withColumn("hire_date", to_date(col("hire_date"),"yyy-MM-dd"))
# emp_date_fix.printSchema()

emp_date = emp_df.withColumn("date_now", current_date()).withColumn("timestamp_now", current_timestamp())
emp_date.show(truncate=False)

emp_null = emp_gender_fixed.withColumn("new_gender", coalesce(col("new_gender"), lit("O")))

emp_final = emp_null.drop('gender').withColumnRenamed("new_gender", "gender")
emp_final.show()
