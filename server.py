import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

spark = SparkSession.builder \
    .appName("Tutorial") \
    .getOrCreate()

data = [
    (1, "John", 5000, "Research"),
    (2, "Jane", 7000, "Development"),
    (3, "Mike", 6000, "Development")
]


df = spark.createDataFrame(
    data,
    ["id", "name", "salary", "department"]
)

df.show()

# dfs = spark.read.csv(
#     "./employees.csv",
#     header=True,
#     inferSchema=True
# )

# dfs.show()
# dfs.printSchema()

df.select("name", "salary").show()
df.filter(df.salary > 6000).show()
df = df.limit(3).show()

df = df.withColumn(
    "bonus",
    col("salary") * 0.10
)

df = df.withColumn(
    "department",
    col("salary") * 0.10
)

df = df.withColumnRenamed(
    "salary",
    "monthly_salary"
)

df = df.drop("bonus")
df.show()

df.groupBy("department") \
  .agg(
      count("*").alias("employee_count"),
      avg("monthly_salary").alias("avg_salary")
  ) \
  .show()