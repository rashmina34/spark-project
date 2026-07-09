from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, count

spark = SparkSession.builder.getOrCreate()

data = [
    ("Electronics", 1200, 2),
    ("Electronics", 800, 1),
    ("Furniture", 500, 4),
    ("Furniture", 700, 2),
    ("Clothing", 100, 5)
]

df = spark.createDataFrame(data, ["Category", "Sales", "Quantity"])
# df.show()

df.agg(
    sum("Sales").alias("TotalSales"),
    avg("Sales").alias("AverageSales"),
    min("Sales").alias("MinSales"),
    max("Sales").alias("MaxSales"),
    count("*").alias("TotalRows")
).show()

df.groupBy("Category") \
  .agg(sum("Sales").alias("TotalSales")) \
  .show()
  
df.groupBy("Category").agg(
    sum("Sales").alias("TotalSales"),
    avg("Sales").alias("AverageSales"),
    max("Sales").alias("HighestSale"),
    min("Sales").alias("LowestSale")
).show()