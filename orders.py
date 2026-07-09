from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from functools import reduce

spark = (
    SparkSession
    .builder
    .appName("Commerce")
    .master("local[*]")
    .getOrCreate()
)

INPUT_PATH = "./data/orders_dirty.csv"

NULL_VALUES = ["NULL", "null", "N/A", "NaN", ""]
word_to_num = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4,
    "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10
}

df = spark.read.option("header", True).option("inferSchema", False).csv(INPUT_PATH)

df = df.dropDuplicates()
distinct_rows = df.dropDuplicates().count()

df = (
    df.withColumn("CustomerName", F.trim(F.col("CustomerName")))
    .withColumn("ShippingAddress", F.trim(F.col("ShippingAddress")))
    .withColumn("PostalCode", F.trim(F.col("PostalCode")))
    .withColumn("Email", F.trim(F.col("Email")))
)
    
for c in df.columns:
    if isinstance(df.schema[c].dataType, StringType):
        df = df.withColumn(
                c, 
                F.when(F.col(c).isin(NULL_VALUES), None)
                .otherwise(F.col(c))
            )
    else:
        df = df.withColumn(c, F.col(c))

        
df = df.withColumn(
    "OrderDate",
    F.date_format(
        F.coalesce(
            F.try_to_date("OrderDate", "yyyy-MM-dd"),
            F.try_to_date("OrderDate", "dd-MM-yyyy"),
            F.try_to_date("OrderDate", "MM/dd/yyyy"),
            F.try_to_date("OrderDate", "yyyy/MM/dd")
        ),
        "yyyy-MM-dd"
    )
)

df = df.withColumn(
    "ShipDate",
    F.date_format(
        F.coalesce(
            F.try_to_date("ShipDate", "yyyy-MM-dd"),
            F.try_to_date("ShipDate", "dd-MM-yyyy"),
            F.try_to_date("ShipDate", "MM/dd/yyyy"),
            F.try_to_date("ShipDate", "yyyy/MM/dd")
        ),
        "yyyy-MM-dd"
    )
)
    
mapping_expr = F.create_map([F.lit(x) for pair in word_to_num.items() for x in pair])

df_error_qty_name = df.filter(
    F.lower(F.trim(F.col("Quantity"))).isin("abc", "xyz", "test")
)

df = df.filter(
    ~F.lower(F.trim(F.col("Quantity"))).isin("abc", "xyz", "test")
)

df = (
    df
    .withColumn("Quantity", F.col("Quantity"))
    .withColumn("Quantity", F.lower(F.trim(F.col("Quantity"))))
    .withColumn(
        "Quantity",
        F.when(
            mapping_expr[F.col("Quantity")].isNotNull(),
            mapping_expr[F.col("Quantity")]
        ).otherwise(F.col("Quantity"))
    )
)

df_error_quantity = df.filter(F.col("Quantity") > 100)

df = df.withColumn("Quantity", F.abs(df["Quantity"]))

df = df.filter(F.col("Quantity") < 100)

df = df.withColumn(
    "UnitPrice",
    F.regexp_replace("UnitPrice","[$,]","")
)
df = df.withColumn("UnitPrice", F.abs(df["UnitPrice"]).cast("double"))
df = df.withColumn("Discount", F.when(F.col("Discount").isNull(), F.lit(0)).otherwise(F.col("Discount")))
df_error_discount = df.filter((F.col("Discount") > 100) | (F.col("Discount") < 0))

df = df.filter((F.col("Discount") >= 0) & (F.col("Discount") <= 100))

df_error_date = df.filter((F.col("ShipDate") < F.col("OrderDate")) | F.col("ShipDate").isNull())

df = df.filter((F.col("ShipDate") >= F.col("OrderDate")))
# row_count = df.count()
# print(f"Total rows: {row_count}")

df_error_far_shipping = (
    df
    .withColumn("OrderDate", F.to_date("OrderDate", "yyyy-MM-dd"))
    .withColumn("ShipDate", F.to_date("ShipDate", "yyyy-MM-dd"))
    .filter(F.datediff("ShipDate", "OrderDate") > 180)
)

df = (
    df
    .withColumn("OrderDate", F.to_date("OrderDate", "yyyy-MM-dd"))
    .withColumn("ShipDate", F.to_date("ShipDate", "yyyy-MM-dd"))
    .filter(F.datediff("ShipDate", "OrderDate") < 180)
)

df_error_email = df.filter(F.col("Email").isNull())

df = df.filter(F.col("Email").isNotNull())

df_error_Name = df.filter(F.col("CustomerName").isNull())
df = df.filter(F.col("CustomerName").isNotNull())

df_error_unit = df.filter(F.col("UnitPrice").isNull())
df = df.filter(F.col("UnitPrice").isNotNull())

dfs_error = [df_error_unit, df_error_Name, df_error_email, df_error_far_shipping, df_error_discount, df_error_quantity, df_error_date]
df_error_final = reduce(lambda df_a, df_b: df_a.union(df_b),dfs_error)
# df_error_final = df_error_unit.union(df_error_Name)
df.show()
df_error_final.show()
