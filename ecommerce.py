from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DecimalType
import os
import shutil

spark = (
    SparkSession
    .builder
    .appName("E-Commerce")
    .master("local[*]")
    .getOrCreate()
)

MAX_DAYS_FUTURE = 180
DEFAULT_QUANTITY = 1

MIN_VALID_QTY = 1
MAX_VALID_QTY = 100        # "normal" business ceiling for a single order line
HARD_OUTLIER_QTY = 1000    # anything beyond this is almost certainly bad data / fraud / test order


word_to_num = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4,
    "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10
}

df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("./data/orders.csv")
# df.show()

df_exact_non_duplicates = df.dropDuplicates() #remove exact duplication
# df_exact_non_duplicates.show()

unique_rows_df = df_exact_non_duplicates.distinct()

# discrepancy_counts = unique_rows_df.groupBy("OrderID").count().filter(F.col("count") > 1)
# discrepancy_counts.show()

window_spec = Window.partitionBy("OrderID")
df_with_status = unique_rows_df.withColumn("OrderID_Count", F.count("OrderID").over(window_spec)).withColumn("Status", F.when(F.col("OrderID_Count") > 1, "FAIL").otherwise("PASS")).drop("OrderID_Count")
# df_with_status.orderBy("OrderId").show()

df_clean_name = df_with_status.withColumn("CustomerName", F.trim(F.col("CustomerName")))

# df_clean_name.select("OrderID", "CustomerName").show()

df_clean_email = df_clean_name.withColumn("Email", F.when(F.trim(F.col("Email")).isin("NULL", "null", ""), F.lit(None)).otherwise(F.trim(F.col("Email"))))
# df_clean_email.show()

df_standarize_date = df_clean_email.withColumn("OrderDate", F.date_format(
    F.coalesce(
        F.try_to_date(F.col("OrderDate"), "yyyy-MM-dd"),
        F.try_to_date(F.col("OrderDate"), "MM/dd/yyyy"),
        # F.try_to_date(F.col("OrderDate"), "dd-MM-yyyy"),
        F.try_to_date(F.col("OrderDate"), "yyyy/MM/dd"),
        # F.try_to_date(F.col("OrderDate"), "d-M-yyyy"),
    ),
    "yyyy-MM-dd"
)).withColumn("ShipDate", F.date_format(
    F.coalesce(
        F.try_to_date(F.col("ShipDate"), "yyyy-MM-dd"),
        F.try_to_date(F.col("ShipDate"), "MM/dd/yyyy"),
        F.try_to_date(F.col("ShipDate"), "dd-MM-yyyy"),
        F.try_to_date(F.col("ShipDate"), "yyyy/MM/dd"),
        F.try_to_date(F.col("ShipDate"), "d-M-yyyy"),
    ),
    "yyyy-MM-dd"
))

# df_standarize_date.show()

df_clean_ship_date = df_standarize_date.withColumn("ShipDate", F.when(F.trim(F.col("ShipDate")).isin("NULL", "null", "NaN", "N/A", ""), F.lit(None)).otherwise(F.trim(F.col("ShipDate"))))
# df_clean_ship_date.show()

df_shipping_validated = df_clean_ship_date.withColumn("Shipping_Status", F.when(F.col("ShipDate") > F.date_add(F.current_date(), MAX_DAYS_FUTURE), "FAIL_DATE_TOO_FAR_FUTURE").when(
    F.col("ShipDate") <  F.col("OrderDate"), "FAIL_SHIPPED_BEFORE_ORDER").otherwise("PASS")
)
# df_shipping_validated.select("OrderID", "ShipDate", "Shipping_Status").show()
# df_default_quntity=df_shipping_validated.withColumn("Quantity", F.when(F.col("Quantity").isNull(), F.lit("1")).otherwise(F.col("Quantity")))
# df_default_quntity.show()

mapping_expr = F.create_map([F.lit(x) for pair in word_to_num.items() for x in pair])
raw_qty = F.trim(F.lower(F.col("Quantity").cast("string")))
null_like = raw_qty.isNull() | (raw_qty == "") | (raw_qty == "null") | (raw_qty == "nan")
word_mapped = mapping_expr.getItem(raw_qty)
numeric_cast = raw_qty.cast(IntegerType())
cleaned_qty = (
    F.when(null_like, F.lit(DEFAULT_QUANTITY))                      # null / empty -> 1
     .when(word_mapped.isNotNull(), word_mapped)      # word number -> mapped digit
     .when(numeric_cast.isNotNull(), numeric_cast)    # numeric string -> cast
     .otherwise(F.lit(1))                             # anything unparseable -> default 1
)
df_clean = df_shipping_validated.withColumn("Quantity", cleaned_qty.cast(IntegerType()))
df_clean = df_clean.withColumn(
    "Quantity",
    F.when(F.col("Quantity") <= 0, F.lit(DEFAULT_QUANTITY)).otherwise(F.col("Quantity"))
)
# df_clean.show(truncate=False)

df_clean_qty = df_clean.withColumn(
    "quantity_outlier_flag",
    F.when(F.col("Quantity") > HARD_OUTLIER_QTY, F.lit("EXTREME_OUTLIER"))
     .when(F.col("Quantity") > MAX_VALID_QTY, F.lit("HIGH_OUTLIER"))
     .otherwise(F.lit("NORMAL"))
)
# df_clean_qty.show()

df_decimal = df_clean_qty.withColumn(
    "UnitPrice",
    F.when(F.regexp_replace(F.col("UnitPrice"), r"\$", "") == "NaN", None)
    .otherwise(F.regexp_replace(F.col("UnitPrice"), r"\$", ""))
    .cast(DecimalType(10,2))
    )
# df_decimal.show()

df_min_price = df_decimal.withColumn(
    "UnitPrice",
    F.when(F.col("UnitPrice") < 0, F.lit(0))
     .otherwise(F.col("UnitPrice"))
)
# df_min_price.show()

# df_unit_clean = df_decimal.withColumn("Discount", 
#                                       F.when(F.col("Discount").isin("NULL", "null","","NaN", "N/A"), F.lit(None))
#                                       .otherwise(F.col("Discount")))
df_discount_clean = df_min_price.withColumn("Discount", 
                                             F.when(F.col("Discount").isin("NULL", "null","","NaN", "N/A"), F.lit(0))
                                             .otherwise(F.col("Discount")))

df_discount = df_discount_clean.withColumn(
    "Discount",
    F.when(F.col("Discount") > 100, F.lit(10))
     .otherwise(F.col("Discount"))
)

df_trimed_ship_addr = df_discount.withColumn("ShippingAddress", F.trim(F.col("ShippingAddress")))

df_ship_addr = df_trimed_ship_addr.withColumn(
    "address_missing_flag",
    F.when(
        (F.col("OrderStatus").isin("Shipped", "Delivered", "Out for Delivery")) &
        (F.col("ShippingAddress").isNull() | (F.trim(F.col("ShippingAddress")) == "")),
        F.lit(True)
    ).otherwise(F.lit(False))
)

df_postal_code = df_ship_addr.withColumn(
    "PostalCodeFlag",
    F.when(
        (F.col("OrderStatus").isin("Shipped", "Delivered", "Out for Delivery")) &
        (F.col("PostalCode").isNull() | (F.trim(F.col("PostalCode")) == "") | F.col("PostalCode").isin("NULL", "null")),
        F.lit(True)
    ).otherwise(F.lit(False))
)

df_default_status = df_postal_code.withColumn("OrderStatus", F.when(F.col("OrderStatus").isin("NULL", "null", ""), F.lit("FAIL")).otherwise(F.col("OrderStatus")))
df_default_status.show()
spark.stop()

# output_dir = "./data/output"
# final_path = "./data/output/orders_clean.csv"
# final_path = "./data/output/orders_clean.parquet"


# df_default_status.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)

# part_file = [f for f in os.listdir(output_dir) if f.startswith("part-") and f.endswith(".csv")][0]

# df_default_status.coalesce(1).write.mode("overwrite").option("header", "true").parquet(output_dir)

# part_file = [f for f in os.listdir(output_dir) if f.startswith("part-") and f.endswith(".parquet")][0]
# os.rename(os.path.join(output_dir, part_file), final_path)

# shutil.rmtree(output_dir)

