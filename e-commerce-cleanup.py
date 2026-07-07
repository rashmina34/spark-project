# """
# E-Commerce Orders Data Cleaning Pipeline
# """

# import logging
# import os
# import shutil

# from pyspark.sql import SparkSession, Window, DataFrame
# import pyspark.sql.functions as F
# from pyspark.sql.types import IntegerType, DecimalType

# logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
# logger = logging.getLogger("ecommerce_pipeline")

# # ---------------------------------------------------------------------------
# # Config (pull from env / config file in real prod, not hardcoded)
# # ---------------------------------------------------------------------------
# INPUT_PATH = os.environ.get("ORDERS_INPUT_PATH", "./data/orders.csv")
# OUTPUT_DIR = os.environ.get("ORDERS_OUTPUT_DIR", "./data/output")
# QUARANTINE_DIR = os.environ.get("ORDERS_QUARANTINE_DIR", "./data/quarantine")
# OUTPUT_FORMAT = os.environ.get("ORDERS_OUTPUT_FORMAT", "parquet")  # "parquet" or "csv"

# MAX_DAYS_FUTURE = 180
# DEFAULT_QUANTITY = 1
# MIN_VALID_QTY = 1
# MAX_VALID_QTY = 100
# HARD_OUTLIER_QTY = 1000
# MAX_DISCOUNT_PCT = 100

# NULL_TOKENS = ("NULL", "null", "NaN", "N/A", "")

# WORD_TO_NUM = {
#     "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4,
#     "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
# }

# ORDER_SHIPPED_STATUSES = ("Shipped", "Delivered", "Out for Delivery")


# def get_spark() -> SparkSession:
#     return (
#         SparkSession.builder
#         .appName("ECommerceOrdersCleaning")
#         .master("local[*]")
#         .getOrCreate()
#     )


# def load_orders(spark: SparkSession, path: str) -> DataFrame:
#     # Explicit schema is preferable in production over inferSchema=True,
#     # since inference is a full extra data pass and is unstable on mixed-type
#     # columns like Quantity ("1" vs "one" vs null). Kept as inferSchema here
#     # only because Quantity/Discount are cleaned as strings downstream anyway.
#     logger.info("Reading orders from %s", path)
#     return spark.read.format("csv").option("header", True).option("inferSchema", True).load(path)


# def dedupe_orders(df: DataFrame) -> DataFrame:
#     before = df.count()
#     df = df.dropDuplicates()  # dropDuplicates() with no subset already dedupes on all columns;
#     # a following .distinct() would be redundant, so it's removed.
#     after = df.count()
#     logger.info("Dropped %d exact duplicate rows", before - after)
#     return df


# def flag_duplicate_order_ids(df: DataFrame) -> DataFrame:
#     window_spec = Window.partitionBy("OrderID")
#     return (
#         df.withColumn("OrderID_Count", F.count("OrderID").over(window_spec))
#           .withColumn("Status", F.when(F.col("OrderID_Count") > 1, "FAIL").otherwise("PASS"))
#           .drop("OrderID_Count")
#     )


# def clean_customer_name(df: DataFrame) -> DataFrame:
#     return df.withColumn("CustomerName", F.trim(F.col("CustomerName")))


# def clean_email(df: DataFrame) -> DataFrame:
#     return df.withColumn(
#         "Email",
#         F.when(F.trim(F.col("Email")).isin(*NULL_TOKENS), F.lit(None)).otherwise(F.trim(F.col("Email"))),
#     )


# def standardize_dates(df: DataFrame) -> DataFrame:
#     df = df.withColumn(
#         "OrderDate",
#         F.date_format(
#             F.coalesce(
#                 F.try_to_date(F.col("OrderDate"), "yyyy-MM-dd"),
#                 F.try_to_date(F.col("OrderDate"), "MM/dd/yyyy"),
#                 F.try_to_date(F.col("OrderDate"), "yyyy/MM/dd"),
#             ),
#             "yyyy-MM-dd",
#         ),
#     ).withColumn(
#         "ShipDate",
#         F.date_format(
#             F.coalesce(
#                 F.try_to_date(F.col("ShipDate"), "yyyy-MM-dd"),
#                 F.try_to_date(F.col("ShipDate"), "MM/dd/yyyy"),
#                 F.try_to_date(F.col("ShipDate"), "dd-MM-yyyy"),
#                 F.try_to_date(F.col("ShipDate"), "yyyy/MM/dd"),
#                 F.try_to_date(F.col("ShipDate"), "d-M-yyyy"),
#             ),
#             "yyyy-MM-dd",
#         ),
#     )
#     return df.withColumn(
#         "ShipDate",
#         F.when(F.trim(F.col("ShipDate")).isin(*NULL_TOKENS), F.lit(None)).otherwise(F.trim(F.col("ShipDate"))),
#     )


# def validate_shipping_dates(df: DataFrame) -> DataFrame:
#     return df.withColumn(
#         "Shipping_Status",
#         F.when(F.col("ShipDate") > F.date_add(F.current_date(), MAX_DAYS_FUTURE), "FAIL_DATE_TOO_FAR_FUTURE")
#          .when(F.col("ShipDate") < F.col("OrderDate"), "FAIL_SHIPPED_BEFORE_ORDER")
#          .otherwise("PASS"),
#     )


# def clean_quantity(df: DataFrame) -> DataFrame:
#     mapping_expr = F.create_map([F.lit(x) for pair in WORD_TO_NUM.items() for x in pair])
#     raw_qty = F.trim(F.lower(F.col("Quantity").cast("string")))
#     null_like = raw_qty.isNull() | raw_qty.isin("", "null", "nan")
#     word_mapped = mapping_expr.getItem(raw_qty)
#     numeric_cast = raw_qty.cast(IntegerType())

#     cleaned_qty = (
#         F.when(null_like, F.lit(DEFAULT_QUANTITY))
#          .when(word_mapped.isNotNull(), word_mapped)
#          .when(numeric_cast.isNotNull(), numeric_cast)
#          .otherwise(F.lit(DEFAULT_QUANTITY))
#     )

#     df = df.withColumn("Quantity", cleaned_qty.cast(IntegerType()))
#     return df.withColumn(
#         "Quantity",
#         F.when(F.col("Quantity") <= 0, F.lit(DEFAULT_QUANTITY)).otherwise(F.col("Quantity")),
#     )


# def flag_quantity_outliers(df: DataFrame) -> DataFrame:
#     return df.withColumn(
#         "quantity_outlier_flag",
#         F.when(F.col("Quantity") > HARD_OUTLIER_QTY, F.lit("EXTREME_OUTLIER"))
#          .when(F.col("Quantity") > MAX_VALID_QTY, F.lit("HIGH_OUTLIER"))
#          .otherwise(F.lit("NORMAL")),
#     )


# def clean_unit_price(df: DataFrame) -> DataFrame:
#     stripped = F.regexp_replace(F.col("UnitPrice").cast("string"), r"\$", "")
#     df = df.withColumn(
#         "UnitPrice",
#         F.when(stripped.isin(*NULL_TOKENS), F.lit(None)).otherwise(stripped).cast(DecimalType(10, 2)),
#     )
#     return df.withColumn(
#         "UnitPrice",
#         F.when(F.col("UnitPrice") < 0, F.lit(0)).otherwise(F.col("UnitPrice")),
#     )


# def clean_discount(df: DataFrame) -> DataFrame:
#     df = df.withColumn(
#         "Discount",
#         F.when(F.col("Discount").cast("string").isin(*NULL_TOKENS), F.lit(0))
#          .otherwise(F.col("Discount"))
#          .cast(DecimalType(5, 2)),
#     )
#     # Negative discounts are just as invalid as >100%; both are clamped.
#     return df.withColumn(
#         "Discount",
#         F.when(F.col("Discount") > MAX_DISCOUNT_PCT, F.lit(MAX_DISCOUNT_PCT))
#          .when(F.col("Discount") < 0, F.lit(0))
#          .otherwise(F.col("Discount")),
#     )


# def flag_missing_shipping_fields(df: DataFrame) -> DataFrame:
#     df = df.withColumn("ShippingAddress", F.trim(F.col("ShippingAddress")))
#     is_shipped = F.col("OrderStatus").isin(*ORDER_SHIPPED_STATUSES)

#     df = df.withColumn(
#         "address_missing_flag",
#         is_shipped & (F.col("ShippingAddress").isNull() | (F.col("ShippingAddress") == "")),
#     )
#     return df.withColumn(
#         "PostalCodeFlag",
#         is_shipped
#         & (
#             F.col("PostalCode").isNull()
#             | (F.trim(F.col("PostalCode")) == "")
#             | F.trim(F.col("PostalCode")).isin("NULL", "null")
#         ),
#     )


# def default_order_status(df: DataFrame) -> DataFrame:
#     return df.withColumn(
#         "OrderStatus",
#         F.when(F.col("OrderStatus").isin("NULL", "null", ""), F.lit("FAIL")).otherwise(F.col("OrderStatus")),
#     )


# def write_single_file(df: DataFrame, output_dir: str, fmt: str = "parquet") -> None:
#     """Write DataFrame as a single part-file, renamed to a clean fixed filename."""
#     if os.path.exists(output_dir):
#         shutil.rmtree(output_dir)

#     writer = df.coalesce(1).write.mode("overwrite")
#     if fmt == "csv":
#         writer.option("header", "true").csv(output_dir)
#         suffix = ".csv"
#     else:
#         writer.parquet(output_dir)
#         suffix = ".parquet"

#     part_files = [f for f in os.listdir(output_dir) if f.startswith("part-") and f.endswith(suffix)]
#     if not part_files:
#         raise FileNotFoundError(f"No part file with suffix {suffix} found in {output_dir}")

#     final_path = output_dir.rstrip("/") + suffix
#     os.rename(os.path.join(output_dir, part_files[0]), final_path)
#     shutil.rmtree(output_dir)
#     logger.info("Wrote single %s file to %s", fmt, final_path)


# def run_pipeline() -> None:
#     spark = get_spark()
#     try:
#         df = load_orders(spark, INPUT_PATH)
#         df = dedupe_orders(df)
#         df = flag_duplicate_order_ids(df)
#         df = clean_customer_name(df)
#         df = clean_email(df)
#         df = standardize_dates(df)
#         df = validate_shipping_dates(df)
#         df = clean_quantity(df)
#         df = flag_quantity_outliers(df)
#         df = clean_unit_price(df)
#         df = clean_discount(df)
#         df = flag_missing_shipping_fields(df)
#         df = default_order_status(df)

#         df.cache()
#         logger.info("Final row count: %d", df.count())

#         # Split clean vs. quarantined records instead of shipping everything downstream
#         quarantine_mask = (
#             (F.col("Status") == "FAIL")
#             | (F.col("Shipping_Status") != "PASS")
#             | F.col("address_missing_flag")
#             | F.col("PostalCodeFlag")
#             | (F.col("quantity_outlier_flag") == "EXTREME_OUTLIER")
#         )
#         df_quarantine = df.filter(quarantine_mask)
#         df_final = df.filter(~quarantine_mask)

#         if df_quarantine.count() > 0:
#             write_single_file(df_quarantine, QUARANTINE_DIR, fmt=OUTPUT_FORMAT)
#             logger.warning("%d records quarantined for manual review", df_quarantine.count())

#         write_single_file(df_final, OUTPUT_DIR, fmt=OUTPUT_FORMAT)

#     finally:
#         spark.stop()  # always stop, even on failure


# if __name__ == "__main__":
#     run_pipeline()

import os

from pyspark.sql import SparkSession, Window, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DecimalType, StringType

INPUT_PATH = os.environ.get("ORDERS_INPUT_PATH", "./data/orders.csv")
OUTPUT_DIR = os.environ.get("ORDERS_OUTPUT_DIR", "./data/output")

NULL_TOKENS = ["NULL", "null", "NaN", "N/A", ""]

spark = SparkSession(
        SparkSession.builder
        .appName("ECommerceOrdersCleaning")
        .master("local[*]")
        .getOrCreate()
    )

df = spark.read.option("header", True).option("inferSchema", True).csv(INPUT_PATH)

df = df.dropDuplicates()

df = (
    df.withColumn("ShippingAddress", F.trim(F.col("ShippingAddress")))
    .withColumn("CustomerName", F.trim(F.col("CustomerName")))
    .withColumn("City", F.trim(F.col("City")))
    .withColumn("Country", F.trim(F.col("Country")))
)

for c in df.columns:
    if isinstance(df.schema[c].dataType, StringType):
        df = df.withColumn(
            c,
            F.when(F.col(c).isin(NULL_TOKENS), None)
            .otherwise(F.col(c))
        )
    
df = df.withColumn(
    "OrderDate",
    F.date_format(
    F.coalesce(
        F.try_to_date("OrderDate","yyyy-MM-dd"),
        F.try_to_date("OrderDate","MM/dd/yyyy"),
        F.try_to_date("OrderDate","yyyy/MM/dd"),
    #    F.to_date(F.from_unixtime("OrderDate"))
    ),
    "yyyy-MM-dd"
    ))

df = df.withColumn(
    "UnitPrice",
    F.regexp_replace("UnitPrice","[$,]","")
)

df = df.withColumn(
    "UnitPrice",
    F.col("UnitPrice").cast("double")
)

df = df.withColumn(
    "Quantity",
    F.when(F.col("Quantity")=="one",1)
    .otherwise(F.col("Quantity"))
    .cast("int")
)

valid_discount = df.filter(
    (F.col("Discount") >= 0) &
    (F.col("Discount") <= 100)
)

invalid_discount = df.filter(
    (F.col("Discount") > 100) |
    (F.col("Discount") < 0)
)

good_df = df.filter(F.col("UnitPrice") >= 0)

bad_df = df.filter(F.col("UnitPrice") < 0)

good_df = good_df.filter(F.col("Email").isNotNull())
bad_df = bad_df.filter(F.col("Email").isNull())

duplicates = (
    df.groupBy("OrderID")
      .agg(
          F.countDistinct("ShippingAddress")
          .alias("AddressCount")
      )
      .filter(F.col("AddressCount") > 1)
)
error_df = df.join(
    duplicates,
    "OrderID"
)

future_dates = df.filter(
    F.col("ShipDate") > F.current_date()
)

invalid_qty = df.filter(
    F.col("Quantity") > 100
)
    
df.show()
