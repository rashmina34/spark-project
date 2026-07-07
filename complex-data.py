from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, explode

spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)

# df_parquet = spark.read.format("parquet").load("./sales_data.parquet")
# df_parquet.printSchema()
# df_parquet.show()

# df_orc = spark.read.format("orc").load("./sales_data.orc")
# df_orc.printSchema()
# df_orc.show() 

# df_single = spark.read.format("json").load("./order_singleline.json")
# df_single.printSchema()
# df_single.show()

# df_multi = spark.read.format("json").option("multiline", True).load("./order_multiline.json")
# df_multi.show()

df = spark.read.format("text").load("./order_singleline.json")
# df.show(truncate=False)

# _schema = "customer_id string, order_id string, contact array<long>"
# df_schema = spark.read.format("json").schema(_schema).load("./order_singleline.json")
# df_schema.show()

# _schema = "contact array<long>, customer_id string, order_id string, order_line_items array<struct<amount double, item_id string, qty long>>"
# df_schema_new = spark.read.format("json").schema(_schema).load("./order_singleline.json")
# df_schema_new.show()

_schema = "contact array<long>, customer_id string, order_id string, order_line_items array<struct<amount double, item_id string, qty long>>"
df_expanded = df.withColumn("parsed", from_json(df.value, _schema))
# df_expanded.show(truncate=False)

# df_unpersed = df_expanded.withColumn("unparsed", to_json(df_expanded.parsed))
# df_unpersed.show(truncate=False)


df_1 = df_expanded.select("parsed.*")
# df_1.show()

df_2 = df_1.withColumn("expanded_line_items", explode("order_line_items"))
# df_2.show()

df_3 = df_2.select("contact", "customer_id", "order_id", "expanded_line_items.*")

df_final = df_3.withColumn("contact_expanded", explode("contact"))
df_final.drop("contact").show()
