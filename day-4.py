from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)

# df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("./employees.csv")

# _schema = "employee_id int, departmenr_id int, name string, age int, gender string, salary double, hire_date date"
# df_schema = spark.read.format("csv").option("header", True).schema(_schema).load("./employees.csv")
# df_schema.show()

#default Mode permissive
# _schema = "employee_id int, departmenr_id int, name string, age int, gender string, salary double, hire_date date, bad_record string"
# df_p = spark.read.format("csv").schema(_schema).option("columnNameOfCorruptRecord", "bad_record").option("header", True).load("./new-employees.csv")
# df_p.where("bad_record is not null").show()

# _schema = "employee_id int, departmenr_id int, name string, age int, gender string, salary double, hire_date date"
# df_m = spark.read.format("csv").option("header", True).option("mode", "DROPMALFORMED").schema(_schema).load("./new-employees.csv")
# df_m.show()

# _schema = "employee_id int, departmenr_id int, name string, age int, gender string, salary double, hire_date date"
# df_f = spark.read.format("csv").option("header", True).option("mode", "FAILFAST").schema(_schema).load("./new-employees.csv") #failed if schema doesn't match data
# df_f.show()


_options = {
    "header": "true",
    "inferSchema" : "true",
    "mode": "PERMISSIVE"
}
df = spark.read.format("csv").options(**_options).load("./employees.csv")
df.show()