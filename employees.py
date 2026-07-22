from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import re
from pyspark.sql.types import IntegerType


spark = (
    SparkSession
    .builder
    .appName("Employees")
    .master("local[*]")
    .getOrCreate()
)

INPUT_PATH = "./data/employee_dirty.csv"
name_pattern = "^[a-zA-Z]+(?:[\\s-'][a-zA-Z]+)*$"

def word_to_numeric(text):
    if not text:
        return 0
    
    text = text.lower().strip()
    
    # If it is already a digit (like "50000" or "-1200")
    if re.match(r'^-?\d+$', text):
        val = int(text)
        return max(0, val) # Apply your previous business logic: change negative to 0
        
    # Vocabulary mappings
    units = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", 
             "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"]
    tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]
    scales = {"thousand": 1000, "million": 1000000, "billion": 1000000000}
    
    num_dict = {word: i for i, word in enumerate(units)}
    for i, word in enumerate(tens):
        if word:
            num_dict[word] = i * 10
            
    # Split text by spaces or hyphens (e.g., "fifty-five")
    words = re.split(r'[\s-]+', text)
    
    current = 0
    total = 0
    
    for word in words:
        if word == "and":
            continue
        if word in num_dict:
            current += num_dict[word]
        elif word == "hundred":
            current = (current if current > 0 else 1) * 100
        elif word in scales:
            total += (current if current > 0 else 1) * scales[word]
            current = 0
            
    return total + current

df = spark.read.option("header", True).option("inferSchema", True).csv(INPUT_PATH)
# df.show()
df = df.dropDuplicates()
df = df.withColumn("FullName",F.trim(F.col("FullName"))).withColumn("Department", F.trim(F.col("Department")))
df = df.withColumn("error_type", F.when(F.col("FullName").isNull(), "Null Name").otherwise(""))
# df = df.filter(F.col("FullName").isNotNull())
df = df.filter(F.col("FullName").rlike(name_pattern))

df = df.withColumn("Gender", F.expr("case when Gender = 'Male' then 'M' when Gender ='Female' then 'F' when Gender = 'F' then 'F' when Gender = 'M' then 'M' else null end"))

df = df.withColumn(
    "DateOfBirth",
    F.date_format(
        F.coalesce(
            F.try_to_date(F.col("DateOfBirth"), "yyyy-MM-dd"),
            F.try_to_date(F.col("DateOfBirth"), "MM/dd/yyyy"),
            F.try_to_date(F.col("DateOfBirth"), "yyyy/MM/dd"),
            F.try_to_date(F.col("DateOfBirth"), "dd-MM-yyyy"),
            F.try_to_date(F.col("DateOfBirth"), "d-M-yyyy"),
            
        ),
        "yyyy-MM-dd"
    )
 )

df = df.withColumn("error_type", F.when(F.col("DateOfBirth").isNull(), "Null Date of Birth").otherwise(""))
df = df.filter(F.col("JoiningDate") != "invalid_date")
df = df.withColumn("JoiningDate", F.try_to_date(F.col("JoiningDate"), "yyyy-MM-dd"))

df = df.withColumn("Department", F.trim(F.initcap(F.col("Department"))))

convert_words_udf = F.udf(word_to_numeric, IntegerType())
df = df.withColumn("Salary", convert_words_udf(F.col("Salary")))

df = df.withColumn("Salary", F.when(F.col("Salary") < 0, 0).otherwise(F.col("Salary")))

df.orderBy("EmployeeID").show()