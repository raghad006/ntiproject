from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    isnan,
    approx_count_distinct
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    IntegerType
)

spark = (
    SparkSession.builder
    .appName("AIS Data Profiling & Describe")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("mmsi", LongType(), True),
    StructField("base_date_time", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("sog", DoubleType(), True),
    StructField("cog", DoubleType(), True),
    StructField("heading", DoubleType(), True),
    StructField("vessel_name", StringType(), True),
    StructField("imo", StringType(), True),
    StructField("call_sign", StringType(), True),
    StructField("vessel_type", IntegerType(), True),
    StructField("status", IntegerType(), True),
    StructField("length", DoubleType(), True),
    StructField("width", DoubleType(), True),
    StructField("draft", DoubleType(), True),
    StructField("cargo", IntegerType(), True),
    StructField("transceiver", StringType(), True)
])

input_path = "hdfs://namenode:9000/user/bigdata/ais/*.csv"

df = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(input_path)
)

print("\n" + "=" * 70)
print("AIS DATA PROFILING")
print("=" * 70)

print("\nInput path:")
print(input_path)

print("\n" + "=" * 70)
print("DATA SCHEMA")
print("=" * 70)

df.printSchema()

print("\n" + "=" * 70)
print("ROW COUNT / NULL COUNTS / APPROX DISTINCT COUNTS")
print("=" * 70)

profile_expressions = [
    count("*").alias("total_records")
]

for field in schema.fields:
    c = field.name

    if isinstance(field.dataType, DoubleType):
        profile_expressions.append(
            count(
                when(
                    col(c).isNull() | isnan(c),
                    col(c)
                )
            ).alias(f"{c}_nulls")
        )
    else:
        profile_expressions.append(
            count(
                when(
                    col(c).isNull(),
                    col(c)
                )
            ).alias(f"{c}_nulls")
        )

    profile_expressions.append(
        approx_count_distinct(col(c)).alias(
            f"{c}_approx_distinct"
        )
    )

profile = df.agg(*profile_expressions).collect()[0]

print("\nTotal Records:")
print(profile["total_records"])

print("\nColumn profiling:")

for field in schema.fields:
    c = field.name

    print(
        "{} -> nulls={}, approx_distinct={}".format(
            c,
            profile[f"{c}_nulls"],
            profile[f"{c}_approx_distinct"]
        )
    )

print("\n" + "=" * 70)
print("NUMERIC SUMMARY STATISTICS")
print("Calculated on a 1% representative sample")
print("=" * 70)

numeric_columns = [
    "longitude",
    "latitude",
    "sog",
    "cog",
    "heading",
    "length",
    "width",
    "draft"
]

numeric_sample = (
    df.select(numeric_columns)
    .sample(
        withReplacement=False,
        fraction=0.01,
        seed=42
    )
)

numeric_sample.describe().show(truncate=False)

print("\n" + "=" * 70)
print("SAMPLE DATA")
print("=" * 70)

df.show(5, truncate=False)

print("\n" + "=" * 70)
print("AIS DATA PROFILING COMPLETED")
print("=" * 70)

spark.stop()
