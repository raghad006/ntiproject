from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

spark = (
    SparkSession.builder
    .appName("InspectCleanedAIS")
    .getOrCreate()
)

input_path = "hdfs://namenode:9000/user/bigdata/ais/processed/ais_cleaned"

print("=" * 70)
print("READING CLEANED AIS DATA")
print("=" * 70)

df = spark.read.parquet(input_path)

print("\nSCHEMA:")
df.printSchema()

print("\nCOLUMNS:")
print(df.columns)

print("\nTOTAL ROWS:")
total_rows = df.count()
print(total_rows)

print("\nSAMPLE DATA:")
df.show(10, truncate=False)

print("\nNULL COUNTS:")
for c in df.columns:
    null_count = df.filter(col(c).isNull()).count()
    if null_count > 0:
        print(f"{c}: {null_count}")

print("\nDISTINCT COUNTS:")
for c in df.columns:
    try:
        distinct_count = df.select(countDistinct(col(c))).collect()[0][0]
        print(f"{c}: {distinct_count}")
    except Exception:
        pass

print("\nPARTITION COUNT:")
print(df.rdd.getNumPartitions())

print("\n" + "=" * 70)
print("INSPECTION COMPLETE")
print("=" * 70)

spark.stop()
