from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_DIR = "hdfs://namenode:9000/user/bigdata/ais"
CLEANED_PATH = "hdfs://namenode:9000/user/bigdata/ais/processed/ais_cleaned"

spark = (
    SparkSession.builder
    .appName("AIS-Data-Cleaning")
    .config("spark.executor.memory", "1500m")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memoryOverhead", "512m")
    .config("spark.executor.cores", "1")
    .config("spark.sql.files.maxPartitionBytes", "33554432")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext
conf = sc._jsc.hadoopConfiguration()
URI = spark._jvm.java.net.URI
Path = spark._jvm.org.apache.hadoop.fs.Path
FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem

fs = FileSystem.get(URI(RAW_DIR), conf)
raw_files = sorted([
    f.getPath().toString()
    for f in fs.listStatus(Path(RAW_DIR))
    if f.getPath().getName().lower().endswith(".csv")
])

print("Found {} raw files.".format(len(raw_files)))


def clean_data(df):
    df = df.withColumn(
        "base_date_time_ts",
        F.to_timestamp("base_date_time", "yyyy-MM-dd'T'HH:mm:ss")
    )
    df = df.filter(
        F.col("latitude").between(-90, 90) &
        F.col("longitude").between(-180, 180)
    )
    df = df.filter(
        F.col("sog").isNull() |
        ((F.col("sog") >= 0) & (F.col("sog") < 102.3))
    )
    df = df.withColumn(
        "status",
        F.when(F.col("status").isNull(), -1).otherwise(F.col("status"))
    )
    df = df.withColumn("vessel_name", F.trim(F.upper("vessel_name")))
    df = df.withColumn("call_sign", F.trim(F.upper("call_sign")))
    df = df.filter(F.length("mmsi") == 9)
    return df.dropDuplicates(["mmsi", "base_date_time"])


total_before = 0
total_after = 0

for i, file_path in enumerate(raw_files, 1):
    print("\n[{}/{}] {}".format(i, len(raw_files), file_path))

    df = spark.read.option("header", True).csv(file_path)
    before = df.count()

    cleaned = clean_data(df)
    after = cleaned.count()

    mode = "overwrite" if i == 1 else "append"
    cleaned.write.mode(mode).parquet(CLEANED_PATH)

    total_before += before
    total_after += after

    print("rows_before={} rows_after={}".format(before, after))

print("\n" + "=" * 50)
print("TOTAL rows_before = {}".format(total_before))
print("TOTAL rows_after  = {}".format(total_after))
print("TOTAL removed     = {}".format(total_before - total_after))

if total_before:
    print(
        "REMOVED %         = {:.2f}%".format(
            (total_before - total_after) * 100.0 / total_before
        )
    )

print("Cleaning complete.")
print("Saved to: {}".format(CLEANED_PATH))

spark.stop()