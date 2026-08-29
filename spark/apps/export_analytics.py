from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AIS_Analytics_Export") \
    .getOrCreate()

base = "hdfs://namenode:9000/user/bigdata/ais/analytics"
output_base = "hdfs://namenode:9000/user/bigdata/ais/powerbi"

datasets = {
    "daily_traffic": base + "/daily_traffic",
    "vessel_summary": base + "/vessel_summary",
    "hourly_traffic": base + "/hourly_traffic",
    "geographic_traffic": base + "/geographic_traffic"
}

print("=" * 70)
print("EXPORTING AIS ANALYTICS FOR POWER BI")
print("=" * 70)

for name in datasets:

    path = datasets[name]

    print("\nExporting: " + name)
    print("Input: " + path)

    df = spark.read.parquet(path)

    output_path = output_base + "/" + name

    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(output_path)

    print("Saved: " + output_path)

print("\n" + "=" * 70)
print("ALL ANALYTICS EXPORTED SUCCESSFULLY")
print("=" * 70)

spark.stop()
