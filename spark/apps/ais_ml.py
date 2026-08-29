from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    hour,
    dayofweek,
    count,
    lag
)
from pyspark.sql.window import Window

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler

# ============================================================
# 1. Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("AIS_MLlib_Analytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Read cleaned Parquet
# ============================================================

INPUT_PATH = "hdfs://namenode:9000/user/bigdata/ais/processed/ais_cleaned"

df = spark.read.parquet(INPUT_PATH)

print("\n==============================")
print("AIS DATA LOADED")
print("==============================")

df.printSchema()


# ============================================================
# 3. Fix timestamp
# ============================================================

df = df.withColumn(
    "timestamp",
    to_timestamp(col("base_date_time"), "yyyy-MM-dd HH:mm:ss")
)


# ============================================================
# 4. Convert numeric columns
# ============================================================

numeric_cols = [
    "longitude",
    "latitude",
    "sog",
    "cog"
]

for c in numeric_cols:
    df = df.withColumn(c, col(c).cast("double"))


# ============================================================
# 5. Prepare ML dataset
# ============================================================

ml_df = df.select(
    "mmsi",
    "timestamp",
    "longitude",
    "latitude",
    "sog",
    "cog"
)

# Remove invalid records
ml_df = ml_df.dropna(
    subset=[
        "longitude",
        "latitude",
        "sog",
        "cog"
    ]
)

# Keep realistic AIS movement values
ml_df = ml_df.filter(
    (col("sog") >= 0) &
    (col("sog") <= 50) &
    (col("cog") >= 0) &
    (col("cog") <= 360) &
    (col("latitude") >= -90) &
    (col("latitude") <= 90) &
    (col("longitude") >= -180) &
    (col("longitude") <= 180)
)


# ============================================================
# 6. Sample data for K-Means training
# ============================================================
#
# We have 131M+ observations, so we train using a
# representative sample rather than forcing K-Means
# to process every single row.
#

training_df = ml_df.sample(
    withReplacement=False,
    fraction=0.01,
    seed=42
)

print("\n==============================")
print("TRAINING SAMPLE CREATED")
print("==============================")


# ============================================================
# 7. Assemble features
# ============================================================

assembler = VectorAssembler(
    inputCols=[
        "longitude",
        "latitude",
        "sog",
        "cog"
    ],
    outputCol="raw_features",
    handleInvalid="skip"
)

training_features = assembler.transform(training_df)


# ============================================================
# 8. Standardize features
# ============================================================

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=True
)

scaler_model = scaler.fit(training_features)

training_scaled = scaler_model.transform(training_features)


# ============================================================
# 9. Train K-Means
# ============================================================

K = 4

kmeans = KMeans(
    k=K,
    seed=42,
    featuresCol="features",
    predictionCol="prediction"
)

kmeans_model = kmeans.fit(training_scaled)


# ============================================================
# 10. Evaluate clustering
# ============================================================

predictions = kmeans_model.transform(training_scaled)

evaluator = ClusteringEvaluator(
    predictionCol="prediction",
    featuresCol="features",
    metricName="silhouette"
)

silhouette = evaluator.evaluate(predictions)

print("\n==============================")
print("K-MEANS RESULTS")
print("==============================")
print("Number of clusters: {}".format(K))
print("Silhouette score: {}".format(silhouette))


# ============================================================
# 11. Inspect cluster characteristics
# ============================================================

predictions.groupBy("prediction").agg(
    count("*").alias("observations")
).orderBy("prediction").show()


predictions.groupBy("prediction").agg(
    {"sog": "avg", "cog": "avg",
     "latitude": "avg", "longitude": "avg"}
).orderBy("prediction").show()


# ============================================================
# 12. Apply trained model to full dataset
# ============================================================

full_features = assembler.transform(ml_df)

full_scaled = scaler_model.transform(full_features)

full_predictions = kmeans_model.transform(full_scaled)


# ============================================================
# 13. Create final ML output
# ============================================================

ml_results = full_predictions.select(
    "mmsi",
    "timestamp",
    "longitude",
    "latitude",
    "sog",
    "cog",
    col("prediction").alias("vessel_cluster")
)


# ============================================================
# 14. Save ML results as Parquet
# ============================================================

OUTPUT_PATH = \
    "hdfs://namenode:9000/user/bigdata/ais/ml/kmeans_results"

ml_results.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)


# ============================================================
# 15. Save cluster summary
# ============================================================

summary = ml_results.groupBy(
    "vessel_cluster"
).agg(
    count("*").alias("observation_count")
)

SUMMARY_PATH = \
    "hdfs://namenode:9000/user/bigdata/ais/ml/kmeans_summary"

summary.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(SUMMARY_PATH)


print("\n==============================")
print("ML COMPLETE")
print("==============================")
print("Results: {}".format(OUTPUT_PATH))
print("Summary: {}".format(SUMMARY_PATH))
print("==============================\n")


spark.stop()