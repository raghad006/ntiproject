
from pyspark.sql import SparkSession


def main():

    # ================================================================
    # 1. INITIALIZE SPARK
    # ================================================================

    spark = (
        SparkSession.builder
        .appName("AIS_Spark_SQL_Analytics")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("STARTING AIS SPARK SQL ANALYTICS")
    print("=" * 70)

    # ================================================================
    # 2. READ CLEANED AIS DATA FROM HDFS
    # ================================================================

    input_path = (
        "hdfs://namenode:9000/user/bigdata/ais/processed/ais_cleaned"
    )

    print("\nReading cleaned AIS data from:")
    print(input_path)

    df = spark.read.parquet(input_path)

    print("\nInput schema:")
    df.printSchema()

    # IMPORTANT:
    # Do NOT cache the entire 131M+ row dataset.
    # Our machine has limited memory and the previous run failed
    # with SparkOutOfMemoryError during aggregation.

    print("\nTotal records:")
    total_records = df.count()
    print(total_records)

    # ================================================================
    # 3. CHECK ORIGINAL DATE/TIME COLUMN
    # ================================================================

    print("\n" + "=" * 70)
    print("CHECKING BASE DATE/TIME")
    print("=" * 70)

    print("\nSample base_date_time values:")
    df.select("base_date_time").show(10, truncate=False)

    # We intentionally use the original base_date_time STRING.
    # The existing base_date_time_ts column contains NULL values.

    df.createOrReplaceTempView("ais")

    print("\nTemporary SQL view 'ais' created successfully.")

    # ================================================================
    # 4. DAILY TRAFFIC ANALYSIS
    # ================================================================

    print("\n" + "=" * 70)
    print("1. DAILY TRAFFIC ANALYSIS")
    print("=" * 70)

    daily_traffic_sql = """
        SELECT
            SUBSTRING(base_date_time, 1, 10) AS date,

            COUNT(*) AS total_records,

            APPROX_COUNT_DISTINCT(mmsi) AS unique_vessels,

            ROUND(
                AVG(CAST(sog AS DOUBLE)),
                2
            ) AS avg_speed_knots

        FROM ais

        WHERE base_date_time IS NOT NULL

        GROUP BY SUBSTRING(base_date_time, 1, 10)

        ORDER BY date
    """

    daily_traffic_df = spark.sql(daily_traffic_sql)

    daily_traffic_df.show(30, truncate=False)

    daily_output = (
        "hdfs://namenode:9000/user/bigdata/ais/analytics/daily_traffic"
    )

    (
        daily_traffic_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(daily_output)
    )

    print("\nDaily traffic saved to:")
    print(daily_output)

    # ================================================================
    # 5. VESSEL ACTIVITY ANALYSIS
    # ================================================================

    print("\n" + "=" * 70)
    print("2. VESSEL ACTIVITY ANALYSIS")
    print("=" * 70)

    vessel_summary_sql = """
        SELECT
            mmsi,

            COUNT(*) AS observation_count,

            ROUND(
                AVG(CAST(sog AS DOUBLE)),
                2
            ) AS avg_speed_knots,

            ROUND(
                MAX(CAST(sog AS DOUBLE)),
                2
            ) AS max_speed_knots,

            ROUND(
                AVG(CAST(latitude AS DOUBLE)),
                4
            ) AS avg_latitude,

            ROUND(
                AVG(CAST(longitude AS DOUBLE)),
                4
            ) AS avg_longitude

        FROM ais

        WHERE mmsi IS NOT NULL

        GROUP BY mmsi
    """

    vessel_summary_df = spark.sql(vessel_summary_sql)

    # Only keep the 20 most active vessels.
    vessel_summary_df = (
        vessel_summary_df
        .orderBy(
            "observation_count",
            ascending=False
        )
        .limit(20)
    )

    vessel_summary_df.show(20, truncate=False)

    vessel_output = (
        "hdfs://namenode:9000/user/bigdata/ais/analytics/vessel_summary"
    )

    (
        vessel_summary_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(vessel_output)
    )

    print("\nVessel summary saved to:")
    print(vessel_output)

    # ================================================================
    # 6. HOURLY TRAFFIC ANALYSIS
    # ================================================================

    print("\n" + "=" * 70)
    print("3. HOURLY TRAFFIC ANALYSIS")
    print("=" * 70)

    hourly_traffic_sql = """
        SELECT
            SUBSTRING(base_date_time, 12, 2) AS hour,

            COUNT(*) AS total_records,

            APPROX_COUNT_DISTINCT(mmsi) AS unique_vessels,

            ROUND(
                AVG(CAST(sog AS DOUBLE)),
                2
            ) AS avg_speed_knots

        FROM ais

        WHERE base_date_time IS NOT NULL

        GROUP BY SUBSTRING(base_date_time, 12, 2)

        ORDER BY hour
    """

    hourly_traffic_df = spark.sql(hourly_traffic_sql)

    hourly_traffic_df.show(30, truncate=False)

    hourly_output = (
        "hdfs://namenode:9000/user/bigdata/ais/analytics/hourly_traffic"
    )

    (
        hourly_traffic_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(hourly_output)
    )

    print("\nHourly traffic saved to:")
    print(hourly_output)

    # ================================================================
    # 7. GEOGRAPHIC TRAFFIC ANALYSIS
    # ================================================================

    print("\n" + "=" * 70)
    print("4. GEOGRAPHIC TRAFFIC ANALYSIS")
    print("=" * 70)

    geographic_sql = """
        SELECT
            ROUND(
                CAST(latitude AS DOUBLE),
                1
            ) AS latitude_zone,

            ROUND(
                CAST(longitude AS DOUBLE),
                1
            ) AS longitude_zone,

            COUNT(*) AS total_records,

            APPROX_COUNT_DISTINCT(mmsi) AS unique_vessels,

            ROUND(
                AVG(CAST(sog AS DOUBLE)),
                2
            ) AS avg_speed_knots

        FROM ais

        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL

        GROUP BY
            ROUND(CAST(latitude AS DOUBLE), 1),
            ROUND(CAST(longitude AS DOUBLE), 1)

        ORDER BY total_records DESC
    """

    geographic_df = spark.sql(geographic_sql)

    geographic_df.show(20, truncate=False)

    geographic_output = (
        "hdfs://namenode:9000/user/bigdata/ais/analytics/geographic_traffic"
    )

    (
        geographic_df
        .write
        .mode("overwrite")
        .parquet(geographic_output)
    )

    print("\nGeographic traffic saved to:")
    print(geographic_output)

    # ================================================================
    # 8. FINISH
    # ================================================================

    print("\n" + "=" * 70)
    print("AIS SPARK SQL ANALYTICS FINISHED SUCCESSFULLY")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()


