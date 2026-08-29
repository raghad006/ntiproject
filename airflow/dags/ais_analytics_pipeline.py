from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# AIS END-TO-END AIRFLOW PIPELINE
#
# NOAA AIS (.zst)
#      ↓
# HDFS raw CSV
#      ↓
# data_describe.py
#      ↓
# clean_ais_data.py
#      ↓
# ais_ml.py
#      ↓
# create_analytics.py
#      ↓
# export_analytics.py
#
# Spark Thrift Server is NOT part of this DAG.
# Start it separately later when Power BI needs JDBC/ODBC access.
# ============================================================


# ============================================================
# HDFS CONFIGURATION
# ============================================================

HDFS = "hdfs://namenode:9000"

RAW_HDFS = f"{HDFS}/user/bigdata/ais"
PROCESSED_HDFS = f"{RAW_HDFS}/processed"
ML_HDFS = f"{RAW_HDFS}/ml"
ANALYTICS_HDFS = f"{RAW_HDFS}/analytics"
POWERBI_HDFS = f"{RAW_HDFS}/powerbi"


# ============================================================
# NOAA CONFIGURATION
# ============================================================

NOAA_BASE_URL = (
    "https://noaaocm.blob.core.windows.net/"
    "ais/csv2/csv2025"
)

# November 1 -> November 15, 2025
AIS_FILES = [
    f"ais-2025-11-{day:02d}.csv.zst"
    for day in range(1, 16)
]


# ============================================================
# AIRFLOW DEFAULT SETTINGS
# ============================================================

default_args = {
    "owner": "raghad",
    "depends_on_past": False,

    # IMPORTANT:
    # We do not automatically retry the heavy Spark jobs.
    # This avoids launching another expensive Spark job after
    # an OOM/resource failure.
    "retries": 0,

    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="ais_end_to_end_pipeline",

    description=(
        "NOAA AIS ingestion to HDFS, data profiling, cleaning, "
        "Spark MLlib K-Means, Spark SQL analytics and Power BI export"
    ),

    default_args=default_args,

    start_date=datetime(2026, 8, 29),

    # Manual execution only for now.
    schedule=None,

    catchup=False,

    # Resource protection.
    # Never run two copies of this pipeline at the same time.
    max_active_runs=1,
    max_active_tasks=1,

    tags=[
        "AIS",
        "HDFS",
        "Spark",
        "MLlib",
        "Analytics",
        "PowerBI",
    ],
) as dag:

    # ========================================================
    # 1. CREATE HDFS DIRECTORIES
    # ========================================================

    create_hdfs_directories = BashOperator(
        task_id="create_hdfs_directories",

        bash_command=f"""
        set -e

        echo "=================================================="
        echo "STEP 1 - CREATE HDFS DIRECTORIES"
        echo "=================================================="

        hdfs dfs -mkdir -p "{RAW_HDFS}"
        hdfs dfs -mkdir -p "{PROCESSED_HDFS}"
        hdfs dfs -mkdir -p "{ML_HDFS}"
        hdfs dfs -mkdir -p "{ANALYTICS_HDFS}"
        hdfs dfs -mkdir -p "{POWERBI_HDFS}"

        echo ""
        echo "HDFS directories:"
        hdfs dfs -ls -h "{RAW_HDFS}"

        echo ""
        echo "HDFS directory creation completed."
        """,
    )


    # ========================================================
    # 2. NOAA -> HDFS
    #
    # Each .zst file is streamed:
    #
    # NOAA
    #   ↓
    # curl
    #   ↓
    # zstd
    #   ↓
    # HDFS
    #
    # No giant decompressed CSV is stored in /tmp.
    #
    # Existing files are skipped so rerunning the DAG does
    # NOT download them again.
    # ========================================================

    download_noaa_ais = BashOperator(
        task_id="download_noaa_ais",

        bash_command=f"""
        set -e

        echo "=================================================="
        echo "STEP 2 - NOAA AIS INGESTION"
        echo "November 1-15, 2025"
        echo "=================================================="

        hdfs dfs -mkdir -p "{RAW_HDFS}"

        for day in $(seq -w 1 15)
        do
            file="ais-2025-11-$day.csv.zst"
            csv_file="ais-2025-11-$day.csv"

            url="{NOAA_BASE_URL}/$file"
            output="{RAW_HDFS}/$csv_file"

            echo ""
            echo "--------------------------------------------------"
            echo "FILE:   $file"
            echo "OUTPUT: $output"
            echo "--------------------------------------------------"

            # ------------------------------------------------
            # Do NOT re-download files that are already in HDFS.
            # ------------------------------------------------

            if hdfs dfs -test -e "$output"; then
                echo "Already exists in HDFS -> SKIPPING"
                continue
            fi

            echo "Downloading and streaming into HDFS..."

            curl -L \
                --fail \
                --retry 3 \
                --retry-delay 5 \
                "$url" \
            | zstd -dc \
            | hdfs dfs -put -f - "$output"

            echo "Completed: $file"
        done

        echo ""
        echo "=================================================="
        echo "NOAA INGESTION COMPLETE"
        echo "=================================================="

        echo ""
        echo "AIS files currently in HDFS:"
        hdfs dfs -ls -h "{RAW_HDFS}"
        """,
    )


    # ========================================================
    # 3. DATA DESCRIPTION / PROFILING
    #
    # Existing script:
    # /opt/spark-apps/data_describe.py
    #
    # Reads:
    # /user/bigdata/ais/*.csv
    # ========================================================

    data_describe = BashOperator(
        task_id="data_describe",

        bash_command="""
        set -e

        echo "=================================================="
        echo "STEP 3 - AIS DATA DESCRIPTION"
        echo "=================================================="

        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark-apps/data_describe.py

        echo ""
        echo "Data description completed."
        """,
    )


    # ========================================================
    # 4. DATA CLEANING
    #
    # Existing script:
    # /opt/spark-apps/clean_ais_data.py
    #
    # Reads:
    # /user/bigdata/ais/*.csv
    #
    # Writes:
    # /user/bigdata/ais/processed/ais_cleaned
    # ========================================================

    clean_ais_data = BashOperator(
        task_id="clean_ais_data",

        bash_command="""
        set -e

        echo "=================================================="
        echo "STEP 4 - AIS DATA CLEANING"
        echo "=================================================="

        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark-apps/clean_ais_data.py

        echo ""
        echo "AIS cleaning completed."
        """,
    )


    # ========================================================
    # 5. MLlib / K-MEANS
    #
    # Existing script:
    # /opt/spark-apps/ais_ml.py
    #
    # Reads:
    # /user/bigdata/ais/processed/ais_cleaned
    #
    # Writes:
    # /user/bigdata/ais/ml/kmeans_results
    # /user/bigdata/ais/ml/kmeans_summary
    # ========================================================

    ais_ml_kmeans = BashOperator(
        task_id="ais_ml_kmeans",

        bash_command="""
        set -e

        echo "=================================================="
        echo "STEP 5 - AIS MLlib K-MEANS"
        echo "=================================================="

        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark-apps/ais_ml.py

        echo ""
        echo "AIS MLlib K-Means completed."
        """,
    )


    # ========================================================
    # 6. SPARK SQL ANALYTICS
    #
    # Existing script:
    # /opt/spark-apps/create_analytics.py
    #
    # Creates:
    # daily_traffic
    # vessel_summary
    # hourly_traffic
    # geographic_traffic
    #
    # Under:
    # /user/bigdata/ais/analytics/
    # ========================================================

    create_analytics = BashOperator(
        task_id="create_analytics",

        bash_command="""
        set -e

        echo "=================================================="
        echo "STEP 6 - SPARK SQL ANALYTICS"
        echo "=================================================="

        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark-apps/create_analytics.py

        echo ""
        echo "Spark SQL analytics completed."
        """,
    )


    # ========================================================
    # 7. POWER BI EXPORT
    #
    # Existing script:
    # /opt/spark-apps/export_analytics.py
    #
    # Creates:
    # /user/bigdata/ais/powerbi/
    #
    # with:
    # daily_traffic
    # vessel_summary
    # hourly_traffic
    # geographic_traffic
    # ========================================================

    export_powerbi = BashOperator(
        task_id="export_powerbi",

        bash_command="""
        set -e

        echo "=================================================="
        echo "STEP 7 - POWER BI EXPORT"
        echo "=================================================="

        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /opt/spark-apps/export_analytics.py

        echo ""
        echo "Power BI export completed."
        """,
    )


    # ========================================================
    # PIPELINE DEPENDENCIES
    # ========================================================

    (
        create_hdfs_directories
        >> download_noaa_ais
        >> data_describe
        >> clean_ais_data
        >> ais_ml_kmeans
        >> create_analytics
        >> export_powerbi
    )
