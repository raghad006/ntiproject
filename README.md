# 🚢 Marine AIS Big Data Analytics

An end-to-end Big Data platform for processing and analyzing large-scale **Automatic Identification System (AIS)** vessel data to understand maritime traffic, vessel behavior, and geographic activity.

## 📊 Dashboard

### Overview

![Marine AIS Dashboard - Overview](docs/dashboard_overview.png)

### Vessel & Traffic Analysis

![Marine AIS Dashboard - Analysis](docs/dashboard_analysis.png)

The Power BI dashboard covers vessel activity, traffic trends, geographic distribution, vessel speed, and clustering results.

## 🏗️ Architecture

```text
AIS Data → Kafka → Hadoop HDFS → Apache Spark
                              ↓
                    Spark SQL + Spark MLlib
                              ↓
                         Airflow
                              ↓
                         Power BI
```

## 🛠️ Tech Stack

* **Python** — Data processing
* **Kafka** — Data ingestion
* **Hadoop HDFS** — Distributed storage
* **Apache Spark** — Processing & analytics
* **Spark MLlib** — Vessel clustering
* **Apache Airflow** — Workflow orchestration
* **Docker** — Containerization
* **Power BI** — Visualization

## 💾 Dataset

The project uses **2025 AIS vessel tracking data** from NOAA/NCEI.

📥 **[NOAA/NCEI AIS 2025 Dataset](https://noaaocm.blob.core.windows.net/ais/csv2/csv2025/index.html?utm_source)**

The raw dataset is not included in this repository due to its size.

## 📈 Analytics

Spark transforms the raw AIS data into optimized datasets for Power BI:

* Daily traffic
* Hourly traffic
* Geographic traffic
* Vessel summaries
* Vessel clusters

Processed data is stored in **Parquet format on HDFS**.

## 🚀 Pipeline

```text
Raw AIS Data
     ↓
Kafka / HDFS
     ↓
Spark Cleaning
     ↓
Parquet
     ↓
Spark SQL + MLlib
     ↓
Aggregated Analytics
     ↓
Power BI
```

## 🐳 Running the Project

```bash
docker compose up -d
```

Check the environment:

```bash
docker compose ps
```

Run a Spark application:

```bash
docker exec -it spark-master \
/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark-apps/ais_ml.py
```

## 🎯 Project Goals

* Process large-scale maritime data
* Analyze vessel traffic and behavior
* Apply machine learning for vessel clustering
* Automate the pipeline with Airflow
* Deliver interactive maritime insights through Power BI

