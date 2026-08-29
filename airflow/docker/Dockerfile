ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.7.1-python3.10

FROM ${AIRFLOW_BASE_IMAGE}

USER root

# ============================================================
# System tools
# ============================================================

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre \
        procps \
        curl \
        zstd \
        wget \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# ============================================================
# Hadoop client
# Match the Hadoop cluster: 3.2.1
# ============================================================

ARG HADOOP_VERSION=3.2.1

RUN wget -q \
    https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt/ && \
    rm /tmp/hadoop.tar.gz


# ============================================================
# Spark client
# Match the Spark cluster: 3.0.0 + Hadoop 3.2
# ============================================================

ARG SPARK_VERSION=3.0.0
ARG SPARK_HADOOP_VERSION=3.2

RUN wget -q \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz \
    -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    rm /tmp/spark.tgz


# ============================================================
# Environment
# ============================================================

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}

ENV HADOOP_CONF_DIR=/opt/hadoop-${HADOOP_VERSION}/etc/hadoop

ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"


# ============================================================
# Airflow
# ============================================================

USER airflow

ARG AIRFLOW_VERSION=2.7.1

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-apache-spark" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"