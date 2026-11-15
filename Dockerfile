FROM apache/airflow:2.9.3

# Spark needs a JVM. The distro package works on both x86 and ARM machines.
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
USER airflow

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Download the Spark-Kafka connector and the Postgres driver once at build time so DAG runs do not need Maven.
# Keep in sync with pyspark in requirements.txt and SPARK_PACKAGES in scripts/config.py.
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3') \
.getOrCreate().stop()"
