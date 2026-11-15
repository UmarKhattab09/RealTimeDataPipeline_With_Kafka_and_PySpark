"""Batch transform: raw click CSVs -> cleaned events and aggregates.

Reads everything in data/raw and rewrites three outputs in data/transformed:

    clean_events/        typed, de-duplicated events with event_date and ingest_lag_seconds
    page_views_per_min/  views and unique users per page per 1-minute window
    user_activity/       per-user totals, first/last seen, and whether they reached /checkout

Each run overwrites the outputs, so it is safe to run repeatedly.
"""
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from config import RAW_DIR, TRANSFORMED_DIR, uri

RAW_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("page", StringType()),
    StructField("timestamp", StringType()),
    StructField("kafka_timestamp", StringType()),
    StructField("processing_time", StringType()),
])


def clean(raw: DataFrame) -> DataFrame:
    return (raw
            .withColumn("user_id", F.col("user_id").cast("int"))
            .withColumn("event_time", F.to_timestamp("timestamp"))
            .withColumn("kafka_timestamp", F.to_timestamp("kafka_timestamp"))
            .withColumn("processing_time", F.to_timestamp("processing_time"))
            .where(F.col("event_id").isNotNull()
                   & F.col("user_id").isNotNull()
                   & F.col("page").isNotNull()
                   & F.col("event_time").isNotNull())
            .dropDuplicates(["event_id"])
            .withColumn("event_date", F.to_date("event_time"))
            .withColumn("ingest_lag_seconds",
                        F.round(F.col("processing_time").cast("double")
                                - F.col("kafka_timestamp").cast("double"), 3))
            .select("event_id", "user_id", "page", "event_time", "event_date",
                    "kafka_timestamp", "processing_time", "ingest_lag_seconds"))


def page_views_per_minute(events: DataFrame) -> DataFrame:
    return (events
            .groupBy(F.window("event_time", "1 minute").alias("w"), "page")
            .agg(F.count("*").alias("views"),
                 F.countDistinct("user_id").alias("unique_users"))
            .select(F.col("w.start").alias("window_start"),
                    F.col("w.end").alias("window_end"),
                    "page", "views", "unique_users")
            .orderBy("window_start", "page"))


def user_activity(events: DataFrame) -> DataFrame:
    return (events
            .groupBy("user_id")
            .agg(F.count("*").alias("total_events"),
                 F.countDistinct("page").alias("distinct_pages"),
                 F.min("event_time").alias("first_seen"),
                 F.max("event_time").alias("last_seen"),
                 F.max((F.col("page") == "/checkout").cast("int")).cast("boolean")
                  .alias("reached_checkout"))
            .orderBy("user_id"))


def write_csv(df: DataFrame, name: str) -> None:
    (df.coalesce(1).write.mode("overwrite").option("header", "true")
       .csv(uri(TRANSFORMED_DIR / name)))


def main() -> None:
    if not any(RAW_DIR.glob("*.csv")):
        print(f"No raw CSV files in {RAW_DIR} yet - run producer.py and spark_stream.py first.")
        sys.exit(0)

    spark = (SparkSession.builder
             .appName("TransformJob")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")  # small data, keep it fast
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.option("header", "true").schema(RAW_SCHEMA).csv(uri(RAW_DIR))
        events = clean(raw).cache()
        write_csv(events.orderBy("event_time"), "clean_events")
        write_csv(page_views_per_minute(events), "page_views_per_min")
        write_csv(user_activity(events), "user_activity")
        print(f"Transformed {events.count()} events -> {TRANSFORMED_DIR}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
