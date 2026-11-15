"""Kafka -> raw CSV, with Spark Structured Streaming.

    python spark_stream.py          # keep running, one micro-batch every 10 seconds
    python spark_stream.py --once   # process everything new in Kafka, then exit (used by Airflow)

The checkpoint remembers the Kafka offsets, so each run picks up where the
previous one stopped and no event is written twice.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from config import (CHECKPOINT_DIR, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, RAW_DIR,
                    SPARK_KAFKA_PACKAGE, uri)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("page", StringType()),
    StructField("timestamp", StringType()),
])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true",
                        help="process all available data and exit")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("KafkaToCsv")
             .master("local[*]")
             .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest")
                .load())

    events = (kafka_df
              .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                      col("timestamp").alias("kafka_timestamp"))
              .select("e.*", "kafka_timestamp")
              .where(col("event_id").isNotNull())  # drop messages that are not valid JSON events
              .withColumn("processing_time", current_timestamp()))

    writer = (events.writeStream
              .format("csv")
              .outputMode("append")
              .option("header", "true")
              .option("path", uri(RAW_DIR))
              .option("checkpointLocation", uri(CHECKPOINT_DIR / "kafka_to_csv")))
    writer = (writer.trigger(availableNow=True) if args.once
              else writer.trigger(processingTime="10 seconds"))

    query = writer.start()
    print(f"Stream started: {KAFKA_TOPIC} -> {RAW_DIR}")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping the stream...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
