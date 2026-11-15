"""Kafka -> Postgres, with Spark Structured Streaming.

Reads all four topics and appends each one to a raw table of the same name in
the warehouse database (click_events, user_signed_in, order_placed,
payment_completed).

    python spark_stream.py          # keep running, one micro-batch every 10 seconds
    python spark_stream.py --once   # process everything new in Kafka, then exit (used by Airflow)

The checkpoint remembers the Kafka offsets, so each run picks up where the
previous one stopped.
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

from config import (CHECKPOINT_DIR, JDBC_PROPERTIES, KAFKA_BOOTSTRAP_SERVERS,
                    SPARK_PACKAGES, TOPICS, WAREHOUSE_JDBC_URL, uri)

_COMMON = [
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("timestamp", StringType()),
]

# The fields each topic carries on top of the common ones.
SCHEMAS = {
    "click_events": StructType(_COMMON + [StructField("page", StringType())]),
    "user_signed_in": StructType(_COMMON + [StructField("device", StringType())]),
    "order_placed": StructType(_COMMON + [
        StructField("order_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("items", IntegerType()),
    ]),
    "payment_completed": StructType(_COMMON + [
        StructField("order_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("method", StringType()),
        StructField("status", StringType()),
    ]),
}
assert set(SCHEMAS) == set(TOPICS)


def write_batch(batch: DataFrame, batch_id: int) -> None:
    """Split one micro-batch by topic and append each part to its own table."""
    batch.persist()
    try:
        for topic, schema in SCHEMAS.items():
            rows = (batch.where(col("topic") == topic)
                    .select(from_json(col("value").cast("string"), schema).alias("e"),
                            col("timestamp").alias("kafka_timestamp"))
                    .select("e.*", "kafka_timestamp")
                    .where(col("event_id").isNotNull())  # drop messages that are not valid events
                    .withColumn("event_time", to_timestamp("timestamp"))
                    .drop("timestamp")
                    .withColumn("ingested_at", current_timestamp()))
            # An empty write still creates the table, so the transform can always read it.
            rows.write.jdbc(WAREHOUSE_JDBC_URL, topic, mode="append", properties=JDBC_PROPERTIES)
        print(f"Batch {batch_id}: wrote {batch.count()} events to Postgres", flush=True)
    finally:
        batch.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--once", action="store_true",
                        help="process all available data and exit")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("KafkaToPostgres")
             .master("local[*]")
             .config("spark.jars.packages", SPARK_PACKAGES)
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", ",".join(TOPICS))
                .option("startingOffsets", "earliest")
                .load())

    writer = (kafka_df.writeStream
              .foreachBatch(write_batch)
              .option("checkpointLocation", uri(CHECKPOINT_DIR / "kafka_to_postgres")))
    writer = (writer.trigger(availableNow=True) if args.once
              else writer.trigger(processingTime="10 seconds"))

    query = writer.start()
    print(f"Stream started: {', '.join(TOPICS)} -> {WAREHOUSE_JDBC_URL}", flush=True)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping the stream...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
