# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, current_timestamp
# from pyspark.sql.types import StructType, StringType, IntegerType
# import os

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("KafkaStreamExample") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
#     .config("spark.sql.streaming.checkpointLocation.minBatchesToRetain", "2") \
#     .getOrCreate()

# # Set log level to reduce console noise
# spark.sparkContext.setLogLevel("WARN")

# # Force local filesystem
# spark.conf.set("spark.hadoop.fs.defaultFS", "file:///")

# # Define schema for incoming JSON data
# schema = StructType() \
#     .add("event_id", StringType()) \
#     .add("user_id", IntegerType()) \
#     .add("page", StringType()) \
#     .add("timestamp", StringType())

# # Checkpoint and output locations (using forward slashes for Windows)
# checkpoint_base = "file:///C:/Users/khatt/Desktop/DataEngineering/spark_checkpoints"
# output_base = "file:///C:/Users/khatt/Desktop/DataEngineering/kafka_output"

# # OPTIONAL: Uncomment these lines to auto-cleanup on startup (useful for testing)
# # import shutil
# # if os.path.exists(checkpoint_base):
# #     shutil.rmtree(checkpoint_base)
# # if os.path.exists(output_base):
# #     shutil.rmtree(output_base)

# # Ensure directories exist
# checkpoint_local = "C:/Users/khatt/Desktop/DataEngineering/spark_checkpoints"
# output_local = "C:/Users/khatt/Desktop/DataEngineering/kafka_output"
# os.makedirs(checkpoint_local, exist_ok=True)
# os.makedirs(output_local, exist_ok=True)

# # Use simple paths for Windows compatibility
# checkpoint_dir = checkpoint_base
# output_dir = output_base

# # Read from Kafka
# df = (spark.readStream
#       .format("kafka")
#       .option("kafka.bootstrap.servers", "localhost:9092")
#       .option("subscribe", "click_events")
#       .option("startingOffsets", "latest")  # Start from latest messages
#       .option("failOnDataLoss", "false")  # Don't fail if data is lost
#       .option("maxOffsetsPerTrigger", "1000")  # Limit messages per batch
#       .load())

# # Cast Kafka message to string
# stream_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp as kafka_timestamp")

# # Parse JSON and add processing timestamp
# parsed_df = (stream_df
#              .select(
#                  from_json(col("value"), schema).alias("data"),
#                  col("kafka_timestamp")
#              )
#              .select("data.*", "kafka_timestamp")
#              .withColumn("processing_time", current_timestamp())
#              .filter(col("event_id").isNotNull()))  # Filter out malformed records

# # Write to CSV files (using the file URI format defined earlier)
# query = (parsed_df.writeStream
#          .format("csv")
#          .outputMode("append")
#          .option("checkpointLocation", checkpoint_local)
#          .option("path", output_local)
#          .option("header", "true")  # Include column headers
#          .trigger(processingTime='10 seconds')  # Create new batch every 30 seconds
#          .start())

# print(f"Streaming query started.")
# print(f"Checkpoint location: {checkpoint_dir}")
# print(f"Output location: {output_dir}")
# print("CSV files will be created in batches every 30 seconds.")
# print("Press Ctrl+C to stop...")

# try:
#     query.awaitTermination()
# except KeyboardInterrupt:
#     print("\nStopping the stream...")
#     query.stop()
#     spark.stop()
#     print("Stream stopped successfully.")







from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("KafkaStreamExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ✅ Local (for Python operations)
checkpoint_local = "C:/Users/khatt/Desktop/DataEngineering/spark_checkpoints"
output_local = "C:/Users/khatt/Desktop/DataEngineering/kafka_output"

# ✅ URI (for Spark operations)
checkpoint_base = "file:///C:/Users/khatt/Desktop/DataEngineering/spark_checkpoints"
output_base = "file:///C:/Users/khatt/Desktop/DataEngineering/kafka_output"

os.makedirs(checkpoint_local, exist_ok=True)
os.makedirs(output_local, exist_ok=True)

# ✅ Schema
schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", IntegerType()) \
    .add("page", StringType()) \
    .add("timestamp", StringType())

# ✅ Kafka source
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "click_events")
      .option("startingOffsets", "earliest")
      .load())

stream_df = df.selectExpr("CAST(value AS STRING)", "timestamp as kafka_timestamp")

parsed_df = (stream_df
             .select(from_json(col("value"), schema).alias("data"), col("kafka_timestamp"))
             .select("data.*", "kafka_timestamp")
             .withColumn("processing_time", current_timestamp())
             .filter(col("event_id").isNotNull()))

# ✅ Write stream to CSV
query = (parsed_df.writeStream
         .format("csv")
         .outputMode("append")
         .option("checkpointLocation", checkpoint_base)   # URI path
         .option("path", output_base)                     # URI path
         .option("header", "true")
         .trigger(processingTime="10 seconds")
         .start())

print("✅ Stream started! Writing CSV batches every 10 seconds...")
print("📁 Output:", output_local)
print("📂 Checkpoint:", checkpoint_local)

query.awaitTermination()
