from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create Spark session
spark = SparkSession.builder \
    .appName("TransformJob") \
    .master("local[*]") \
    .getOrCreate()

# Define input/output paths - notice the file:/// format
raw_path = "file:///C:/Users/khatt/Desktop/DataEngineering/kafka_output"
output_path = "file:///C:/Users/khatt/Desktop/DataEngineering/transformed"
checkpoint_path = "file:///C:/Users/khatt/Desktop/DataEngineering/checkpoints"


# Define schema that MATCHES your CSV headers
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("page", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("kafka_timestamp", StringType(), True),
    StructField("processing_time", StringType(), True)
])


# Read continuously from the input folder (streaming)
df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .format("csv")\
    .load(f"{raw_path}/*.csv")

print("✅ df reading:", df.isStreaming)

# Apply transformations
df_clean = df
print(df_clean.isStreaming)
# Write to output
query = df_clean.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .option("header", "true") \
    .start()
print("✅ Stream started! Transforming CSV ..")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping the stream...")
    query.stop()
    spark.stop()
    print("Stream stopped successfully.")