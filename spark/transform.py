from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
spark = SparkSession.builder \
    .appName("KafkaStreamExample") \
    .getOrCreate()
print("✅ Spark Session started successfully!")
print("Spark version:", spark.version)
raw_path = "file:///C:/Users/khatt/Desktop/DataEngineering/kafka_output"
df = spark.read.csv(f"{raw_path}/*.csv", header=True, inferSchema=True)
df_clean = df
raw_path = "file:///C:/Users/khatt/Desktop/DataEngineering/kafka_output"
df_clean.write.mode("overwrite").parquet(f"{raw_path}/transformed/")


print("✅ Transformation complete. Data written to:")
print(f"   {raw_path}/transformed/")

spark.stop()