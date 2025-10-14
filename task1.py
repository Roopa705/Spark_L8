from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("RideSharingAnalytics-Task1") \
    .getOrCreate()

# Set log level to reduce verbose output
spark.sparkContext.setLogLevel("WARN")

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into columns using the defined schema
parsed_stream = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Define a function to write each batch to CSV AND print to console
def write_batch_to_csv(batch_df, batch_id):
    if not batch_df.isEmpty():
        # Print to console
        print(f"\n{'='*50}")
        print(f"Batch {batch_id}: {batch_df.count()} rows")
        print(f"{'='*50}")
        batch_df.show(truncate=False)
        
        # Write to CSV with headers
        batch_df.coalesce(1).write \
            .mode("append") \
            .option("header", "true") \
            .csv(f"outputs/task_1/batch_{batch_id}")
        print(f"✓ Batch {batch_id} saved to outputs/task_1/batch_{batch_id}/")
    else:
        print(f"✗ Batch {batch_id} is empty, skipping")

# Write to CSV using foreachBatch (combines console + CSV in single query)
query = parsed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch_to_csv) \
    .option("checkpointLocation", "checkpoints/task_1") \
    .start()

print("Streaming started. Waiting for data from localhost:9999...")
print("Press Ctrl+C to stop.\n")

# Wait for the query to finish
query.awaitTermination()