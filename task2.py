from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("RideSharingAnalytics-Task2") \
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

# Compute aggregations: total fare and average distance grouped by driver_id
aggregated_stream = parsed_stream.groupBy("driver_id") \
    .agg(
        spark_sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

# Define a function to write each batch to a CSV file
def write_to_csv(batch_df, batch_id):
    # Save the batch DataFrame as a CSV file with the batch ID in the filename
    batch_df.coalesce(1).write \
        .mode("append") \
        .option("header", "true") \
        .csv(f"outputs/task_2/batch_{batch_id}")
    print(f"Batch {batch_id} written to CSV")

# Use foreachBatch to apply the function to each micro-batch
query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "checkpoints/task_2") \
    .start()

# Also print to console for monitoring
console_query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()