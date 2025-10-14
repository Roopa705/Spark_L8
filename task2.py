from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a Spark session
spark = SparkSession.builder \
    .appName("RideSharingAnalytics-Task2") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into columns using the defined schema
parsed_df = df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Compute aggregations: total fare and average distance grouped by driver_id
aggregated_df = parsed_df.groupBy("driver_id").agg(
    _sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Define a function to write each batch to a CSV file
def write_batch_to_csv(batch_df, batch_id):
    """
    Save the batch DataFrame as a CSV file with the batch ID in the filename
    """
    output_path = f"outputs/task2/batch_{batch_id}"
    batch_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)
    print(f"Batch {batch_id} written to {output_path}")

# Use foreachBatch to apply the function to each micro-batch
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch_to_csv) \
    .option("checkpointLocation", "checkpoints/task2") \
    .start()

# Wait for the streaming to finish
query.awaitTermination()