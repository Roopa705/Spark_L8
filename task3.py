from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a Spark session
spark = SparkSession.builder \
    .appName("RideSharingAnalytics-Task3") \
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

# Convert timestamp column to TimestampType and add a watermark
parsed_df = parsed_df.withColumn(
    "event_time", 
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

# Add watermark of 1 minute to handle late data
watermarked_df = parsed_df.withWatermark("event_time", "1 minute")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_df = watermarked_df.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).agg(
    _sum("fare_amount").alias("total_fare")
)

# Extract window start and end times as separate columns
result_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# Define a function to write each batch to a CSV file with column names
def write_windowed_batch_to_csv(batch_df, batch_id):
    """
    Save the batch DataFrame as a CSV file with headers included
    """
    output_path = f"outputs/task3/batch_{batch_id}"
    batch_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)
    print(f"Batch {batch_id} written to {output_path} with {batch_df.count()} rows")

# Use foreachBatch to apply the function to each micro-batch
query = result_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_windowed_batch_to_csv) \
    .option("checkpointLocation", "checkpoints/task3") \
    .trigger(processingTime="10 seconds") \
    .start()

# Wait for the streaming to finish
query.awaitTermination()