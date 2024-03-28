from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, variance, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("WeatherDataBatchProcessing") \
    .getOrCreate()

# Define schema for the JSON data
schema = StructType([
    StructField("date", StringType(), True),
    StructField("city", StringType(), True),
    StructField("lon", FloatType(), True),
    StructField("lat", FloatType(), True),
    StructField("temperature", FloatType(), True),
    StructField("feels_like", FloatType(), True)
])

# Read data from JSON file into DataFrame
df = spark.read.json("data.json", schema=schema)

# Filter data for the last 5 minutes
five_minutes_ago = current_timestamp() - expr("INTERVAL 5 MINUTES")
df_filtered = df.filter(df.date >= five_minutes_ago)

# Calculate mean, variance, and the difference between feels_like and temperature grouped by city
result = df_filtered.groupBy("city").agg(
    avg("temperature").alias("avg_temperature"),
    variance("temperature").alias("variance_temperature"),
    avg("feels_like").alias("avg_feels_like"),
    variance("feels_like").alias("variance_feels_like"),
    (avg("feels_like") - avg("temperature")).alias("diff_feels_like_temperature")
)

# Show the result
result.show()

# Stop the SparkSession
spark.stop()
