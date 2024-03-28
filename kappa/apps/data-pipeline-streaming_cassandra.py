from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType
from pyspark.sql.functions import split, from_json, col

import sys

def main():
    # Define the schema for the weather data
    weatherSchema = StructType([
        StructField("date", StringType(), False),
        StructField("city", StringType(), False),
        StructField("lat", FloatType(), False),
        StructField("lon", FloatType(), False),
        StructField("temperature", FloatType(), False),
        StructField("feels_like", FloatType(), False)
    ])

    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    # Set log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    # Get the Kafka topic from command-line arguments
    topic = sys.argv[1]

    # Read streaming data from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic) \
        .option("delimiter", ",") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON data and select required fields
    df1 = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), weatherSchema).alias("data")) \
            .select("data.*")

    # Calculate the difference between temperature and feels_like
    df1 = df1.withColumn("temperature_minus_feels_like", col("temperature") - col("feels_like"))

    # Print the schema of the DataFrame
    df1.printSchema()

    # Function to write data to Cassandra
    def writeToCassandra(writeDF, epochId):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="weather_table", keyspace="weather_keyspace") \
            .save()

    # Write the data to Cassandra
    df1.writeStream \
        .option("spark.cassandra.connection.host", "cassandra1:9042") \
        .foreachBatch(writeToCassandra) \
        .outputMode("update") \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    main()
