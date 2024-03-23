"""
Demo Spark Structured Streaming + Apache Kafka + Cassandra
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType
from pyspark.sql.functions import split,sum,from_json,col


import sys

def main():

    weatherSchema = StructType([
                StructField("date",StringType(),False),
                StructField("city",StringType(),False),
                StructField("lat",FloatType(),False),
                StructField("lon",FloatType(),False),
                StructField("temperature",FloatType(),False)
            ])

    spark = SparkSession.builder \
        .appName("Spark-Kafka-Cassandra") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    topic = sys.argv[1]

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic) \
        .option("delimeter",",") \
        .option("startingOffsets", "latest") \
        .load()

    df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),weatherSchema).alias("data")).select("data.*")
    df1.printSchema()

    def writeToCassandra(writeDF, epochId):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="weather_table", keyspace="weather_keyspace")\
            .save()

    df1.writeStream \
        .option("spark.cassandra.connection.host","cassandra1:9042")\
        .foreachBatch(writeToCassandra) \
        .outputMode("update") \
        .start()\
        .awaitTermination()
    
    def writeToHDFS(df, epochId):
        df.write \
            .format("parquet") \
            .mode("append") \
            .save("hdfs://hadoop-master:9000/hadoop/dfs/data")
    
    hdfs_writer = DataStreamWriter(weather_df.writeStream)
    hdfs_writer \
        .option("checkpointLocation", "/tmp/hdfs_checkpoint") \
        .foreachBatch(writeToHDFS) \
        .outputMode("append") \
        .start() \
        .awaitTermination()
   

if __name__ == "__main__":
    main()













