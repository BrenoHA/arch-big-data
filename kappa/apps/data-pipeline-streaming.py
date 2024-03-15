"""
Demo Spark Structured Streaming + Apache Kafka + Cassandra
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType, FloatType, StringType
from pyspark.sql.functions import split,sum,from_json,col


import sys

def writeToCassandra(df, epochId):
    df.write \
        .pprint()
#    df.write \
#        .format("org.apache.spark.sql.cassandra") \
#        .options(table="transactions", keyspace="demo") \
#        .mode("append") \
#        .save()

def main():

    capteurSchema = StructType([
                StructField("date",StringType(),False),
                StructField("numero",IntegerType(),False),
                StructField("capteur",StringType(),False),
                StructField("valeur",FloatType(),False)
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

    df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),capteurSchema).alias("data")).select("data.*")
    df1.printSchema()

    #query = """
    #    SELECT FROM_JSON(
    #            CAST(value AS STRING), 'date TEXT, numero INT, capteur TEXT, valeur FLOAT'
    #        ) AS json_struct 
    #    FROM tmp_table
    #"""


    df1.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()

#    spark.sql(query) \
#        .select("json_struct.*") \
#        .groupBy("numero","capteur").agg(mean("valeur").alias("valeur")) \
#        .option("spark.cassandra.connection.host","cassandra1:9042")\
#        .foreachBatch(writeToCassandra) \
#        .outputMode("update") \
#        .trigger(processingTime='10 seconds') \
#        .start() \
#        .awaitTermination()
    
#    spark.sql(query) \
##        .select("json_struct.*") \
#       .groupBy("numero","capteur").agg(mean("valeur").alias("valeur")) \
#        .writeStream \
#        .foreachBatch(writeToCassandra) \
#        .outputMode("update") \
#        .trigger(processingTime='10 seconds') \
#        .start() \
#        .awaitTermination()


#    spark.sql(query) \
##        .select("json_struct.*") \
#       .groupBy("numero","capteur").agg(mean("valeur").alias("valeur")) \
#        .writeStream \
#        .foreachBatch(writeToCassandra) \
#        .outputMode("update") \
#        .trigger(processingTime='10 seconds') \
#        .start() \
#        .awaitTermination()


if __name__ == "__main__":
    main()













