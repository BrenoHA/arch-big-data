from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder \
.appName("Spark Structured Streaming from Kafka") \
.getOrCreate()

sdfRides = spark \
.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "pi-node09:9092") \
.option("subscribe", "taxirides") \
.option("startingOffsets", "latest") \
.load() \
.selectExpr("CAST(value AS STRING)")

sdfFares = spark \
.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "pi-node09:9092") \
.option("subscribe", "taxifares") \
.option("startingOffsets", "latest") \
.load() \
.selectExpr("CAST(value AS STRING)")

from pyspark.sql.types import *

taxiFaresSchema = StructType([ \
StructField("rideId", LongType()), StructField("taxiId",
LongType()), \
StructField("driverId", LongType()), StructField("startTime",
TimestampType()), \
StructField("paymentType", StringType()), StructField("tip",
FloatType()), \
StructField("tolls", FloatType()), StructField("totalFare",
FloatType())])

taxiRidesSchema = StructType([ \
StructField("rideId", LongType()), StructField("isStart",
StringType()), \
StructField("endTime", TimestampType()),
StructField("startTime", TimestampType()), \
StructField("startLon", FloatType()), StructField("startLat",
FloatType()), \
StructField("endLon", FloatType()), StructField("endLat",
FloatType()), \
StructField("passengerCnt", ShortType()), StructField("taxiId",
LongType()), \
StructField("driverId", LongType())])

def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    col = split(sdf['value'], ',') #split attributes to nested array in one Column
    #now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf.select([field.name for field in schema])

sdfRides = parse_data_from_kafka_message(sdfRides, taxiRidesSchema)
sdfFares = parse_data_from_kafka_message(sdfFares, taxiFaresSchema)

LON_EAST, LON_WEST, LAT_NORTH, LAT_SOUTH = -73.7, -74.05, 41.0, 40.5
sdfRides = sdfRides.filter( \
sdfRides["startLon"].between(LON_WEST, LON_EAST) & \
sdfRides["startLat"].between(LAT_SOUTH, LAT_NORTH) & \
sdfRides["endLon"].between(LON_WEST, LON_EAST) & \
sdfRides["endLat"].between(LAT_SOUTH, LAT_NORTH))
# Notice that rides with faulty geospatial data as e.g. (0, 0) are filtered out also
sdfRides = sdfRides.filter(sdfRides["isStart"] == "END") #Keep only finished!

# Apply watermarks on event-time columns
sdfFaresWithWatermark = sdfFares \
.selectExpr("rideId AS rideId_fares", "startTime", "totalFare",
"tip") \
.withWatermark("startTime", "30 minutes") # maximal delay

sdfRidesWithWatermark = sdfRides \
.selectExpr("rideId", "endTime", "driverId", "taxiId", \
"startLon", "startLat", "endLon", "endLat") \
.withWatermark("endTime", "30 minutes") # maximal delay

# Join with event-time constraints
sdf = sdfFaresWithWatermark \
.join(sdfRidesWithWatermark, \
expr("""
rideId_fares = rideId AND
endTime > startTime AND
endTime <= startTime + interval 2 hours
"""))

query = sdfRides.groupBy("driverId").count()

query.writeStream \
.outputMode("complete") \
.format("console") \
.option("truncate", False) \
.start() \
.awaitTermination()
