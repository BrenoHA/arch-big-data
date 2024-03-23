from pyspark import SparkContext
from datetime import datetime, timedelta

# Create the SparkContext
sc = SparkContext("local", "TemperatureMean")

# Set log level to error
sc.setLogLevel("ERROR")

# Sample data - Replace this with your Cassandra data reading logic
temperature_data = [
    ("Lyon", "2024-03-23 14:22:18", 45.7578, 4.832, 12.7),
    ("Bordeaux", "2024-03-23 14:22:22", 44.8378, -0.5792, 17.5),
    ("Toulouse", "2024-03-23 14:22:09", 43.6045, 1.4442, 12.1),
    ("Strasbourg", "2024-03-23 14:22:29", 48.5734, 7.7521, 20.5),
    ("Lille", "2024-03-23 14:22:19", 50.6292, 3.0573, 10.7),
    ("Rennes", "2024-03-23 14:22:15", 48.1173, -1.6778, 15.8),
    ("Lyon", "2024-03-23 14:22:28", 45.7578, 4.832, 14.2)
]

# Simulated current time
current_time = datetime.strptime("2024-03-23 14:25:00", "%Y-%m-%d %H:%M:%S")

# Filter records within the last 5 minutes
five_minutes_ago = current_time - timedelta(minutes=5)
filtered_data = filter(lambda x: datetime.strptime(x[1], "%Y-%m-%d %H:%M:%S") >= five_minutes_ago, temperature_data)

# Map to (city, temperature) pairs
city_temperature_pairs = map(lambda x: (x[0], x[4]), filtered_data)

# Group by city and calculate mean temperature
city_mean_temperature = sc.parallelize(city_temperature_pairs) \
    .mapValues(lambda x: (x, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: x[0] / x[1]) \
    .collect()

# Print results
for city, mean_temp in city_mean_temperature:
    print(f"{city}: {mean_temp}°C")

# Stop the SparkContext
sc.stop()