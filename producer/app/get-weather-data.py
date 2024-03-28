import json 
import time 
import sys
from datetime import datetime
from kafka import KafkaProducer, KafkaClient 
from kafka.cluster import ClusterMetadata
from kafka.admin import KafkaAdminClient, NewTopic
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access your variable
api_key = os.getenv('API_KEY')

city_coordinates = {
    "Paris": (48.8566, 2.3522),
    "Marseille": (43.2965, 5.3698),
    "Lyon": (45.7578, 4.832),
    "Toulouse": (43.6045, 1.4442),
    "Nice": (43.7102, 7.262),
    "Nantes": (47.2184, -1.5536),
    "Montpellier": (43.6109, 3.8772),
    "Strasbourg": (48.5734, 7.7521),
    "Bordeaux": (44.8378, -0.5792),
    "Lille": (50.6292, 3.0573),
    "Brest": (48.3904, -4.4861),
    "Rennes": (48.1173, -1.6778)
}

def get_weather_data(lat, lon, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching weather data for lat={lat} lon={lon}: {e}")
        return None

def produce_to_kafka(topic, data):
    producer = KafkaProducer(
        bootstrap_servers=["kafka:29092"], 
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    producer.send(topic, data)
    producer.flush()

def main():
    topic = "kafka-weather-events"

    kafka_client = KafkaClient(bootstrap_servers='kafka:29092')

    cluster_metadata = kafka_client.cluster
    server_topics = cluster_metadata.topics()

    if topic not in server_topics:
        try:
            print("Creating new topic:", topic)
            admin = KafkaAdminClient(bootstrap_servers='kafka:29092')

            topic1 = NewTopic(name=topic,
                             num_partitions=1,
                             replication_factor=1)
            admin.create_topics([topic1])
        except Exception:
            pass

    producer = KafkaProducer(bootstrap_servers="kafka:29092")

    while True: 

        # Get weather data for each city
        for city, coordinates in city_coordinates.items():
            latitude, longitude = coordinates
            
            weather_data = get_weather_data(latitude, longitude, api_key)
            weather_info = {
                "city": city,
                "coord": weather_data["coord"],
                "main": weather_data["main"]
            }
            produce_to_kafka("kafka-weather-events", weather_info)
            print(weather_info)

        time.sleep(10)

if __name__ == "__main__":
    main()
