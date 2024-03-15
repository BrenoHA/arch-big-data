import json
import time
import urllib.request
import sys, time, json, random
from datetime import datetime
from kafka import KafkaProducer, KafkaClient 
from kafka.cluster import ClusterMetadata
from kafka.admin import KafkaAdminClient, NewTopic


def main():


    API_KEY = "8e547fa511d87582d153fb74c1305ea4ba6ea9b3" # FIXME Set your own API key here
    url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

    topic = sys.argv[1]
    
    kafka_client = KafkaClient(bootstrap_servers='kafka:29092')
    
    clusterMetadata = kafka_client.cluster
    server_topics = clusterMetadata.topics()

    if topic not in server_topics:
        try:
            print("create new topic velib")
            admin = KafkaAdminClient(bootstrap_servers='kafka:29092')

            topic1 = NewTopic(name=topic,
                             num_partitions=1,
                             replication_factor=1)
            admin.create_topics([topic1])
        except Exception:
            pass

    producer = KafkaProducer(bootstrap_servers="kafka:29092")

    while True:
        response = urllib.request.urlopen(url)
        stations = json.loads(response.read().decode())
        print(len(stations))
        for station in stations:
            producer.send("velib-stations", json.dumps(station).encode())
            
        print("{} Produced {} station records".format(time.time(), len(stations)))
        time.sleep(10)

if __name__ == "__main__":
    main()
