#!/usr/bin/python 

import sys, time, json, random
from kafka import KafkaProducer, KafkaClient 
from kafka.cluster import ClusterMetadata
from kafka.admin import KafkaAdminClient, NewTopic

def generate_random_event():
    x = {
        "client": random.randrange(1, 6),
        "amount": random.randrange(-500, 500, 50)
    }
    return x

def produce_to_kafka(data, topic):
    producer = KafkaProducer(
        bootstrap_servers=["kafka:9092"], 
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    producer.send(topic, data)
    producer.flush()

def main():
    topic = sys.argv[1]

    kafka_client = KafkaClient(bootstrap_servers='kafka:29092')

    clusterMetadata = kafka_client.cluster
    server_topics = clusterMetadata.topics()

    if topic not in server_topics:
        try:
            print("create new topic :", topic)
            admin = KafkaAdminClient(bootstrap_servers='kafka:29092')

            topic1 = NewTopic(name=topic,
                             num_partitions=1,
                             replication_factor=1)
            admin.create_topics([topic1])
        except Exception:
            pass

    while True:
        data = generate_random_event()
        print(data)
        produce_to_kafka(data, topic)
        time.sleep(random.randrange(1, 6))

if __name__ == "__main__":
    main()