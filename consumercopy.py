from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time
import json
import os

def create_kafka_consumer(topic_name, bootstrap_servers, retries=5, wait_time=5):
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Kafka Consumer connection established")
            return consumer
        except KafkaError as e:
            print(f"Failed to connect to Kafka ({e}), retrying... {retries} attempts left")
            retries -= 1
            time.sleep(wait_time)

    raise Exception("Failed to connect to Kafka after several attempts")

# Replace with your topic and Kafka server details
topic_name = 'sensor_data'
bootstrap_servers = ['kafka:9092']

# Create the consumer
consumer = create_kafka_consumer(topic_name, bootstrap_servers)




# Consume messages
for message in consumer:
    print(f"{message.value}")


