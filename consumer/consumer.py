from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time
import json
import os
import pandas as pd 
from datetime import datetime 
import db_sink
from sqlalchemy import create_engine

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

db_url = 'postgresql://root:root@data-pipe-postgres-1:5432/sensor_db'

# Create database connection
db_engine = db_sink.create_db_connection(db_url)

sensor_dfs = {}


# Consume messages
for message in consumer:
    data = message.value
    device_id = data['deviceId']
    for entry in data['payload']:
        sensor_name = entry['name']
        time_stamp = entry['time']
        readable_time = datetime.fromtimestamp(time_stamp / 1_000_000_000)
        values = entry['values']

        # Create a DataFrame for this entry
        row_df = pd.DataFrame([{'deviceId': device_id, 'time': readable_time, **values}])

        # If this sensor type doesn't have a DataFrame yet, create one
        if sensor_name not in sensor_dfs:
            sensor_dfs[sensor_name] = row_df
        else:
            # Concatenate the new DataFrame row with the sensor's DataFrame
            sensor_dfs[sensor_name] = pd.concat([sensor_dfs[sensor_name], row_df], ignore_index=True)

        
        # Optional: Print or log the updated master DataFrame
       
        for sensor_name, df in sensor_dfs.items():
            db_sink.insert_dataframe_to_db(df, sensor_name, db_engine)
  


        print("Updated Master DataFrame:")
        print("DataFrames inserted into database")