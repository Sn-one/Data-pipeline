from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import psycopg2
import pandas as pd
import time



def create_kafka_consumer(topic_name, bootstrap_servers, retries=5, wait_time=5):
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Kafka Consumer connection established")
            return consumer
        except KafkaError:
            retries -= 1
            print(f"Failed to connect to Kafka, retrying... {retries} attempts left")
            time.sleep(wait_time)

    raise Exception("Failed to connect to Kafka after several attempts")

topic_name = 'sensor_data'
bootstrap_servers = ['kafka:9092']  
consumer = create_kafka_consumer(topic_name, bootstrap_servers)



# Define the validate_and_transform function (as previously described)
def validate_and_transform(record):
    # Define a template for the expected structure
    expected_structure = {
        'messageId': int,
        'sessionId': str,
        'deviceId': str,
        'payload': {
            'name': str,
            'time': int,
            'values': dict  # This will be further validated based on the sensor type
        }
    }

    # Function to validate each field
    def validate_field(data, key, expected_type):
        if key not in data or not isinstance(data[key], expected_type):
            raise ValueError(f"Invalid or missing field: {key}")
        return data[key]

    # Validate the top-level structure
    message_id = validate_field(record, 'messageId', int)
    session_id = validate_field(record, 'sessionId', str)
    device_id = validate_field(record, 'deviceId', str)
    payload = validate_field(record, 'payload', dict)

    # Validate the payload
    sensor_name = validate_field(payload, 'name', str)
    sensor_time = validate_field(payload, 'time', int)
    sensor_values = validate_field(payload, 'values', dict)

    # Flatten and transform the payload for DataFrame compatibility
    flattened_record = {
        'messageId': message_id,
        'sessionId': session_id,
        'deviceId': device_id,
        'sensorName': sensor_name,
        'sensorTime': sensor_time
    }

    # Add sensor values to the flattened record
    for key, value in sensor_values.items():
        flattened_record[f'value_{key}'] = value

    return flattened_record

def transform_data(record):
    # Create an empty DataFrame to store the transformed data
    df_transformed = pd.DataFrame(columns=[
        "Timestamp (s)", "Latitude", "Longitude", "Altitude (m)", 
        "Accelerometer X (m/s²)", "Accelerometer Y (m/s²)", "Accelerometer Z (m/s²)", 
        "Gyroscope X (rad/s)", "Gyroscope Y (rad/s)", "Gyroscope Z (rad/s)"
    ])

    for sensor in record:
        if sensor['name'] == 'location':
            # Convert timestamp to seconds and round off to 1 decimal place
            timestamp = round(sensor['time'] / 1_000_000_000_000, 1)  
            latitude = sensor['values']['latitude']
            longitude = sensor['values']['longitude']
            altitude = sensor['values'].get('altitude', 0)

            # Append to DataFrame
            df_transformed = df_transformed.append({
                "Timestamp (s)": timestamp, "Latitude": latitude, "Longitude": longitude, 
                "Altitude (m)": altitude
            }, ignore_index=True)

        elif sensor['name'] in ['accelerometer', 'gyroscope']:
            timestamp = round(sensor['time'] / 1_000_000_000_000, 1)  
            x = sensor['values']['x']
            y = sensor['values']['y']
            z = sensor['values']['z']

            # Set the appropriate column names based on sensor type
            prefix = sensor['name'].capitalize()
            columns = {
                "Accelerometer X (m/s²)": f"{prefix} X (m/s²)",
                "Accelerometer Y (m/s²)": f"{prefix} Y (m/s²)",
                "Accelerometer Z (m/s²)": f"{prefix} Z (m/s²)"
            }

            # Append to DataFrame
            df_transformed = df_transformed.append({
                "Timestamp (s)": timestamp, columns["Accelerometer X (m/s²)"]: x,
                columns["Accelerometer Y (m/s²)"]: y, columns["Accelerometer Z (m/s²)"]: z
            }, ignore_index=True)

    # Combine rows by Timestamp
    df_combined = df_transformed.groupby('Timestamp (s)').first().reset_index()

    return df_combined




# PostgreSQL connection setup
conn = psycopg2.connect(
    host="data-pipe-postgres-1",
    database="sensor_db",
    user="root",
    password="root"
)
cursor = conn.cursor()

# Processing messages from Kafka
for message in consumer:
    try:
        record = validate_and_transform(message.value)
        df = pd.DataFrame([record])
        df_transformed = transform_data(df)

        # Insert into PostgreSQL
        df_transformed.to_sql('sensor_data', conn, if_exists='append', index=False)
        conn.commit()
    except Exception as e:
        print(f"Error processing message: {e}")

# Clean up
cursor.close()
conn.close()
