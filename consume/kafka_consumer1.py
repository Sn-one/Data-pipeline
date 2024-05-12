import json
from kafka import KafkaConsumer , KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import psycopg2
from psycopg2.extras import execute_values
from pydantic_settings import BaseSettings
import time

class Settings(BaseSettings):
    database_hostname: str
    database_port: str
    database_password: str
    database_name: str
    database_username: str
    kafka_bootstrap_servers: str = "kafka:9092"

    class Config:
        env_file = ".env_config"

settings = Settings()

def create_connection():
    try:
        conn = psycopg2.connect(
            host=settings.database_hostname,
            user=settings.database_username,
            password=settings.database_password,
            dbname=settings.database_name,
            port=settings.database_port
        )
        return conn
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return None

def insert_message_into_db(conn, message):
    insert_query = "INSERT INTO test (message) VALUES %s;"
    cursor = conn.cursor()
    try:
        execute_values(cursor, insert_query, [(message,)])
        conn.commit()
        print(f"Message inserted into database: {message}")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

def wait_for_kafka_connection(kafka_bootstrap_servers, max_retries=5, wait_time=5):
    retry_count = 0
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
            producer.close()
            print("Connected to Kafka!")
            return True
        except NoBrokersAvailable:
            print(f"Waiting for Kafka to become available... {retry_count + 1}/{max_retries}")
            time.sleep(wait_time)
            retry_count += 1
    return False

def wait_for_database_connection(settings, max_retries=5, wait_time=5):
    retry_count = 0
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(
                host=settings.database_hostname,
                user=settings.database_username,
                password=settings.database_password,
                dbname=settings.database_name,
                port=settings.database_port
            )
            conn.close()
            print("Connected to PostgreSQL database!")
            return True
        except OperationalError as e:
            print(f"Waiting for PostgreSQL database to become available... {retry_count + 1}/{max_retries}")
            print(f"Error: {e}")
            time.sleep(wait_time)
            retry_count += 1
    return False

def consume_messages(settings):
    if not wait_for_kafka_connection(settings.kafka_bootstrap_servers):
        return
    
    if not wait_for_database_connection(settings):
        return
    
    consumer = KafkaConsumer(
        'sensor_data',
        bootstrap_servers=settings.kafka_bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x  # Temporarily handle raw bytes
    )

    conn = create_connection()
    if not conn:
        logging.error("Database connection failed.")
        return

    logging.info("Starting to consume messages...")
    for message in consumer:
        try:
            # Attempt to deserialize JSON from the message value
            msg = json.loads(message.value.decode('utf-8'))
            logging.info(f"Received message: {msg}")
            insert_message_into_db(conn, str(msg))
        except json.JSONDecodeError as e:
            # Log an error and skip this message
            logging.error(f"Error decoding JSON: {message.value} - Error: {e}")
            continue
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            continue


if __name__ == "__main__":
    consume_messages(settings)
