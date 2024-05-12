import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import OperationalError
from pydantic_settings import BaseSettings

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    """Create and return a new database connection using the settings from the environment."""
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
        logging.error(f"Error creating database connection: {e}")
        return None

def insert_message_into_db(conn, message):
    """Insert a message into the database."""
    insert_query = "INSERT INTO test (message) VALUES %s;"
    cursor = conn.cursor()
    try:
        execute_values(cursor, insert_query, [(message,)])
        conn.commit()
        logging.info(f"Message inserted into database: {message}")
    except (Exception, psycopg2.DatabaseError) as e:
        logging.error(f"Error inserting message into database: {e}")

def wait_for_kafka_connection(kafka_bootstrap_servers, max_retries=5, wait_time=5):
    """Wait for Kafka to become available, retrying connection attempts."""
    retry_count = 0
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
            producer.close()
            logging.info("Successfully connected to Kafka!")
            return True
        except NoBrokersAvailable:
            logging.warning(f"Waiting for Kafka to become available... Attempt {retry_count + 1}/{max_retries}")
            time.sleep(wait_time)
            retry_count += 1
    logging.error("Failed to connect to Kafka after several retries.")
    return False

def wait_for_database_connection(settings, max_retries=5, wait_time=5):
    """Wait for the database to become available, retrying connection attempts."""
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
            logging.info("Successfully connected to PostgreSQL database!")
            return True
        except OperationalError as e:
            logging.warning(f"Waiting for PostgreSQL database to become available... Attempt {retry_count + 1}/{max_retries}")
            logging.error(f"Error: {e}")
            time.sleep(wait_time)
            retry_count += 1
    logging.error("Failed to connect to PostgreSQL database after several retries.")
    return False

def safe_json_deserializer(message_value):
    """Safely deserialize JSON messages."""
    try:
        return json.loads(message_value.decode('utf-8'))
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON: {message_value}")
        return None

def consume_messages(settings):
    """Consume messages from Kafka and process them."""
    if not wait_for_kafka_connection(settings.kafka_bootstrap_servers):
        return
    
    if not wait_for_database_connection(settings):
        return
    
    consumer = KafkaConsumer(
        'sensor_data',
        bootstrap_servers=settings.kafka_bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=safe_json_deserializer
    )

    conn = create_connection()
    if not conn:
        logging.error("Database connection failed. Exiting consumer.")
        return

    logging.info("Starting to consume messages...")
    for message in consumer:
        msg = message.value
        if msg is None:
            logging.warning("Received a null or non-JSON message, skipping...")
            continue
        logging.info(f"Received message: {msg}")
        insert_message_into_db(conn, json.dumps(msg))

if __name__ == "__main__":
    consume_messages(settings)
