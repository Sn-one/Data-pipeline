from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import sys
# Import database functions from test_db.py
from test_db import create_connection, create_test_table, insert_into_test_table

app = Flask(__name__)

def create_kafka_producer(retries=5, wait=5):
    while retries > 0:
        try:
            return KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except NoBrokersAvailable:
            retries -= 1
            time.sleep(wait)
    raise Exception("Kafka broker not available")

producer = create_kafka_producer()

@app.route('/data', methods=['POST'])
def send_to_kafka():
    data = request.get_json()
    try:
        producer.send('sensor_data', value=data)
        print("Message sent to Kafka", file=sys.stdout)
        return jsonify({"status": "sent to Kafka"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/test_db', methods=['GET'])
def test_db_operations():
    # Create a database connection
    conn = create_connection()
    if conn is not None:
        create_test_table(conn)
        insert_into_test_table(conn, "Hello, this is a test message from Flask.")
        return jsonify({"status": "Database operations successful"}), 200
    else:
        return jsonify({"status": "Database connection failed"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
