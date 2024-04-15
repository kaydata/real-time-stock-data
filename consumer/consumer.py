from confluent_kafka import Consumer, KafkaError
import json
import logging
import redis
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Set up basic logging
logging.basicConfig(level=logging.INFO)

# Initialize Redis
r = redis.Redis()

def kafka_consumer():
    """
    Create a Kafka consumer and subscribe to the topic.
    """
    # Configuration for Kafka Consumer to connect to Confluent Cloud
    conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
        'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
        'group.id': 'stock_price_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    topic = 'stock_prices'

    try:
        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for messages

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logging.info('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaError(msg.error())
            else:
                # Message is a normal message
                message = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Received message: {message}")
                publish_message(json.dumps(message))  # Store the message in Redis
    finally:
        # Clean up on exit
        consumer.close()

def publish_message(message):
    """
    Publish the message to Redis for real-time visualization.
    """
    r.lpush('stock_prices', message)
    r.ltrim('stock_prices', 0, 49)  # Keep only the latest 50 entries

if __name__ == "__main__":
    kafka_consumer()
