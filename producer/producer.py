import yfinance as yf
from confluent_kafka import Producer
import socket
import json
import time
import logging
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Set up basic logging
logging.basicConfig(level=logging.INFO)

def fetch_stock_price(symbol):
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="1m")
        return hist['Close'].iloc[-1]
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return None

def kafka_producer(symbols):
    # Configuration for Kafka Producer to connect to Confluent Cloud
    conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
        'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
        'client.id': socket.gethostname()
    }

    producer = Producer(conf)
    topic = 'stock_prices'

    while True:
        for symbol in symbols:
            price = fetch_stock_price(symbol)
            if price is not None:
                message = {'symbol': symbol, 'price': price}
                try:
                    producer.produce(topic, key=symbol, value=json.dumps(message))
                    producer.flush()
                    logging.info(f"Sent data: {message}")
                except Exception as e:
                    logging.error(f"Error sending message to Kafka: {e}")
        time.sleep(60)

if __name__ == "__main__":
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    kafka_producer(symbols)
