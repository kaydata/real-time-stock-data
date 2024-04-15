import yfinance as yf
from confluent_kafka import Producer
import socket
import json
import time
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO)

def fetch_stock_price(symbol):
    """
    Fetch the latest stock price from Yahoo Finance.
    Returns the price or None if no data could be fetched.
    """
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="1m")
        return hist['Close'].iloc[-1]
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return None

def kafka_producer():
    """
    Create a Kafka producer and publish stock prices.
    """
    # Configuration for Kafka Producer
    conf = {'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'LPOFOXKBKDNHG4EU',
        'sasl.password': 'EVCVgmbjsY0kQwYp2LGSoIOD31ABILx0qEmzCSbnGAtvlEnmsup0kvtyvSe7Aj1H',
        'client.id': socket.gethostname()}
    producer = Producer(conf)
    topic = 'stock_prices'

    while True:
        price = fetch_stock_price(symbol)  # Fetch the latest price for Apple

        if price is not None:
            # Prepare the message for sending
            message = {'symbol': symbol, 'price': price}
            # Send the message to Kafka
            try:
                producer.produce(topic, key= symbol, value=json.dumps(message))
                producer.flush()
                logging.info(f"Sent data: {message}")
            except Exception as e:
                logging.error(f"Error sending message to Kafka: {e}")

        time.sleep(60)  # Sleep for a minute to rate limit the message sending

if __name__ == "__main__":
    kafka_producer()
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']  # Add more stock symbols as needed
    kafka_producer(symbols)
