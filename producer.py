from kafka import KafkaProducer
import requests
import json
import time


# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'flight_delay_updates'

# Alpha Vantage API configuration
#api_key = 'JHP9POA79NOQ1AUB'
#symbol = 'AAPL'  # Example stock symbol

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Infinite loop to fetch stock prices and send to Kafka
while True:
    try:
        # Fetch stock data from Alpha Vantage
        response = requests.post(f'http://127.0.0.1:8000/flight_details')

        data = response.json()
    

        # Send stock price to Kafka
        producer.send(topic_name, value={'data': data})

        # Wait for some time before fetching the next price
        time.sleep(30)  # Adjust as needed

    except Exception as e:
        print(f'Error fetching or sending flight data: {str(e)}')