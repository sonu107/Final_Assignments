from kafka import KafkaConsumer
import json

bootstrap_servers = 'localhost:9092'

topic = 'flight_delay_updates'

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         max_poll_interval_ms=60000,  # Set the max poll interval
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def consume_stock_index_data():
    for message in consumer:
        # Print received message
        print(f"Received message: {message.value}")


if __name__ == "__main__":
    consume_stock_index_data()
