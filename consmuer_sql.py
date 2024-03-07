from kafka import KafkaConsumer
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, DateTime
from sqlalchemy.exc import ProgrammingError
import json

database = "flight_data"  
username = "postgres"
password = "9066"
host = "localhost"
port = '5433'

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'flight_delay_updates'

# SQL database configuration
db_uri = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

# Create SQLAlchemy engine
engine = create_engine(db_uri)

# Define SQL table schema
metadata = MetaData()
flight_table = Table(
    'flights',
    metadata,
    Column('id', Integer),
    Column('flight_number', String),
    Column('departure_city', String),
    Column('arrival_city', String),
    Column('departure_time', DateTime),
    Column('arrival_time', DateTime),
    Column('status', String),
    Column('gate', String)
)

# Create SQL table if not exists
metadata.create_all(engine, checkfirst=True)

def consume_and_write_to_sql():
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    for message in consumer:
        data = message.value['data']
        try:
            # Insert message data into SQL table
            with engine.connect() as conn:
                conn.execute(flight_table.insert().values(
                    flight_number=data['flight_number'],
                    departure_city=data['departure_city'],
                    arrival_city=data['arrival_city'],
                    departure_time=data['departure_time'],
                    arrival_time=data['arrival_time'],
                    status=data['status'],
                    gate=data['gate']
                ))
            print("Message written to SQL table successfully.")
        except ProgrammingError as e:
            print(f"Error writing message to SQL table: {e}")

if __name__ == "__main__":
    consume_and_write_to_sql()
