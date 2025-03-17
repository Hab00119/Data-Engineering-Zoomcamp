import json
import pandas as pd
from kafka import KafkaProducer
from time import time
from functools import wraps

# Decorator to measure execution time
def measure_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time()
        result = func(*args, **kwargs)
        end_time = time()
        print(f'Took {(end_time - start_time):.2f} seconds')
        return result
    return wrapper

# JSON serializer function
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Kafka producer setup (optimized settings)
server = 'localhost:9092'
topic_name = "green-trips"

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer,
   # linger_ms=50,   # Reduce network calls by waiting up to 50ms before sending a batch
   # batch_size=32 * 1024  # Increase batch size to 32KB for efficiency
)

# Function to send data to Kafka
@measure_time
def send_data_to_kafka():
    # Define columns needed
    columns_needed = [
        'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID',
        'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount'
    ]

    # Load the dataset with only the necessary columns
    df = pd.read_csv(
        "/workspaces/Data-Engineering-Zoomcamp/06-streaming/green_tripdata_2019-10.csv.gz",
        usecols=columns_needed,
        compression='gzip'
    )

    # Convert dataframe to list of dictionaries
    messages = df.to_dict(orient='records')

    # Send messages in bulk
    for message in messages:
        producer.send(topic_name, value=message)

    # Ensure all messages are sent before exiting
    producer.flush()

# Run the function
if __name__ == "__main__":
    send_data_to_kafka()
