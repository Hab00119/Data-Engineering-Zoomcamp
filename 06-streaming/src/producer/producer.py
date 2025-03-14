import json
import time
from kafka import KafkaProducer
from functools import wraps


# Decorator to measure execution time
def measure_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f'Took {(end_time - start_time):.2f} seconds')
        return result
    return wrapper


# JSON serializer function
def json_serializer(data):
    return json.dumps(data).encode('utf-8')


# Kafka broker (change if running on a different host)
KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "test-topic"

# Initialize Kafka Producer with optimized settings
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=json_serializer,
    linger_ms=10,  # Wait up to 10ms before sending a batch
    batch_size=16384,  # Batch size of 16KB (default Kafka setting)
)


# Callback function for successful message delivery
def on_success(metadata, message):
    print(f"✔ Message {message} sent to {metadata.topic} | Partition {metadata.partition} | Offset {metadata.offset}")


# Callback function for failed message delivery
def on_error(exception, message):
    print(f"❌ Error sending message {message}: {exception}")


# Function to produce messages
@measure_time
def produce_messages():
    for i in range(1, 1000):
        message = {"test_data": i, "event_timestamp": time.time() * 1000}
        #producer.send(TOPIC_NAME, value=message).add_callback(on_success).add_errback(on_error)
        future = producer.send(TOPIC_NAME, value=message)
        future.add_callback(lambda metadata, msg=message: on_success(metadata, msg))
        future.add_errback(lambda exception, msg=message: on_error(exception, msg))
    
    # Ensure all messages are sent before exiting
    producer.flush()


# Run producer
if __name__ == "__main__":
    produce_messages()
