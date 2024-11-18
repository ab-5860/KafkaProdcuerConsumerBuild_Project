import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import json

# Define Kafka configurations
kafka_config = {
    'bootstrap.servers':'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
    'sasl.mechanisms':'PLAIN',
    'security.protocol':'SASL_SSL',
    'sasl.username':'33BEUPLWOFRJXS2Q',
    'sasl.password':'tDxDKI/6G8CuP99VIaqnGEbfrdWXKUvu9XyA33YRAHN4rXOQ+A04v3C9ck5Rghtd',
    'group.id':'product_update_group',
    'auto.offset.reset':'earliest'
}


# Create a Schema Registry Client
schema_registry_client = SchemaRegistryClient({
    'url':'https://psrc-3n6x0.southeastasia.azure.confluent.cloud',
    'basic.auth.user.info':'{}:{}'.format('IMAXUGVYDSFFI57F','51odHHIrS2dSEdoBoUgOzOd1USb8gHXwOsS73s44iEgCaeY2VV4vlZ1Jqt+SZBaD')
})

# Fetch the latest Avro schema for the value
subject_name  = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str


# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client,schema_str)


# Define the DesrializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers' : kafka_config['bootstrap.servers'],
    'security.protocol' : kafka_config['security.protocol'],
    'sasl.mechanisms' : kafka_config['sasl.mechanisms'],
    'sasl.username' : kafka_config['sasl.username'],
    'sasl.password' : kafka_config['sasl.password'],
    'key.deserializer' : key_deserializer,
    'value.deserializer' : avro_deserializer,
    'group.id' : kafka_config['group.id'],
    'auto.offset.reset' : kafka_config['auto.offset.reset']
})



# Subscribe to the topic
consumer.subscribe(['product_updates'])

def transform_data(record):

    """
        Apply data transformation logic
        Convert category to uppercase and apply a discount on price
    """

    transformed_record = record.copy()

    # Convert the category to uppercase
    if 'category' in transformed_record:
        transformed_record['category'] = transformed_record['category'].upper()

    if 'last_updated' in transformed_record:
        transformed_record['last_updated'] = transformed_record['last_updated'].isoformat()

    # Apply discount logic (e.g 10% off for ELECTRONICS category)
    if transformed_record['category'] == 'ELECTRONICS':
        if 'price' in transformed_record:
            transformed_record['price'] = round(transformed_record['price']*0.9, 2)
    
    return transformed_record


def write_to_json_file(data, filename='transformed_data.json'):
    """
    Write the transformed data to JSON file
    Each record should be written on a new line
    """

    try:
        # open the file in append mode
        with open(filename, 'a') as json_file:
            json.dump(data, json_file)
            json_file.write('\n')
    except Exception as e:
        print(f"Error writing to JSON file: {e}")


# Function to consume messages
def consume_messages():
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Deserialize and transform the message value
            original_value = msg.value()

            if original_value is None:
                continue

            # Transform the data
            transformed_value = transform_data(original_value)

            print(f"Consumed record with key: {msg.key()}, original value: {original_value}, transformed value: {transformed_value}")

            # Write the transformed data to a JSON file
            write_to_json_file(transformed_value)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


# Run Consumers in thread
consumer_threads = []
for _ in range(5):    # Creating 5 consumers in the group
    thread = threading.Thread(target=consume_messages)
    consumer_threads.append(thread)
    thread.start()
    

# Wait for all threads to complete
for thread in consumer_threads:
    thread.join()