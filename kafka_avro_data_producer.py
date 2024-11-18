import datetime
import time
from time import sleep
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import mysql.connector
from decimal import Decimal
from io import BytesIO

# Callback for delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f'Record {msg.key()} successfully produced to {msg.topic()} ["{msg.partition()}] at offset {msg.offset()}')


# MySQL Configuration
mysql_config = {
    'host':'localhost',
    'user':'root',
    'password':'root',
    'database':'kafka_table'
}

# kafka configurations
kafka_config = {
    'bootstrap.servers':'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
    'sasl.mechanisms':'PLAIN',
    'security.protocol':'SASL_SSL',
    'sasl.username':'33BEUPLWOFRJXS2Q',
    'sasl.password':'tDxDKI/6G8CuP99VIaqnGEbfrdWXKUvu9XyA33YRAHN4rXOQ+A04v3C9ck5Rghtd'
}

# Schema Registry configuration
schema_registry_client = SchemaRegistryClient({
    'url':'https://psrc-3n6x0.southeastasia.azure.confluent.cloud',
    'basic.auth.user.info':'{}:{}'.format('IMAXUGVYDSFFI57F','51odHHIrS2dSEdoBoUgOzOd1USb8gHXwOsS73s44iEgCaeY2VV4vlZ1Jqt+SZBaD')
})


# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str


# Create Avro Serializer
key_serializer = StringSerializer('utf_8')
value_serializer = AvroSerializer(schema_registry_client, schema_str)

# Initialize Kafka Producer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms' : kafka_config['sasl.mechanisms'],
    'sasl.username' : kafka_config['sasl.username'],
    'sasl.password' : kafka_config['sasl.password'],
    'key.serializer' : key_serializer,
    'value.serializer' : value_serializer
})

#  Function to fetch data from MySQL
def fetch_data_from_mysql(last_read_timestamp):
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor(dictionary=True)

    query = """
        SELECT product_id, product_name, category, price, last_updated
        FROM products
        WHERE last_updated > %s
        ORDER BY last_updated
    """

    cursor.execute(query, (last_read_timestamp,))
    records = cursor.fetchall()
    cursor.close()
    connection.close()
    return records


#  Main function to produce data to Kafka
def produce_to_kafka():
    last_read_timestamp = datetime.datetime.min.strftime('%Y-%m-%d %H:%M:%S')

    while True:
        records = fetch_data_from_mysql(last_read_timestamp)

        if not records:
            print("No new records found.")
            sleep(10)
            continue

        # Publish each record to Kafka 
        for record in records:
            try:
                # Set the last_read_timestamp to the last record's updated time
                last_read_timestamp = record['last_updated']



                # Send the record to Kafka
                producer.produce(
                    topic='product_updates',
                    key=str(record['product_id']),
                    value=record,
                    on_delivery=delivery_report
                )
                # Ensure the message is sent
                producer.flush()

            except Exception as e:
                print(f"Failed to send record: {e}")

        
        
        # Wait before next fetch
        time.sleep(5)

        

if __name__ == '__main__':
    produce_to_kafka()