# Kafka Avro Consumer Project

## Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Kafka Configuration](#kafka-configuration)
- [Schema Registry Configuration](#schema-registry-configuration)
- [Data Transformation](#data-transformation)
- [Setup and Installation](#setup-and-installation)
- [Running the Consumer](#running-the-consumer)
- [Multi-threading](#multi-threading)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## Project Overview
This project is a **Kafka Avro Consumer** that consumes messages from a Kafka topic using the **Confluent Kafka Python Client**. The messages are deserialized using Avro schema from Confluent Schema Registry, transformed based on business rules, and then saved as JSON records to a local file.

### Features
- Connects to a Kafka cluster using SASL_SSL authentication.
- Fetches Avro schemas dynamically from Confluent Schema Registry.
- Consumes and deserializes Kafka messages using Avro deserializer.
- Applies data transformation, such as converting categories to uppercase and applying discounts.
- Supports multi-threaded consumers for higher throughput.
- Saves transformed data to a JSON file.

### Technologies Used
- **Python**: Core programming language.
- **Confluent Kafka Python Client**: For Kafka integration.
- **Schema Registry**: For Avro schema management.
- **Multi-threading**: To run multiple consumers in parallel.

## Kafka Configuration
```python
kafka_config = {
    'bootstrap.servers': 'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '<your_kafka_username>',
    'sasl.password': '<your_kafka_password>',
    'group.id': 'product_update_group',
    'auto.offset.reset': 'earliest'
}
