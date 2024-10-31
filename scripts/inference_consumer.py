# iot_consumer.py
#
# Author: Team 14 (Maddox, Emma, Abhay)
# CS4287-5287: Principles of Cloud Computing, Vanderbilt University
#
#
# Purpose: iot consumer file that gathers images from kafka and sends them to the ml server. Inference responses
# from the server are sent back to kafka and placed in the 'inferences' configuration topic. db_consumer.py will
# pull data from these topics for the database

import json
import requests
from kafka import KafkaConsumer, KafkaProducer
import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka-service:9092')
IOT_IMAGES_TOPIC = os.environ.get('IOT_IMAGES_TOPIC', 'iot-images')
INFERENCE_RESULT_TOPIC = os.environ.get('INFERENCE_RESULT_TOPIC', 'inference-result')
ML_SERVER_URL = os.environ.get('ML_SERVER_URL', 'http://ml-inference-service:5000/infer')

# Kafka Consumer Configuration
consumer = KafkaConsumer(
    IOT_IMAGES_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Kafka Producer Configuration for inferences
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1
)


for msg in consumer:
    message = msg.value
    print(f"Image {message['ID']} ground truth: {message['GroundTruth']}")

    response = requests.post(ML_SERVER_URL, json={
        'ID': message['ID'],
        'Data': message['Data']
    })

    if response.status_code == 200:
        inferred_value = response.json()['inference']
        print(f"Image {message['ID']} retrieved and labeled as: {inferred_value}")

        # Create new message for the 'inference-result' topic
        inference_message = {
            'ID': message['ID'],
            'GroundTruth': message['GroundTruth'],
            'Inference': inferred_value
        }

        # Send the inference to the 'inference-result' Kafka topic
        producer.send('inference-result', value=inference_message)
        print(f"Inference for image {message['ID']} sent to Kafka 'inference-result' topic")
    else:
        print(f"Failed to retrieve label for image {message['ID']}")

consumer.close()