# iot_consumer.py
#
# Author: Team 14 (Maddox, Emma, Abhay)
# CS4287-5287: Principles of Cloud Computing, Vanderbilt University
#
#
# Purpose: iot producer file that sends images to the ml server and receives inference responses from the server.
# Inference responses are stored in the 'inference-result' topic and sent to the db_consumer.py file for storage in the database 

import time
import json
import random
import threading
import torchvision
import torchvision.transforms as transforms
from kafka import KafkaConsumer, KafkaProducer
import numpy as np
import cv2
import os
import sys
broker_ip = "192.168.5.241:9092"

if len(sys.argv) > 1:
    producer_id = sys.argv[1]
else:
    producer_id = 'producer1'  # Default value


sent_messages = {}
lock = threading.Lock()
latency_results = {}  # sequence_number: latency

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=broker_ip,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1
)

# Kafka Consumer Configuration for receiving inferences
consumer = KafkaConsumer(
    'inference-result',
    bootstrap_servers=broker_ip,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=f'iot-producer-group-{producer_id}'  # Unique group_id per producer
)

# Download the CIFAR-100 dataset using torchvision
transform = transforms.Compose([transforms.ToTensor()])
cifar100 = torchvision.datasets.CIFAR100(root='./data', train=True, download=True, transform=transform)
class_labels = cifar100.classes

# Function to convert image to bytes
def image_to_bytes(image):
    img_numpy = image.permute(1, 2, 0).numpy()
    _, img_encoded = cv2.imencode('.png', img_numpy * 255)
    return img_encoded.tobytes()

# Consumer thread function
def consumer_thread():
    for message in consumer:
        value = message.value
        msg_id = value.get('ID')
        if msg_id:

            # Extract sequence number from msg_id
            parts = msg_id.split('_')
            if len(parts) >= 4 and parts[0] == producer_id and parts[1] == 'seq':
                seq_num = int(parts[2])
                with lock:
                    send_time = sent_messages.pop(seq_num, None)
                if send_time:
                    latency = time.time() - send_time
                    with lock:
                        latency_results[seq_num] = latency
                    print(f"Received inference for {msg_id}, latency: {latency} seconds")
                else:
                    pass
            else:
                pass

# Start the consumer thread
consumer_thread = threading.Thread(target=consumer_thread)
consumer_thread.daemon = True  # So it exits when main thread exits
consumer_thread.start()

# Main loop to produce images and receive inference results
messages_sent = 0
total_messages = 1000
sequence_number = 1  

while messages_sent < total_messages:
    # Randomly select an image from CIFAR-100
    index = random.randint(0, len(cifar100) - 1)
    image, label = cifar100[index]
    image_bytes = image_to_bytes(image)

    # Generate unique message ID
    # Includes producer_id, 'seq', sequence_number, and timestamp
    msg_id = f"{producer_id}_seq_{sequence_number}_{int(time.time()*1000)}"

    # Store the timestamp when message is sent, keyed by sequence_number
    with lock:
        sent_messages[sequence_number] = time.time()

 
    message = {
        "ID": msg_id,
        "GroundTruth": class_labels[label],
        "Data": image_bytes.decode('latin1')  
    }

    # Send the message to Kafka
    producer.send("iot-images", value=message)
    producer.flush()

    print(f"Sent image {msg_id} with ground truth '{class_labels[label]}' to Kafka")

    sequence_number += 1
    messages_sent += 1
    time.sleep(1)  

print("All messages sent. Waiting for inference results...")
time.sleep(60)

# After all messages are sent and responses are received, write latency results to file in order
with lock:

    ordered_seq_nums = sorted(latency_results.keys())
    filename = f'producer.txt'
    with open(filename, 'w') as f:
        for seq_num in ordered_seq_nums:
            latency = latency_results[seq_num]
            f.write(f"{seq_num},{latency}\n")
    print(f"Latency results saved to {filename}")
