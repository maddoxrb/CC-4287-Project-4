import os
import sys
import time
import json
import base64
import random
import threading
import torchvision
import torchvision.transforms as transforms
from kafka import KafkaConsumer, KafkaProducer
import cv2

# Initialize producer_id from environment variable
producer_id = os.getenv('PRODUCER_ID', 'producer1')

# Kafka configurations from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-service:9092')
IOT_IMAGES_TOPIC = os.getenv('IOT_IMAGES_TOPIC', 'iot-images')
INFERENCE_RESULT_TOPIC = os.getenv('INFERENCE_RESULT_TOPIC', 'inference-result')

# Initialize Kafka Producer and Consumer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1
)

consumer = KafkaConsumer(
    INFERENCE_RESULT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=f'iot-producer-group-{producer_id}'
)

# Load CIFAR-100 dataset
transform = transforms.Compose([transforms.ToTensor()])
cifar100 = torchvision.datasets.CIFAR100(root='./data', train=True, download=True, transform=transform)
class_labels = cifar100.classes

# Function to convert image to Base64 bytes
def image_to_bytes(image):
    img_numpy = image.permute(1, 2, 0).numpy()
    _, img_encoded = cv2.imencode('.png', img_numpy * 255)
    img_bytes = img_encoded.tobytes()
    return base64.b64encode(img_bytes).decode('utf-8')

# Thread function to consume inference results
def consumer_thread():
    for message in consumer:
        value = message.value
        msg_id = value.get('ID')
        if msg_id:
            parts = msg_id.split('_')
            if len(parts) >= 4 and parts[0] == producer_id and parts[1] == 'seq':
                seq_num = int(parts[2])
                with lock:
                    send_time = sent_messages.pop(seq_num, None)
                if send_time:
                    latency = time.time() - send_time
                    with lock:
                        latency_results[seq_num] = latency
                    print(f"Received inference for {msg_id}, latency: {latency:.4f} seconds")
            else:
                print(f"Received message with invalid ID format: {msg_id}")

# Initialize data structures
sent_messages = {}
latency_results = {}
lock = threading.Lock()

# Start the consumer thread
threading.Thread(target=consumer_thread, daemon=True).start()

# Main loop to send images
messages_sent = 0
total_messages = 1000
sequence_number = 1

try:
    while messages_sent < total_messages:
        index = random.randint(0, len(cifar100) - 1)
        image, label = cifar100[index]
        image_bytes = image_to_bytes(image)

        msg_id = f"{producer_id}_seq_{sequence_number}_{int(time.time()*1000)}"

        with lock:
            sent_messages[sequence_number] = time.time()

        message = {
            "ID": msg_id,
            "GroundTruth": class_labels[label],
            "Data": image_bytes
        }

        producer.send(IOT_IMAGES_TOPIC, value=message)
        producer.flush()

        print(f"Sent image {msg_id} with ground truth '{class_labels[label]}' to Kafka")

        sequence_number += 1
        messages_sent += 1
        time.sleep(1)

    print("All messages sent. Waiting for inference results...")
    time.sleep(60) 

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    with lock:
        ordered_seq_nums = sorted(latency_results.keys())
        filename = f'/latency-results/producer_{producer_id}.txt'
        try:
            with open(filename, 'w') as f:
                for seq_num in ordered_seq_nums:
                    latency = latency_results[seq_num]
                    f.write(f"{seq_num},{latency:.4f}\n")
            print(f"Latency results saved to {filename}")
        except Exception as e:
            print(f"Failed to write latency results: {e}")