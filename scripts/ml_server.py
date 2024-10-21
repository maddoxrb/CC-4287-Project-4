# ml_server.py
# 
# Author: Team 14 (Maddox, Emma, Abhay)
# CS4287-5287: Principles of Cloud Computing, Vanderbilt University
#
# Created: Sept 18, 2024
#
# Purpose: Processes images passed fom inference_consumer.py and uses a pretrrained model to classify the image.
# Returns the predicted class label to inference_consumer.py to be stored in the database
#

from flask import Flask, request, jsonify
import torch
import torchvision.transforms as transforms
from PIL import Image
import io
import time
import json
import random
import torchvision
import torchvision.transforms as transforms
from kafka import KafkaProducer
import numpy as np


app = Flask(__name__)
producer = KafkaProducer(
    bootstrap_servers="192.168.5.241:9092", 
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1
)

# Load the pretrained model
model = torch.hub.load('chenyaofo/pytorch-cifar-models', 'cifar100_resnet20', pretrained=True)
model.eval()

transform = transforms.Compose([
    transforms.Resize(32),
    transforms.ToTensor(),
    transforms.Normalize((0.5071, 0.4865, 0.4409), (0.2675, 0.2565, 0.2761))
])

# CIFAR-100 class labels
cifar100_labels = {
    0: 'apple',
    1: 'aquarium_fish',
    2: 'baby',
    3: 'bear',
    4: 'beaver',
    5: 'bed',
    6: 'bee',
    7: 'beetle',
    8: 'bicycle',
    9: 'bottle',
    10: 'bowl',
    11: 'boy',
    12: 'bridge',
    13: 'bus',
    14: 'butterfly',
    15: 'camel',
    16: 'can',
    17: 'castle',
    18: 'caterpillar',
    19: 'cattle',
    20: 'chair',
    21: 'chimpanzee',
    22: 'clock',
    23: 'cloud',
    24: 'cockroach',
    25: 'couch',
    26: 'crab',
    27: 'crocodile',
    28: 'cup',
    29: 'dinosaur',
    30: 'dolphin',
    31: 'elephant',
    32: 'flatfish',
    33: 'forest',
    34: 'fox',
    35: 'girl',
    36: 'hamster',
    37: 'house',
    38: 'kangaroo',
    39: 'keyboard',
    40: 'lamp',
    41: 'lawn_mower',
    42: 'leopard',
    43: 'lion',
    44: 'lizard',
    45: 'lobster',
    46: 'man',
    47: 'maple_tree',
    48: 'motorcycle',
    49: 'mountain',
    50: 'mouse',
    51: 'mushroom',
    52: 'oak_tree',
    53: 'orange',
    54: 'orchid',
    55: 'otter',
    56: 'palm_tree',
    57: 'pear',
    58: 'pickup_truck',
    59: 'pine_tree',
    60: 'plain',
    61: 'plate',
    62: 'poppy',
    63: 'porcupine',
    64: 'possum',
    65: 'rabbit',
    66: 'raccoon',
    67: 'ray',
    68: 'road',
    69: 'rocket',
    70: 'rose',
    71: 'sea',
    72: 'seal',
    73: 'shark',
    74: 'shrew',
    75: 'skunk',
    76: 'skyscraper',
    77: 'snail',
    78: 'snake',
    79: 'spider',
    80: 'squirrel',
    81: 'streetcar',
    82: 'sunflower',
    83: 'sweet_pepper',
    84: 'table',
    85: 'tank',
    86: 'telephone',
    87: 'television',
    88: 'tiger',
    89: 'tractor',
    90: 'train',
    91: 'trout',
    92: 'tulip',
    93: 'turtle',
    94: 'wardrobe',
    95: 'whale',
    96: 'willow_tree',
    97: 'wolf',
    98: 'woman',
    99: 'worm'
}

@app.route('/infer', methods=['POST'])
def infer():
    image_bytes = request.data
    image = Image.open(io.BytesIO(image_bytes))

    # Apply the transform
    image = transform(image)
    image = image.unsqueeze(0)  

    # Pass through the model
    with torch.no_grad():
        outputs = model(image)

    # Get predicted class
    _, predicted = outputs.max(1)
    predicted_label = cifar100_labels[int(predicted.item())]

    print(predicted_label)

    index = random.randint(0, 999999)

    # Create message
    msg_id = index
    message = {
        "ID": msg_id,
        "Inference": predicted_label
    }

    # Send the message to Kafka
    producer.send("inference-result", value=message)
    producer.flush()

    
    return jsonify({'inference': predicted_label})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


































import time
import json
import requests 
from kafka import KafkaConsumer, KafkaProducer

# Kafka Consumer Configuration 
consumer = KafkaConsumer(
    'iot-images',
    bootstrap_servers='192.168.5.226:9092', 
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Kafka Producer Configuration for inferences
producer = KafkaProducer(
    bootstrap_servers='192.168.5.226:9092',  
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1
)


ML_SERVER_URL = 'http://192.168.5.193:5000/infer' 


for msg in consumer:
    message = msg.value
    image_bytes = message['Data'].encode('latin1')  
    
    
    print(f"Image {message['ID']} ground truth: {message['GroundTruth']}")

    # Send image to ML server for labeling
    response = requests.post(ML_SERVER_URL, data=image_bytes)
    
    # Print the response from the ML server
    if response.status_code == 200:
        inferred_value = response.json()['inference']
        print(f"Image {message['ID']} retrieved and labeled as: {inferred_value}")
        
        # Create new message for the 'inferences' topic
        inference_message = {
            'ID': message['ID'],
            'GroundTruth': message['GroundTruth'],
            'InferredValue': inferred_value
        }

        # Send the inference to the 'inferences' Kafka topic
        producer.send('inferences', value=inference_message)
        print(f"Inference for image {message['ID']} sent to Kafka 'inferences' topic")
    else:
        print(f"Failed to retrieve label for image {message['ID']}")

consumer.close()
