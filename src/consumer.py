from kafka import KafkaConsumer
import json

consumer = KafkaConsumer("crypto-stream")
for msg in consumer:
    print(msg.offset)
    print(json.loads(msg.value))
