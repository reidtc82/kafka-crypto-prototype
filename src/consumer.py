from kafka import KafkaConsumer

consumer = KafkaConsumer('crypto-stream')
for msg in consumer:
    print (msg)