from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(0,10))
for i in range(100):
    print('I tried')
    producer.send('crypto-stream', b'ao;skhfg;alshfakl;sf')