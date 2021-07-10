from kafka import KafkaConsumer
from kafka.structs import TopicPartition

consumer = KafkaConsumer()
mypartition = TopicPartition("crypto-stream", 0)
assigned_topic = [mypartition]
consumer.assign(assigned_topic)

consumer.seek(mypartition, 500)

for _ in range(30):
    poll_result = consumer.poll(0, 3, True)
    for key in poll_result.keys():
        for item in poll_result[key]:
            print("\n", item.value)
