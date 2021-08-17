source crypto/bin/activate
pip install -r requirements.txt
zKServer start
kafka-server-start /usr/local/etc/kafka/server.properties
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-crypto
