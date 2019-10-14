#!/bin/bash   

#set frameworks path
FWK_PATH="/Users/rob/src/extlib"

cd $FWK_PATH/hadoop-2.9.2
bin/hadoop dfs -mkdir /wine
bin/hadoop dfs -mkdir /activity
bin/hadoop dfs -mkdir /btc

cd $FWK_PATH/kafka_2.12-2.3.0
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic wine
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic activity
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic btc
#start dummy consumer
cd $FWK_PATH/kafka_2.12-2.3.0
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning --consumer-property group.id=test1
#start dummy producer
cd $FWK_PATH/kafka_2.12-2.3.0
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
#remove topic
bin/kafka-topics.sh --zookeeper localhost:2181 --topic btc --delete
