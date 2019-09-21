#!/bin/bash   

#set frameworks path
FWK_PATH="/Users/rob/src/extlib"

cd $FWK_PATH/hadoop-2.9.2
bin/hadoop dfs -mkdir /wine
bin/hadoop dfs -mkdir /activity

cd $FWK_PATH/kafka_2.12-2.3.0
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic wine
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic activity
