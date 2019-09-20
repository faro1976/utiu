#!/bin/bash   

#set frameworks path
FWK_PATH="/Users/rob/src/extlib"

cd $FWK_PATH/hadoop-2.9.2
bin/hadoop dfs -mkdir /wine

cd $FWK_PATH/hadoop-2.9.2
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic wine
