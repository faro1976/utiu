#!/bin/bash   

#set frameworks path
FWK_PATH="/Users/rob/src/extlib"

#start zookeeper
cd $FWK_PATH/kafka_2.12-2.3.0
nohup bin/zookeeper-server-start.sh config/zookeeper.properties &

sleep 5
#start kafka server
nohup bin/kafka-server-start.sh config/server.properties &

sleep 5
#start hdfs
cd $FWK_PATH/hadoop-2.9.2
nohup sbin/start-dfs.sh &

jps
