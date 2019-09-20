#!/bin/bash   

#set frameworks path
FWK_PATH="/Users/rob/src/extlib"

#stop hdfs
cd $FWK_PATH/hadoop-2.9.2
sbin/stop-dfs.sh

#stop kafka server
bin/kafka-server-stop.sh config/server.properties

#stop zookeeper
cd $FWK_PATH/kafka_2.12-2.3.0
bin/zookeeper-server-stop.sh config/zookeeper.properties



