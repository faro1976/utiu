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
sbin/start-dfs.sh

sleep 5
#start spark
cd $FWK_PATH/spark-2.4.0-bin-hadoop2.7
export SPARK_IDENT_STRING=spark1
sbin/start-master.sh --port 8001 --webui-port 8011
sleep 2
sbin/start-slave.sh localhost:8001
export SPARK_IDENT_STRING=spark2
sleep 2
sbin/start-master.sh --port 8002 --webui-port 8012
sleep 2
sbin/start-slave.sh localhost:8002

sleep 5
jps
