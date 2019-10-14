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
export SPARK_IDENT_STRING=spark1
cd $FWK_PATH/spark-2.4.0-bin-hadoop2.7_1
sbin/start-master.sh --port 8001 --webui-port 8011
sleep 2
sbin/start-slave.sh localhost:8001
export SPARK_IDENT_STRING=spark2
cd $FWK_PATH/spark-2.4.0-bin-hadoop2.7_2
sleep 2
sbin/start-master.sh --port 8002 --webui-port 8012
sleep 2
sbin/start-slave.sh localhost:8002
export SPARK_IDENT_STRING=spark3
cd $FWK_PATH/spark-2.4.0-bin-hadoop2.7_3
sleep 2
sbin/start-master.sh --port 8003 --webui-port 8013
sleep 2
sbin/start-slave.sh localhost:8003

sleep 5
jps
