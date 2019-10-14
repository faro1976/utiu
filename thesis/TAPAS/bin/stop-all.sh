#!/bin/bash   

#set frameworks path
FWK_PATH="/Users/rob/src/extlib"

cd $FWK_PATH/spark-2.4.0-bin-hadoop2.7
export SPARK_IDENT_STRING=spark1
sbin/stop-slave.sh localhost:8001
sbin/stop-master.sh --port 8001 --webui-port 8011
sleep 2
export SPARK_IDENT_STRING=spark2
sbin/stop-slave.sh localhost:8002
sbin/stop-master.sh --port 8002 --webui-port 8012
export SPARK_IDENT_STRING=spark3
sbin/stop-slave.sh localhost:8003
sbin/stop-master.sh --port 8003 --webui-port 8013

cd $FWK_PATH/hadoop-2.9.2
sbin/stop-dfs.sh

cd $FWK_PATH/kafka_2.12-2.3.0
bin/kafka-server-stop.sh config/server.properties
bin/zookeeper-server-stop.sh config/zookeeper.properties

jps
