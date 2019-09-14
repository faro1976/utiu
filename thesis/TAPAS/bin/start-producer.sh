#!/bin/bash   

#set frameworks path
FWK_PATH="/Users/rob/src/extlib"

#start dummy producer
cd $FWK_PATH/kafka_2.12-2.3.0
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

