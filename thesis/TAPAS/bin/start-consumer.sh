#!/bin/bash   

#set frameworks path
FWK_PATH="/Users/rob/src/extlib"

#start dummy consumer
cd $FWK_PATH/kafka_2.12-2.3.0
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
