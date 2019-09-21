#!/bin/bash   

while true; do 
	NOW=`date +%s`
	curl https://api.blockchair.com/bitcoin/stats > /Users/rob/UniNettuno/dataset/bitcoin/blockchair/btc-stats.$NOW.data
	sleep 30 
done

