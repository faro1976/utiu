#!/bin/bash   

while true; do 
	NOW=`date +%s`
	curl https://api.blockchair.com/bitcoin/stats > ../rt/btc/input/btc-stats.$NOW.data
	sleep 60
done

