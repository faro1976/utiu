#!/bin/bash   

while true; do 
	NOW=`date +%s`
	STR_IN=""
	for i in {1..1000}; do
		STR_IN=${STR_IN}$"250.5,0.87003,0.46851,0.0091224,1,-67,0.85289,920.75\n"
	done
	echo -e $STR_IN
	echo -e $STR_IN > rt/activity/input/random.$NOW.input
done

