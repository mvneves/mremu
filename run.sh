#!/bin/bash

save_history()
{
	# save history
	t=`date +%Y%m%d%H%M%S`
	input=`cat config.json | tr -d '",' | grep "trace:" | awk '{print $2}'`
	output=`cat config.json | tr -d '",' | grep "output:" | awk '{print $2}'`
	job_name=`cat $input | grep job | grep name | tr -d '",' | awk '{print $2}'`

	cp config.json ./history/$t-$job_name-config.json
	cp output/done.json ./history/$t-$job_name-done.json
	#cp $output ./history/$t-$job_name-result.json
}

# cleanup previous executions 
./cleanup.sh
mkdir /tmp/iperf

# run emulations
sudo python emulator.py
cat output/done.json
sleep 2

# save history
save_history

