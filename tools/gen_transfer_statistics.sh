#!/bin/sh

compute_time()
{
	f=$1

	line=`cat $f | grep OUTPUT | sed 's/OUTPUT=//g'`

	size=`echo $line | cut -d',' -f8`
	speed=`echo $line | cut -d',' -f9`
	time=`echo "($size*8)/$speed" | bc -l | awk '{printf "%.9f", $0}'`

	echo $line,$time
}

for i in /tmp/iperf/iperf-*-iperf--c-*.txt
do
	compute_time $i
done

