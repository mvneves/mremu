#!/bin/bash

sudo mn -c

x=`ps auxf | grep emulator.py | grep mnexec | awk '{print $2}'`
if [ ! -z "$x" ]
then
    sudo kill -9 $x
fi

x=`ps auxf | grep emulator.py | grep python | awk '{print $2}'`
if [ ! -z "$x" ]
then
    sudo kill -9 $x
fi

x=`ps axuf | grep iperf | grep "/bin/sh" | awk '{print $2}'`
if [ ! -z "$x" ]
then
    sudo kill -9 $x
fi

x=`ps auxf | grep iperf | sed '/grep/d' | awk '{print $2}'`
if [ ! -z "$x" ]
then
    sudo kill -9 $x
fi

x=`ps axuf | grep bwm-ng | grep "/bin/sh" | awk '{print $2}'`
if [ ! -z "$x" ]
then
    sudo kill -9 $x
fi

x=`ps auxf | grep bwm-ng | sed '/grep/d' | awk '{print $2}'`
if [ ! -z "$x" ]
then
    sudo kill -9 $x
fi


rm error-*.txt -f
rm output-*.txt -f
rm monitor-*.txt -f
rm ./hadoop/logs/hadoop-*.log -f
rm ./hadoop/logs/simulator-*.log -f
rm ./hadoop/tmp/* -f
rm -rf /tmp/iperf
mkdir -p /tmp/iperf
rm -rf ./output/*
