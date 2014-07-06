#!/bin/sh

(top -b -p 1 -d 1 | grep --line-buffered "^Cpu")

