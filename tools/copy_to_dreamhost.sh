#!/bin/sh

if [ $# -ne 1 ]
then
	echo "Usage: $0 file"
	exit 0
fi

scp $1 mvneves@marceloneves.org:~/public/
