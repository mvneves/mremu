#!/usr/bin/python

import sys
import json

if len(sys.argv) < 2:
    print "Usage: %s jog_trace.json" % (sys.argv[0])
    exit()

job_file = sys.argv[1]

json_file = open(job_file)
data = json.load(json_file)
json_file.close()

print "src,dst,size,duration"
for t in data["transfers"]:
	if t["srcAddress"] != t["dstAddress"]:
		print t["srcAddress"] + "," + t["dstAddress"] + "," + str(t["size"]) + "," + str(t["duration"])

