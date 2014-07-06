#!/usr/bin/python

import sys
import json

if len(sys.argv) < 2:
    print "Usage: %s job.jso" % (sys.argv[0])
    exit()

job_file = sys.argv[1]

json_file = open(job_file)
data = json.load(json_file)
json_file.close()

for t in data["tasks"]:
	if t["finishTime"] > 0 and t["startTime"] > 0:
		print t["type"] + "," + t["host"] + "," + t["name"] + "," + str(t["finishTime"]-t["startTime"])

