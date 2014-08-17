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

if "transfers" not in data.keys():
    data["transfers"] = data["transfersReducer"]

stats = {
    "jobCompletionTime" : float(data["finishTime"]-data["startTime"]),
    "numMaps" : int(data["numMaps"]),
    "numReduces" : int(data["numReduces"]),
    "numTransfers" : len([x for x in data["transfers"]]),
#    "numTransfersMissed" : len([x for x in data["transfers"] if x["intentionTime"] < 0]),
    "numMapSlotsPerNode" : int(data["config"]["numMapSlots"]),
#    "numWaves" : len(set([x["waveNumber"] for x in data["transfers"]])),
#    "dfsReplication" : int(data["dfsReplication"]),
#    "dfsBlockSize" : int(data["dfsBlockSize"])/1024/1024,
#    "numTransfersAggregatedByHost" : len(data["transfersAggregatedByHost"]),
#    "numTransfersAggregatedByHostByWave" : len(data["transfersAggregatedByHostByWave"])
}



# Prediction time (in seconds)
#intent = [float(x['intentionTime'])/1000 for x in data["transfers"] if x['intentionTime'] > 0 and x["startTime"] > 0]
#finish = [float(x['finishTime'])/1000 for x in data["transfers"] if x['intentionTime'] > 0 and x["startTime"] > 0]
#prediction = [x - y for x, y in zip(finish, intent)]
#stats["predictionTime"] = {
#    "min" : min(prediction),
#    "max" : max(prediction),
#    "avg" : reduce(lambda x, y: x + y, prediction) / len(prediction)
#}

# Transfer duration (in seconds)
start = [float(x['startTime']) for x in data["transfers"] if x["srcAddress"] != x["dstAddress"]]
finish = [float(x['finishTime']) for x in data["transfers"] if x["srcAddress"] != x["dstAddress"]]
duration = [x - y for x, y in zip(finish, start)]
stats["transferDuration"] = {
    "min" : min(duration),
    "max" : max(duration),
    "avg" : reduce(lambda x, y: x + y, duration) / len(duration),
    "sum" : sum(duration)
}



# Transfer size (in MB)
size = [float(x['size'])/1024/1024 for x in data["transfers"] if x["size"] > 0]
stats["transferSize"] = {
    "min" : min(size),
    "max" : max(size),
    "avg" : reduce(lambda x, y: x + y, size) / len(size),
    "sum" : sum(size)
}


# Map task duration (in seconds)
start = [float(x['startTime']) for x in data["tasks"] if x["type"] == "MAP" and x["finishTime"] > 0 and x["startTime"] > 0]
finish = [float(x['finishTime']) for x in data["tasks"] if x["type"] == "MAP" and x["finishTime"] > 0 and x["startTime"] > 0]
duration = [x - y for x, y in zip(finish, start)]
stats["taskDurationMap"] = {
    "min" : min(duration),
    "max" : max(duration),
    "avg" : reduce(lambda x, y: x + y, duration) / len(duration),
    "sum" : sum(duration)
}

# Reduce task duration (in seconds)
start = [float(x['startTime']) for x in data["tasks"] if x["type"] == "REDUCE" and x["finishTime"] > 0 and x["startTime"] > 0]
finish = [float(x['finishTime']) for x in data["tasks"] if x["type"] == "REDUCE" and x["finishTime"] > 0 and x["startTime"] > 0]
duration = [x - y for x, y in zip(finish, start)]
stats["taskDurationReduce"] = {
    "min" : min(duration),
    "max" : max(duration),
    "avg" : reduce(lambda x, y: x + y, duration) / len(duration),
    "sum" : sum(duration)
}

# Suffle duration (in seconds)
start = min([float(x['startTime']) for x in data["transfers"] if x['startTime'] > 0])
finish = max([float(x['finishTime']) for x in data["transfers"] if x['finishTime'] > 0])
stats["shuffleDuration"] = finish-start

print json.dumps(stats, indent=4, sort_keys=True)

