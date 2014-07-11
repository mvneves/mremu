# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import os
import json
import re

class Trace(object):
    trace_file = None
    hostmap_file = None
    maps = []
    reduces = []
    task2host = dict()
    hosts = []
 
    def __init__(self, trace_file, hostmap_file=None, host=None):
        """Init trace loader object"""
        if os.path.exists(trace_file) is False:
            raise Exception("Trace file %s not found." % trace_file)
        if hostmap_file is not None and os.path.exists(hostmap_file) is False:
            raise Exception("Hostname mapping file %s not found." % hostmap_file)
        self.trace_file = trace_file
        self.hostmap_file = hostmap_file
        self.host = host
        self._load()
        self._read_tasks()
        self._read_transfers()
        self.hosts = self.sort_ip_list(sorted(set([x["host"] for x in self.trace["tasks"]])))
        self.initialMapDelay = sorted([x["startTime"] for x in self.trace["tasks"] if x["type"] == "MAP" and x["host"] == host])[0] - self.trace["startTime"]
        reducers = sorted([x["startTime"] for x in self.trace["tasks"] if x["type"] == "REDUCE" and x["host"] == host])
        if len(reducers) < 1:
            self.initialReduceDelay = 0
        else:
            self.initialReduceDelay = reducers[0] - self.trace["startTime"]

    def sort_ip_list(self, ip_list):
        """Sort an IP address list."""
        from IPy import IP
        ipl = [(IP(ip).int(), ip) for ip in ip_list]
        ipl.sort()
        return [ip[1] for ip in ipl]

    def _load_hostmap(self):
        self.hostmap = None
        if self.hostmap_file is None:
            return
        json_file = open(self.hostmap_file)
        self.hostmap = json.load(json_file)
        json_file.close()

    def _load_trace(self):
        json_file = open(self.trace_file)
        self.trace = json.load(json_file)
        json_file.close()

    def _translate_hostnames(self):
        if self.hostmap is not None:
            for task in self.trace["tasks"]:
                task["host"] = self.hostmap[task["host"]]
            for transfer in self.trace["transfers"]:
                transfer["srcAddress"] = self.hostmap[transfer["srcAddress"]]
                if transfer["dstAddress"] == "null":
                    transfer["dstAddress"] = None
                else:
                    transfer["dstAddress"] = self.hostmap[transfer["dstAddress"]]
        
    def _load(self):
        self._load_trace()
        self._load_hostmap()
        self._translate_hostnames()
       
    def _read_tasks(self):
        del self.maps[:]
        del self.reduces[:]
        tasks = sorted(self.trace["tasks"],key=lambda x:x['name'])
        for task in tasks:
            if task["type"] == "MAP":
                duration = (float(task["finishTime"])-float(task["startTime"]))
                self.maps.append(Map(task["name"], task["host"], duration))
            elif task["type"] == "REDUCE":
                duration = float(task["sortingTime"])+float(task["processingTime"])
                t = Reduce(task["name"], task["host"], duration, float(task["waitingTime"]))
                t.sortingTime = float(task["sortingTime"])
                t.processingTime = float(task["processingTime"])
                t.waitingTime = float(task["waitingTime"])
                lastTransfer = sorted([float(x["finishTime"]) for x in self.trace["transfers"] if x["reducer"] == task["name"]], reverse=True)[0]
                t.mergingTime = float(task["shuffleFinished"]) - lastTransfer
                self.reduces.append(t)
            self.task2host[task["name"]] = task["host"]
        return (self.maps, self.reduces)

    def _read_transfers(self):
        for task in self.maps:
            for transfer in self.trace["transfers"]:
                if transfer["dstPort"] != 0 and transfer["mapper"] == task.name:
                    partition = DataPartition(transfer["mapper"], transfer["reducer"], 
                        self.task2host[transfer["mapper"]], transfer["srcPort"], self.task2host[transfer["reducer"]], transfer["dstPort"], transfer["size"])
                    task.addPartition(partition)
        for task in self.reduces:
            for transfer in self.trace["transfers"]:
                if transfer["dstPort"] != 0 and transfer["reducer"] == task.name:
                    partition = DataPartition(transfer["mapper"], transfer["reducer"], 
                        self.task2host[transfer["mapper"]], transfer["srcPort"], self.task2host[transfer["reducer"]], transfer["dstPort"], transfer["size"])
                    task.addPartition(partition)

    def getInitialDelayJob(self):
        return float(self.trace["startTime"]) - float(self.trace["submitTime"])

    def getInitialMapDelay(self, host):
        return self.initialMapDelay

    def getInitialReduceDelay(self, host):
        return self.initialReduceDelay

    def getPostDelayJob(self):
        reducers = sorted([x["finishTime"] for x in self.trace["tasks"] if x["type"] == "REDUCE"], reverse=True)
        if len(reducers) < 1:
            return 0
        return self.trace["finishTime"] - reducers[0]

    def getJobCompletionTime(self):
        return float(self.trace["finishTime"]) - float(self.trace["startTime"])

    def getHosts(self):
        return self.hosts

    def getNumHosts(self):
        return len(self.hosts)
        
    def getNumTransfers(self, host):
        return len([x for x in self.trace["transfers"] if x["srcAddress"] == host])

    def getMapTasks(self):
        return self.maps

    def getReduceTasks(self):
        return self.reduces
        
    def getMapTasksPerHost(self, host):
        return [task for task in self.maps if task.host == host]

    def getReduceTasksPerHost(self, host):
        return [task for task in self.reduces if task.host == host]
    
    def getJobName(self):
        return self.trace["name"]
    
    def getNumMaps(self):
        return int(self.trace["numMaps"])
    
    def getNumReduces(self):
        return int(self.trace["numReduces"])
    
    def printMaps(self):
        maps = self.getMapTasks()
        for m in maps:
            print m
    
    def printReduces(self):
        reduces = self.getReduceTasks()
        for r in reduces:
            print r

    def getConfig(self, host):
        config = Configuration(host, self.trace["config"]["user"], int(self.trace["config"]["numMapSlots"]), 
        int(self.trace["config"]["numReduceSlots"]), int(self.trace["config"]["maxParallelCopies"]), self.hostmap[self.trace["config"]["jobTracker"]])
        return config

class TraceOutput(object):
    tmp_path = "./hadoop/tmp/"

    def __init__(self, host):
        self.host = host

    def saveTaskTracker(self, maps, reduces, transfers): 
        output = dict()
        mapList = []
        for task in maps:
            entry = {"name": task.name,
                    "host": task.host,
                    "type": task.type,
                    "startTime": task.startTime,
                    "finishTime": task.finishTime
                }
            mapList.append(entry)
 
        reduceList = []
        for task in reduces:
            entry = {"name": task.name,
                    "host": task.host,
                    "type": task.type,
                    "startTime": task.startTime,
                    "finishTime": task.finishTime,
                    "shuffleFinished": task.shuffleFinished,
                    "sortFinished" : task.sortFinished,
                    "waitFinished" : task.waitFinished,
                    "processingTime": task.processingTime,
                    "sortingTime": task.sortingTime,
                    "waitingTime": task.waitingTime
                }
            reduceList.append(entry)

        transferList = []
        for t in transfers:
            entry = {"mapper": t.mapper,
                    "reducer": t.reducer,
                    "srcAddress": t.srcAddress,
                    "srcPort": t.srcPort,
                    "dstAddress": t.dstAddress,
                    "dstPort": t.dstPort,
                    "size": t.size,
                    "duration": t.duration,
                    "startTime": t.startTime,
                    "finishTime": t.finishTime
                }
            transferList.append(entry)

        output["tasks"] = mapList + reduceList
        output["transfers"] = transferList
        self._write(self.tmp_path + "%s.json" % self.host, output)

    def saveJobTracker(self, config, submitTime, startTime, finishTime, name, numMaps, numReduces):
        output = dict()

        conf = {
            "jobTracker": config.jobTracker,
            "maxParallelCopies": config.maxParallelTransfer,
            "numMapSlots": config.mapTaskSlots,
            "numReduceSlots": config.reduceTaskSlots,
            "user": config.username
        }

        output["config"] = conf
        output["name"] = name
        output["numMaps"] = numMaps
        output["numReduces"] = numReduces
        output["submitTime"] = submitTime
        output["startTime"] = startTime
        output["finishTime"] = finishTime

        self._write(self.tmp_path + "job.json", output)

    def _write(self, fileName, obj):
        outfile = open(fileName, 'w')
        json.dump(obj, outfile, indent=4, sort_keys=True)
        outfile.close()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

class Task(object):
    def __init__(self, name, type, host, duration):
        self.name = name
        self.type = type
        self.duration = duration
        self.host = host
        self.partitions = []
        self.startTime = 0
        self.finishTime = 0

    def addPartition(self, partition):
        self.partitions.append(partition)

    def printPartitions(self):
        for p in self.partitions:
            print p

    def __str__(self):
        return "Task: name=%s, type=%s, host=%s, duration=%s" % (self.name, 
            self.type, self.host, str(self.duration))


class Map(Task):
    
    def __init__(self, name, host, duration):
        super(Map, self).__init__(name, "MAP", host, duration)

class Reduce(Task):
    
    def __init__(self, name, host, duration, initialDelay):
        super(Reduce, self).__init__(name, "REDUCE", host, duration)
        self.initialDelay = initialDelay
        self.shuffleFinished = 0
        self.sortFinished = 0
        self.waitFinished = 0
        self.sortingTime = 0
        self.processingTime = 0
        self.waitingTime = 0
        self.mergingTime = 0
 
    def __str__(self):
        return "Task: name=%s, type=%s, host=%s, duration=%s, initialDelay=%s" % (self.name, 
            self.type, self.host, str(self.duration), str(self.initialDelay))

class DataPartition(object):
    def __init__(self, mapper, reducer, srcAddress, srcPort, dstAddress, dstPort, size):
        self.mapper = mapper
        self.reducer = reducer
        self.srcAddress = srcAddress
        self.srcPort = srcPort
        self.dstAddress = dstAddress
        self.dstPort = dstPort
        self.size = size
        self.duration = 0
        self.startTime = 0
        self.finishTime = 0

    def __str__(self):
        return "Partition: mapper=%s (%s:%d), reducer=%s (%s:%d), size=%d" % (self.mapper, 
            self.srcAddress, self.srcPort, self.reducer, self.dstAddress, self.dstPort, self.size)

class Configuration(object):
    def __init__(self, host, username, numMapSlots, numReduceSlots, maxParallelTransfer, jobTracker):
        self.host = host
        self.username = username
        self.mapTaskSlots = numMapSlots
        self.reduceTaskSlots = numReduceSlots
        self.maxParallelTransfer = maxParallelTransfer
        self.jobTracker = jobTracker
    
    def __str__(self):
        return "Configuration: host=%s, username=%s, mapTaskSlots=%s, reduceTaskSlots=%s, maxParallelTransfer=%s, jobTracker=%s" % (self.host, 
            self.username, self.mapTaskSlots, self.reduceTaskSlots, self.maxParallelTransfer, self.jobTracker)


