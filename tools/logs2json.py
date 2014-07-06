#!/usr/bin/python

import re
from datetime import datetime
import sys
import json

def logParseJobTracker(logFile=None):
    # parse file
    jobs = dict()
    f = open(logFile)
    for line in f.readlines():
        m = re.match('(.*) INFO org.apache.hadoop.mapred.JobInProgress: (.*): nMaps=(.*) nReduces=(.*) max=.*', line, re.M|re.I)
        if m:
            #print m.group()
            d = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S,%f')
            startTime = int(d.strftime("%s%f"))/1000
            name = m.group(2)
            jobs[name] = {"name" : name,
                            "startTime" : (float(startTime)/1000), # timestamp in seconds
                            "numMaps" : m.group(3),
                            "numReduces" : m.group(4)
                            }
            continue
        m = re.match('(.*) INFO org.apache.hadoop.mapred.JobInProgress: Job (.*) has completed successfully.', line, re.M|re.I)
        if m:
            #print m.group()
            d = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S,%f')
            finishTime = int(d.strftime("%s%f"))/1000
            name = m.group(2)
            jobs[name]["finishTime"] = (float(finishTime)/1000) # timestamp in seconds

    return jobs

def getJobName(taskName=""):
    p = taskName.split("_")
    return "job_%s_%s" % (p[1], p[2])

def logParseTaskTracker(logFile=None):

    # search for ip address
    ipAddress = ""
    f = open(logFile)
    for line in f.readlines():
        m = re.match('.*STARTUP_MSG:   host = (.*)/(.*)', line, re.M|re.I)
        if m:
            ipAddress = m.group(2)
            break
    f.close()

    # parse file
    transferList = dict()
    tasks = dict()
    f = open(logFile)
    for line in f.readlines():
        m = re.match('(.*) INFO org.apache.hadoop.mapred.TaskTracker: JVM with ID: .* given task: (.*)', line, re.M|re.I)
        if m:
            #print m.group()
            d = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S,%f')
            startTime = int(d.strftime("%s%f"))/1000
            name = m.group(2)
            jobName = getJobName(name)
            #print "TASK_START: %s %d" % (taskID, startTime)
            taskType = "MAP"
            if re.search('_r_', name):
                taskType = "REDUCE"
            if jobName not in tasks:
                tasks[jobName] = dict()
            tasks[jobName][name] = {"startTime" : (float(startTime)/1000), # timestamp in seconds
                                    "type" : taskType,
                                    "host" : ipAddress
                                    }
            continue
        m = re.match('(.*) INFO org.apache.hadoop.mapred.TaskTracker: Task (.*) is done.', line, re.M|re.I)
        if m:
            #print m.group()
            d = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S,%f')
            startTime = int(d.strftime("%s%f"))/1000
            name = m.group(2)
            #print "TASK_FINISH: %s %d" % (taskID, startTime)
            tasks[jobName][name]["finishTime"] = (float(startTime)/1000) # timestamp in seconds
            continue
        m = re.match('(.*) INFO org.apache.hadoop.mapred.TaskTracker.clienttrace: src: (.*):(.*), dest: (.*):(.*), bytes: (.*), op: MAPRED_SHUFFLE, cliID: (.*), duration: (.*), reducer: (.*)', line, re.M|re.I)
        if m:
            #print m.group()
            d = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S,%f')
            finishTime = float(d.strftime("%s%f"))/1000
            duration = float(m.group(8))/1000000
            startTime = finishTime-duration
            mapper = m.group(7)
            reducer = "%s_r_%06d_0" % (mapper.split("_m_")[0], int(m.group(9)))
            transfer = {"mapper" : m.group(7),
                    "reducer" : reducer,
                    "srcAddress" : m.group(2),
                    "srcPort" : int(m.group(3)),
                    "dstAddress" : m.group(4),
                    "dstPort" : int(m.group(5)),
                    "size" : int(m.group(6)),                    
                    "startTime" :  (startTime/1000), # timestamp in seconds
                    "finishTime" : (finishTime/1000), # timestamp in seconds
                    "duration" : (duration/1000) # in seconds
                }
            if jobName not in transferList:
                transferList[jobName] = []
            transferList[jobName].append(transfer)
            #print transfer
            continue

    taskList = dict()
    for j in tasks:
        taskList[j] = []
        for t in tasks[j]:
            task = {"name" : t,
                    "startTime" : tasks[j][t]["startTime"],
                    "finishTime" : tasks[j][t]["finishTime"],
                    "type" : tasks[j][t]["type"],
                    "host" : tasks[j][t]["host"]
               }
            taskList[j].append(task)
    
    return (taskList, transferList)

if __name__ == "__main__":
        
    if len(sys.argv) < 2:
        print "Usage: %s logfile1.log, logfile2.log ..." % sys.argv[0]
        sys.exit(1)

    logsTaskTracker = []
    logsJobTracker = []
    for arg in sys.argv[1:]:
        if re.match("hadoop-.*-tasktracker-.*.log", arg):
            logsTaskTracker.append(arg)
        elif re.match("hadoop-.*-jobtracker-.*.log", arg):
            logsJobTracker.append(arg)
    if len(logsJobTracker) < 1:
        print "Missing log files for JobTrackers."
        sys.exit(1)

    if len(logsTaskTracker) < 1:
        print "Missing log files for TaskTrackers."
        sys.exit(1)

    jobList = dict()
    for logFile in logsJobTracker:
        jobList = logParseJobTracker(logFile)
        for j in jobList:
            jobList[j]["tasks"] = []
            jobList[j]["transfers"] = []
        break
    #print json.dumps(jobList, indent=4, sort_keys=True)

    for logFile in logsTaskTracker:
        (tasks, transfers) = logParseTaskTracker(logFile)
        for j in tasks:
            jobList[j]["tasks"] += tasks[j]
        for j in transfers:
            jobList[j]["transfers"] += transfers[j]
    #print json.dumps(jobList, indent=4, sort_keys=True)

    for j in jobList:
        fileName = "%s.json" % j
        print fileName
        outfile = open(fileName, 'w')
        json.dump(jobList[j], outfile,indent=4, sort_keys=True)
        outfile.close()
