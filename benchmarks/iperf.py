
from threading import Thread, Condition, Lock
from subprocess import Popen
import time

class Transfer(object):
    def __init__(self, srcAddress, srcPort, dstAddress, dstPort, size):
        self.srcAddress = srcAddress
        self.srcPort = srcPort
        self.dstAddress = dstAddress
        self.dstPort = dstPort
        self.size = size
        self.duration = 0
        self.startTime = 0
        self.finishTime = 0

    def __str__(self):
        return "Transfer: src=(%s:%d), dst=(%s:%d), size=%d" % (self.srcAddress, 
        self.srcPort, self.dstAddress, self.dstPort, self.size)

class IperfClient(Thread):
    def __init__(self, transfer):
        Thread.__init__(self)
        self.transfer = transfer

    def run(self):
        print "IperfClient: starting iperf client"
        #size = self.transfer.size * MININET_BW
        size = self.transfer.size
        cmd = "./trafficgen/iperf-client %s %s %d %d" % (self.transfer.srcAddress, self.transfer.dstAddress, self.transfer.dstPort, size)
        print "IperfClient: run %s" % cmd
        self.transfer.startTime = time.time()
        p = Popen(cmd, shell=True)
        p.wait()
        self.transfer.finishTime = time.time()
        self.transfer.duration = float(self.transfer.finishTime-self.transfer.startTime)
        print "IperfClient: finished %s:%d" % (self.transfer.dstAddress, self.transfer.dstPort)


class IperfServer(Thread):
    def __init__(self, transfer):
        Thread.__init__(self)
        self.transfer = transfer

    def run(self):
        print "ReduceCopier: starting iperf server at %s:%d" % (self.transfer.dstAddress, self.transfer.dstPort)
        cmd = "./trafficgen/iperf-server %s %d" % (self.transfer.dstAddress, self.transfer.dstPort)
        print "IperfServer: run %s" % cmd
        p = Popen(cmd, shell=True)
        p.wait()

        print "IperfServer: finished %s:%d" % (self.transfer.dstAddress, self.transfer.dstPort)
