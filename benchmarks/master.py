# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

from threading import Thread, Condition, Lock
import time
import socket
import sys
import traceback
import cPickle as pickle
from comm import MASTER_PORT, SLAVE_PORT

class Master(Thread):
    def __init__(self, host, nodes):
        Thread.__init__(self)
        self.host = host
        self.nodes = nodes
        self.result = None

    def run(self):
        print "Master: start at %s ... " % self.host
        self.waitForSlavesReady()
        print "Master: all slaves are ready ..."
        time.sleep(1)
        self.startTime = time.time()
        for node in self.nodes:
            ok = False
            while not ok:
                ok = self.sendStart(node)
        print "Master: sent start to all nodes"
        messages = self.waitForSlavesFinish()
        self.finishTime = time.time()
        print "Master: all slaves are finished ..."
        self.result = self.finish(messages)

    def waitForSlavesReady(self):
        s = socket.socket()
        host = self.host
        port = MASTER_PORT
        print "Master: starting server in %s:%d" % (host, port)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.bind((host, port))
        s.listen(1024)

        print self.nodes
        
        numNodesReady = 0
        while numNodesReady < len(self.nodes):
            c, addr = s.accept()
            data = c.recv(262144)
            print "Master: received %s from %s:%d" % (data, addr[0], addr[1])
            data = "ok"
            c.send(data)
            c.close()
            numNodesReady += 1

    def waitForSlavesFinish(self):
        s = socket.socket()
        host = self.host
        port = MASTER_PORT
        print "Master: starting server in %s:%d" % (host, port)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.bind((host, port))
        s.listen(1024)

        print self.nodes
        
        messages = []
        numNodesReady = 0
        while numNodesReady < len(self.nodes):
            c, addr = s.accept()
            data = ""
            ok = False
            while not ok:
                data += c.recv(4096)
                try:
                    obj = pickle.loads(data)
                    ok = True
                except:
                    ok = False
                    #print traceback.print_exc(None, sys.stderr)
            messages.append(obj)
            print "Master: received finish from %s:%d" % (addr[0], addr[1])
            data = "ok"
            c.send(data)
            c.close()
            numNodesReady += 1
        
        return messages

    def sendStart(self, node):
        try:
            s = socket.socket()
            host = node
            port = SLAVE_PORT
            print "Master: %s is sending start to %s:%d" % (self.host, host, port)
            s.connect((host, port))
            data = "start"
            s.send(data)
            data = s.recv(1024)
            print "Master: %s received %s from %s:%d" % (self.host, str(data), host, port)
            s.close()
            return True
        except:
            print traceback.print_exc(None, sys.stderr)
            return False

    def finish(self, messages):
        print len(messages)
        transfer_list = []
        for transfers in messages:
            print len(transfers)
            for t in transfers:
                #print t
                entry = {"srcAddress": t.srcAddress,
                         "srcPort": t.srcPort,
                         "dstAddress": t.dstAddress,
                         "dstPort": t.dstPort,
                         "duration": t.duration,
                         "startTime": t.startTime,
                         "finishTime": t.finishTime,
                         "size": t.size,
                         "speed": float(t.size*8)/t.duration/1000000}
                transfer_list.append(entry)
        data = {"name": "iperf",
                "startTime": self.startTime,
                "finishTime": self.finishTime,
                "testDuration": float(self.finishTime-self.startTime),
                "transfers": transfer_list,
                "nodes": self.nodes}
        return data
