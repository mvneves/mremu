# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

from threading import Thread, Condition, Lock
from comm import MASTER_PORT, SLAVE_PORT
import time
import socket
import sys
import copy
from Queue import Queue
import cPickle as pickle
from subprocess import Popen
import traceback
from iperf import IperfClient, IperfServer

TIMEOUT = 3600

class Slave(Thread):
    def __init__(self, host, master, nodes, transfers):
        Thread.__init__(self)
        self.host = host
        self.master = master
        self.nodes = nodes
        self.transfers = transfers

    def run(self):
        print "Slave: start at %s ... " % self.host

        # Start iperf servers
        threads = []
        for t in self.transfers:
            if t.dstAddress == self.host:
                iperf = IperfServer(t)
                iperf.start()
                threads.append(iperf)
                print t
            
        time.sleep(1)
        ok = False
        while not ok:
            ok = self.sendMessage("ready")
        print "Slave: successfully send ready"
        self.waitForStart()
        print "Slave: start test at " + str(time.time())
        
        # Start iperf clients
        myTransfers = []
        for t in self.transfers:
            if t.srcAddress == self.host:
                myTransfers.append(t)
                iperf = IperfClient(t)
                iperf.start()
                threads.append(iperf)
                print t

        # Wait for all of them to finish
        [x.join() for x in threads]

        message = pickle.dumps(myTransfers, 1)
        import sys
        print "sizeof=",sys.getsizeof(message)
        ok = False
        while not ok:
            ok = self.sendMessage(message)
        print "Slave: successfully send finish"

    def sendMessage(self, message):
        try:
            s = socket.socket()
            host = self.master
            port = MASTER_PORT
            print "Slave: %s is sending ready to %s:%d" % (self.host, host, port)
            s.connect((host, port))
            data = message
            s.sendall(data)
            data = s.recv(1024)
            print "Slave: %s received %s from %s:%d" % (self.host, str(data), host, port)
            s.close()
            return True
        except:
            print traceback.print_exc(None, sys.stderr)
            return False

    def waitForStart(self):
        s = socket.socket()
        host = self.host
        port = SLAVE_PORT
        print "Slave: starting server in %s:%d" % (host, port)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.bind((host, port))
        s.listen(1024)
        
        c, addr = s.accept()
        data = c.recv(1024)
        print "Slave: received %s from %s:%d" % (data, addr[0], addr[1])
        data = "ok"
        c.send(data)
        c.close()
