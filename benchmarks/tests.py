#!/usr/bin/python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

from trace import Trace
from master import Master
from slave import Slave
from iperf import Transfer

import json
import os
import sys
import time
import re

class Test():
    def __init__(self, host, master, nodes):
        self.host = host
        self.master = master
        self.nodes = nodes

    def genShuffleTransfers(self, mapper, nodes, num_transfers, size):
        transfers = []
        for n in nodes:
            port = 7777
            if n != mapper:
                for i in range(0,num_transfers):
                    #print i,n,mapper,port
                    t = Transfer(mapper,port,n,port,size)
                    transfers.append(t)
                    port += 1

        for t in transfers:
            print t

        return transfers

    def run(self):
        print "Test: starting emulation"

        # Single shuffle (one-to-many) 
        num_transfers = 1
        size = 1024*1024*10
        transfers = self.genShuffleTransfers(nodes[0], nodes, num_transfers,size)

        # Parallel shuffle (many-to-many) 
#        num_transfers = 1
#        size = 1024
#        transfers = []
#        for mapper in nodes:
#            transfers += self.genShuffleTransfers(mapper, nodes, num_transfers,size)
        
        # start master
        if host in master:
            m = Master(host, nodes)
            m.start()
        
        # start slaves
        if host in nodes:
            s = Slave(host, master, nodes, transfers)
            s.start()

        if host in nodes:
            s.join()
        
        if host in master:
            m.join()
            outfile = open("./output/done.json", 'w')
            json.dump(m.result, outfile, indent=4, sort_keys=True)
            outfile.close()
        
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: %s host\n" % (sys.argv[0])
        exit()

    host = sys.argv[1]
#    nodes = ["10.0.0.2", "10.0.0.3"]
    nodes = ["10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5", "10.0.0.6",
    "10.0.0.7", "10.0.0.8", "10.0.0.9","10.0.0.10","10.0.0.11","10.0.0.12",
    "10.0.0.13", "10.0.0.14", "10.0.0.15","10.0.0.16",]
    master = "10.0.0.1"
    
    if host not in nodes and host not in master:
        sys.exit(0)

    # start test
    test = Test(host, master, nodes)
    test.run()
