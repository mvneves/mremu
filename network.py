from mininet.topo import Topo
from mininet.node import Controller, RemoteController, OVSKernelSwitch, CPULimitedHost, OVSController
from mininet.net import Mininet
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.util import custom, irange
from mininet.log import setLogLevel, info, warn, error, debug
from mininet.topo import LinearTopo
from mininet.topolib import TreeTopo

from topology import FatTreeTopo, NonBlockingTopo, LinearMultipathTopo

from subprocess import Popen, PIPE
from argparse import ArgumentParser
import multiprocessing
from time import sleep
#from monitor.monitor import monitor_devs_ng
import os
import sys
import json

#from simulator import simulator

# Number of pods in Fat-Tree 
K = 4

# Queue Size
QUEUE_SIZE = 100

# Link capacity (Mbps)
BW = 10 

def NonBlockingNet(k=4, bw=10, cpu=-1, queue=100):
    ''' Create a NonBlocking Net '''

    topo = NonBlockingTopo(k)
    host = custom(CPULimitedHost, cpu=cpu)
    link = custom(TCLink, bw=bw, max_queue_size=queue)

    #net = Mininet(topo, host=host, link=link, switch=OVSKernelSwitch, controller=RemoteController)
    net = Mininet(topo, host=host, link=link, switch=OVSKernelSwitch, controller=OVSController, autoSetMacs=True, autoStaticArp=False)

    return net

def FatTreeNet(k=4, bw=10, cpu=-1, queue=100):
    ''' Create a Fat-Tree network '''

    info('*** Creating the topology')
    topo = FatTreeTopo(k)

    host = custom(CPULimitedHost, cpu=cpu)
    link = custom(TCLink, bw=bw, max_queue_size=queue)

    net = Mininet(topo, host=host, link=link, switch=OVSKernelSwitch,
            controller=RemoteController)

    return net

def LinearNet(n=2, m=4, bw=100, cpu=-1, queue=100):
    ''' Create a Linear network '''

    info('*** Creating the topology')
    topo = LinearMultipathTopo(n,m)

    host = custom(CPULimitedHost, cpu=cpu)
    link = custom(TCLink, bw=bw, max_queue_size=queue)
    
    net = Mininet(topo, host=host, link=link, switch=OVSKernelSwitch,
            controller=RemoteController)

    return net

def RegularTreeNet(depth=2, fanout=4, bw=BW, cpu=-1, queue=100):
    "Create an empty network and add nodes to it."
    topo = TreeTopo(depth, fanout)
    host = custom(CPULimitedHost, cpu=cpu)
    link = custom(TCLink, bw=bw, max_queue_size=queue)
    #net = Mininet(host=host, link=link, switch=OVSKernelSwitch, controller=RemoteController, autoSetMacs=True, autoStaticArp=False)
    net = Mininet(topo, host=host, link=link, switch=OVSKernelSwitch, controller=OVSController, autoSetMacs=True, autoStaticArp=False)

    return net

def SingleSwitchNet(numHosts=16, bw=BW, cpu=-1, queue=100, remoteController=False):
    "Create an empty network and add nodes to it."
    host = custom(CPULimitedHost, cpu=cpu)
    link = custom(TCLink, bw=bw, max_queue_size=queue)
    if remoteController is True:
        controller = RemoteController
    else:
        controller = OVSController
    net = Mininet(host=host, link=link, switch=OVSKernelSwitch, controller=controller, autoSetMacs=True, autoStaticArp=False)

    info('*** Adding controller\n')
    net.addController( 'c0' )

    info('*** Adding hosts\n')
    allHosts = []
    i = 1

    info('*** Adding hosts\n')
    # Adding hosts
    for n in irange(1, numHosts):
        print "h%s" % n
        host = net.addHost("h%s" % n, ip=("10.0.0.%s" % str(i)))
        i = i + 1
        allHosts.append(host)

    info('*** Adding switches\n')
    i = 1
    # Adding switches
    print "s%s" % i
    switch = net.addSwitch("s%s" % i)

    info('*** Creating links to hosts\n')
    # Creating links to hosts
    for n in irange(0, numHosts-1):
        host = allHosts[n]
        print "linking %s to %s" % (host.name, switch.name)
        net.addLink(host, switch)
        print ""

    return net

def SinglepathTreeNet(depth = 2, fanout = 4, bw=BW, cpu=-1, queue=100, remoteController=False):
    "Create an empty network and add nodes to it."
    host = custom(CPULimitedHost, cpu=cpu)
    link = custom(TCLink, bw=bw, max_queue_size=queue)
    if remoteController is True:
        controller=RemoteController
    else:
        controller=OVSController
    net = Mininet(host=host, link=link, switch=OVSKernelSwitch, controller=controller, autoSetMacs=True, autoStaticArp=False)

    info( '*** Adding controller\n' )
    net.addController( 'c0' )

    info('*** Adding hosts\n')
    switches = []
    allHosts = []
    i = 1
    
    depth = 2
    fanout = 4

    # Adding hosts
    for n in irange(1, pow(depth,fanout)):
        print "h%s" % n
        host = net.addHost("h%s" % n, ip=("10.0.0.%s" % str(i)))
        i = i + 1
        allHosts.append(host)

    info('*** Adding switches\n')
    i = 1
    # Adding switches
    rootSwitches = []
    print "s%s" % i
    switch = net.addSwitch("s%s" % i)
    rootSwitches.append(switch)
    i = i + 1

    torSwitches = []
    for n in irange(1, 4):
       print "s%s" % i
       switch = net.addSwitch("s%s" % i)
       torSwitches.append(switch)
       i = i + 1

    info('*** Wiring up switches\n')
    # Wiring up switches
    i = 1
    for switch in torSwitches:
        for up in rootSwitches:
            if i == 1:
                bandwidth=bw#10
            else:
                bandwidth=bw
            bandwidth=bw
            i = i + 1
            link = custom(TCLink, bw=bandwidth, max_queue_size=queue)
            linkObj = net.addLink(up, switch, cls=link)
            print "link=" + str(linkObj)

    info('*** Creating links to hosts\n')
    # Creating links to hosts
    for n in irange(0, 3):
        switch = torSwitches[n]
        for m in irange(0, 3):
            host = allHosts[(n*4)+m]
            print "linking %s to %s" % (host.name, switch.name)
            net.addLink(host, switch)
            print ""

    return net


def MultipathTreeNet(depth=2, fanout=4, bw=BW, cpu=-1, queue=100):
    "Create an empty network and add nodes to it."
    host = custom(CPULimitedHost, cpu=cpu)
    link = custom(TCLink, bw=bw, max_queue_size=queue)
    net = Mininet(host=host, link=link, switch=OVSKernelSwitch, controller=RemoteController, autoSetMacs=True, autoStaticArp=False)

    info( '*** Adding controller\n' )
    net.addController( 'c0' )

    info( '*** Adding hosts\n' )
    switches = []
    allHosts = []
    i = 1
    
    # Adding hosts
    for n in irange(1, pow(depth,fanout)):
        print "h%s" % n
        host = net.addHost("h%s" % n, ip=("10.0.0.%s" % str(i)))
        i = i + 1
        allHosts.append(host)

    info( '*** Adding switches\n' )
    i = 1
    # Adding switches
    rootSwitches = []
    for n in irange(1, 2):
       print "s%s" % i
       switch = net.addSwitch("s%s" % i)
       rootSwitches.append(switch)
       i = i + 1

    torSwitches = []
    for n in irange(1, 4):
       print "s%s" % i
       switch = net.addSwitch("s%s" % i)
       torSwitches.append(switch)
       i = i + 1
       
    info( '*** Wiring up switches\n' )
    # Wiring up switches
    i = 1
    for switch in torSwitches:
        for up in rootSwitches:
            if i == 1:
                bandwidth=bw#10
            else:
                bandwidth=bw
            #bandwidth=50
            i = i + 1
            link = custom(TCLink, bw=bandwidth, max_queue_size=queue)
            linkObj = net.addLink(up, switch, cls=link)
            print "link=" + str(linkObj)


    info( '*** Creating links to hosts\n' )
    # Creating links to hosts
    for n in irange(0, 3):
        switch = torSwitches[n]
        for m in irange(0, 3):
            host = allHosts[(n*4)+m]
            print "linking %s to %s" % (host.name, switch.name)
            net.addLink(host, switch)
            print ""
    return net


def DumbbellNet(numSwitches=2, numHostsPerSwitch=8, L=1, bw=BW, cpu=-1, queue=100, remoteController=True):
    "Create an empty network and add nodes to it."
    host = custom(CPULimitedHost, cpu=cpu)
    link = custom(TCLink, bw=bw, max_queue_size=queue)
    if remoteController is True:
        controller=RemoteController
    else:
        controller=OVSController
    net = Mininet(host=host, link=link, switch=OVSKernelSwitch, controller=controller, autoSetMacs=True, autoStaticArp=False)

    info('*** Adding controller\n')
    if remoteController is True:
        net.addController('c0', ip='172.16.2.1', port=6633)
    else:
       net.addController('c0')

    info('*** Adding hosts\n')
    switches = []
    allHosts = []
    i = 1

    # Adding hosts
    for n in irange(1, numSwitches):
#        print "s%s" % n
#        switch = net.addSwitch("s%s" % n)
#        switches.append(switch)
        hosts = []
        for m in irange(1, numHostsPerSwitch):
            print "s%sh%s" % (n, m)
            host = net.addHost("s%sh%s" % (n, m), ip=("10.0.0.%s" % str(i)))
            i = i + 1
            hosts.append(host)
 #           net.addLink(host, switch)
        allHosts.append(hosts)

    info('*** Adding switches\n')
    # Adding switches
    for n in irange(1, numSwitches):
       print "s%s" % n
       switch = net.addSwitch("s%s" % n)
       switches.append(switch)

    info('*** Wiring up switches\n')
    # Wiring up switches
    i = 1
    last = None
    for switch in switches:
        if last:
            for l in irange(1, L):
#               print "linking %s to %s" % (last.name, switch.name)
                if i == 1:
                    bandwidth=10
                else:
                    bandwidth=bw
                bandwidth=bw
                i = i + 1
                link = custom(TCLink, bw=bandwidth, max_queue_size=queue)
                linkObj = net.addLink(last, switch, cls=link)
                print "link=" + str(linkObj)
        last = switch

    info('*** Creating links to hosts\n')
    # Creating links to hosts
    for n in irange(0, numSwitches-1):
        switch = switches[n]
        for host in allHosts[n]:
            print "linking %s to %s" % (host.name, switch.name)
            net.addLink(host, switch)
            print ""

    return net
