from mininet.topo import Topo
from mininet.node import (Controller, RemoteController, OVSKernelSwitch,
                          CPULimitedHost, OVSController)
from mininet.net import Mininet
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.util import custom, irange
from mininet.log import setLogLevel, info, warn, error, debug

from subprocess import Popen, PIPE
from argparse import ArgumentParser
import multiprocessing
from time import sleep
from monitor.monitor import monitor_devs_ng
import os
import sys
import json

from network import (DumbbellNet, SingleSwitchNet, SinglepathTreeNet,
                     MultipathTreeNet, FatTreeNet, ShamrockNet,
                     RegularTreeNet, ManagementNet)
from applauncher import HadoopTest, IperfTest

from nat import connectToGateway, establishRoutes, startNAT, stopNAT
from sshd import startSSH, stopSSH

def RunTest(net=None, remoteController=False, enableNAT=True):
	mgntController, mgntSwitch = ManagementNet(net)

	if enableNAT:
		root = connectToGateway(net, switch='mgnt0')

	net.build()
	info( '*** Starting controller\n' )
	for controller in net.controllers:
		controller.start()
	info( '*** Starting %s switches\n' % len( net.switches ) )
	for switch in net.switches:
		info( switch.name + ' ')
		if switch.name != "mgnt0":
			switch.start( [net.controllers[0]])
	mgntSwitch.start([mgntController])
	info( '\n' )

	if enableNAT:
		startNAT(root)
		establishRoutes(net)
		startSSH(net)

	# wait for the switches to connect to the controller
	info('** Waiting for switches to connect to the controller\n')
	sleep(5)
	if remoteController:
		sleep(30)

	#CLI(net)

	hosts = net.hosts

	#sleep(30)
	#net.pingAll()
	#net.pingAll()
	net.ping(hosts=hosts, timeout="1")
	if remoteController:
		net.ping(hosts=hosts, timeout="1")
		net.ping(hosts=hosts, timeout="1")

	#CLI(net)

	HadoopTest(hosts)
	#IperfTest(hosts)

	#CLI(net)

	if enableNAT:
		stopNAT(root)
		stopSSH(net)

	net.stop()

if __name__ == '__main__':
	setLogLevel( 'info' )

	json_file = open("config.json")
	config = json.load(json_file)
	json_file.close()
	print config
	#sys.exit()

	topology = config["networkTopology"]
	if topology == "singleswitch":
		net = SingleSwitchNet(numHosts=config["numHostsPerSwitch"],
			bw=config["bandwidthMininet"],
			cpu=config["cpuLimit"],
                        #cpu=0.05,
			queue=config["queue"],
			remoteController=config["remoteController"])
	elif topology == "dumbbell":
		net = DumbbellNet(numSwitches=config["numSwitches"],
			numHostsPerSwitch=config["numHostsPerSwitch"],
			L=config["numberTrunkLinks"],
			bw=config["bandwidthMininet"],
			cpu=config["cpuLimit"],
			queue=config["queue"],
			remoteController=config["remoteController"])
	elif topology == "singlepathtree":
		net = SinglepathTreeNet(depth=2, fanout=4,
			bw=config["bandwidthMininet"],
                        cpu=config["cpuLimit"],
                        queue=config["queue"],
			remoteController=config["remoteController"])
	elif topology == "multipathtree":
		net = MultipathTreeNet(depth=2, fanout=4,
			bw=config["bandwidthMininet"],
			cpu=config["cpuLimit"],
			queue=config["queue"])
	elif topology == "fattree":
		net = FatTreeNet(k=4,
			bw=config["bandwidthMininet"],
                        cpu=config["cpuLimit"],
                        queue=config["queue"])
	elif topology == "shamrock":
		net = ShamrockNet(bw=config["bandwidthMininet"],
                        cpu=config["cpuLimit"],
                        queue=config["queue"],
                        remoteController=config["remoteController"])
	else:
		print "Error: Unknown topology type."
		sys.exit(1)

	RunTest(net=net, remoteController=config["remoteController"])

	os.system('sudo mn -c')

