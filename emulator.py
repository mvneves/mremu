from mininet.topo import Topo
from mininet.node import Controller, RemoteController, OVSKernelSwitch, CPULimitedHost, OVSController
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

from network import DumbbellNet, SingleSwitchNet, SinglepathTreeNet, MultipathTreeNet, FatTreeNet

def RunTestHadoop(hosts):
	basedir = "./hadoop"
	done = "./output/done.json"
	emulator = basedir + "/emulator.py"

	if os.path.isfile(done):
		os.remove(done)

	print "Starting monitor ..."
	output_dir="output"
	monitor = multiprocessing.Process(target = monitor_devs_ng, args =
                ('%s/rate.txt' % output_dir, 0.01))
	monitor.start()

	print "Running Hadoop simulation ..."
	for h in hosts:
		h.popen('%s %s > ./output/output-%s.txt 2> ./output/error-%s.txt' % (emulator, h.IP(), h.IP(), h.IP()), shell = True)
	while True:
		if os.path.isfile(done):
			break
		sleep(1)

	print "Stopping monitor ..."
	monitor.terminate()
        os.system("killall -9 bwm-ng")

	sleep(5)
	print "Done."

def RunTestIperf(hosts):
	basedir = "./benchmarks"
	done = "./output/done.json"
	emulator = basedir + "/emulator.py"

	if os.path.isfile(done):
		os.remove(done)

	print "Starting monitor ..."
	output_dir="output"
	monitor = multiprocessing.Process(target = monitor_devs_ng, args =
                ('%s/rate.txt' % output_dir, 0.01))
	monitor.start()

	print "Running tests ..."
	for h in hosts:
		h.popen('%s %s > ./output/output-%s.txt 2> ./output/error-%s.txt' % (emulator, h.IP(), h.IP(), h.IP()), shell = True)
	while True:
		if os.path.isfile(done):
			break
		sleep(1)

	print "Stopping monitor ..."
	monitor.terminate()
        os.system("killall -9 bwm-ng")

	sleep(5)
	print "Done."

def RunTest(net=None, remoteController=False):
	net.start()

	# wait for the switches to connect to the controller
	info('** Waiting for switches to connect to the controller\n')
	sleep(5)
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

        RunTestHadoop(hosts)
        #RunTestIperf(hosts)

	#CLI(net)
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

	RunTest(net=net, remoteController=config["remoteController"])

	os.system('sudo mn -c')

