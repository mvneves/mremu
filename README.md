# Hadoop Emulator

# Instalation 

## Mininet

Install git

	sudo apt-get install git

Clone mininet repository

	git clone git://github.com/mininet/mininet
	cd mininet
	git checkout -b 2.1.0 2.1.0
	cd ..

Edit file mininet/util/install.sh and modify oflops.git URL (line 558):

	# change it for:
	git clone git://gitosis.stanford.edu/oflops.git

nstall mininet

	mininet/util/install.sh -a

Add mininet to user path

	export PATH=$PATH:$HOME/mininet/bin/

## Hadoop Emulator dependencies:

	sudo apt-get install bwm-ng iperf python-pip
	sudo pip install IPy


# Execution an experiment

This is an exempla of how to run a very simple exeperiment. More documentation will be provided soon.

Edit the configuration file:

	{
		"trace": "examples/input/job_201312301708_0016_trace.json",
		"mapping": "examples/input/hostname_mapping-16nodes.json",
		"output": "examples/job_201312301708_0016_result.json",
		"remoteController": false,
		"networkTopology": "singleswitch",
		"numSwitches": 1,
		"numHostsPerSwitch": 16,
		"numberTrunkLinks": 1,
		"bandwidthMininet": 100,
		"bandwidthTrace": 1000,
		"cpuLimit": -1,
		"queue": 100
	}

Run the emulator:

	./run.sh


References:

* NEVES, M.; DE ROSE, C. A. F.; KATRINIS, M. K.; MRemu: An Emulation-based Framework for Datacenter Network Experimentation using Realistic MapReduce Traffic. In: IEEE 23rd International Symposium on Modelling, Analysis and Simulation of Computer and Telecommunication Systems (MASCOTS 2015), Atlanta, USA. 2015
