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

