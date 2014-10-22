#!/usr/bin/python

"""
Create a network and start sshd(8) on each host.
"""

import sys

def createBanner(host, intfSuffix="eth1"):
    """*** Create banner file for sshd"""
    f = open( '/tmp/%s.banner' % host.name, 'w' )
    f.write( '\nWelcome to %s at %s!\n\n' % ( host.name, host.IP(intf=host.intf(host.name + "-" + intfSuffix))))
    f.close()

def startSSH( network, cmd='/usr/sbin/sshd', opts='-D -o UseDNS=no -u0', intfSuffix="eth1"):
    """Run sshd on all hosts."""
    for host in network.hosts:
        createBanner(host, intfSuffix=intfSuffix)
        new_opts = opts + ' -o "Banner /tmp/%s.banner"' % host.name
        host.cmd( cmd + ' ' + new_opts + '&' )
    print
    print "*** Hosts are running sshd at the following addresses:"
    print
    for host in network.hosts:
        print host.name, host.IP(intf=host.intf(host.name + "-" + intfSuffix))
    print

def stopSSH(network, cmd='/usr/sbin/sshd'):
    for host in network.hosts:
        host.cmd( 'kill %' + cmd )

