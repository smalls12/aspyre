
# ======================================================================
#  zbeacon - LAN discovery and presence
#
#  Copyright (c) the Contributors as noted in the AUTHORS file.
#  This file is part of CZMQ, the high-level C binding for 0MQ:
#  http://czmq.zeromq.org.
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
    The zbeacon class implements a peer-to-peer discovery service for local
    networks. A beacon can broadcast and/or capture service announcements
    using UDP messages on the local area network. This implementation uses
    IPv4 UDP broadcasts. You can define the format of your outgoing beacons,
    and set a filter that validates incoming beacons. Beacons are sent and
    received asynchronously in the background.

    This class replaces zbeacon_v2, and is meant for applications that use
    the CZMQ v3 API (meaning, zsock).
"""

import logging
import ipaddress
import socket
import zmq
import struct
import time
from sys import platform
import uuid
from . import zhelper
from .zhelper import u

import asyncio

import zmq.asyncio
from zmq.asyncio import Context

BEACON_VERSION = 1

INTERVAL_DFLT = 1.0
BEACON_MAX = 255      # Max size of beacon data
MULTICAST_GRP = '225.25.25.25'
ENETDOWN = 50   #socket error, network is down
ENETUNREACH = 51 #socket error, network unreachable

class ZAsyncBeacon():

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.logger = logging.getLogger("aspyre").getChild(self.name)
        self.udpsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                                      #  UDP socket for send/recv

        self.port_nbr = 0             #  UDP port number we work on
        self.interval = INTERVAL_DFLT #  Beacon broadcast interval
        self.ping_at = time.time()    #  Next broadcast time

        self.terminated = False       #  Did caller ask us to quit?
        self.hostname = ""            #  Saved host name

        self.address = None
        self.network_address = None
        self.broadcast_address = None
        self.interface_name = None

    def __del__(self):
        if self.udpsock:
            self.udpsock.close()
    
    def start(self, identity, port_nbr):
        self.port_nbr = port_nbr
        self.prepare_udp()

    def stop(self, identity):
        pass

    def prepare_udp(self):
        self._prepare_socket()
        try:
            self.udpsock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.udpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            #  On some platforms we have to ask to reuse the port
            try:
                self.udpsock.setsockopt(socket.SOL_SOCKET,
                                        socket.SO_REUSEPORT, 1)

            except AttributeError:
                pass

            if self.broadcast_address.is_multicast:
                # TTL
                self.udpsock.setsockopt(socket.IPPROTO_IP,
                                        socket.IP_MULTICAST_TTL, 2)

                # TODO: This should only be used if we do not have inproc method!
                self.udpsock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

                # Usually, the system administrator specifies the
                # default interface multicast datagrams should be
                # sent from. The programmer can override this and
                # choose a concrete outgoing interface for a given
                # socket with this option.
                #
                # this results in the loopback address?
                # host = socket.gethostbyname(socket.gethostname())
                # self.udpsock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(host))
                # You need to tell the kernel which multicast groups
                # you are interested in. If no process is interested
                # in a group, packets destined to it that arrive to
                # the host are discarded.
                # You can always fill this last member with the
                # wildcard address (INADDR_ANY) and then the kernel
                # will deal with the task of choosing the interface.
                #
                # Maximum memberships: /proc/sys/net/ipv4/igmp_max_memberships
                # self.udpsock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
                #       socket.inet_aton("225.25.25.25") + socket.inet_aton(host))
                self.udpsock.bind(("", self.port_nbr))

                group = socket.inet_aton("{0}".format(self.broadcast_address))
                mreq = struct.pack('4sl', group, socket.INADDR_ANY)

                self.udpsock.setsockopt(socket.SOL_IP,
                                        socket.IP_ADD_MEMBERSHIP, mreq)

            else:
                # Platform specifics
                if platform.startswith("linux"):
                    # on linux we bind to the broadcast address and send to
                    # the broadcast address
                    self.udpsock.bind((str(self.broadcast_address),
                                       self.port_nbr))
                else:
                    self.udpsock.bind(("", self.port_nbr))

                self.logger.debug("Set up a broadcast beacon to {0}:{1}".format(self.broadcast_address, self.port_nbr))
        except socket.error:
            self.logger.exception("Initializing of {0} raised an exception".format(self.__class__.__name__))

    def _prepare_socket(self):
        netinf = zhelper.get_ifaddrs()

        self.logger.debug("Available interfaces: {0}".format(netinf))

        for iface in netinf:
            # Loop over the interfaces and their settings to try to find the broadcast address.
            # ipv4 only currently and needs a valid broadcast address
            for name, data in iface.items():
                self.logger.debug("Checking out interface {0}.".format(name))
                # For some reason the data we need lives in the "2" section of the interface.
                data_2 = data.get(2)

                if not data_2:
                    self.logger.debug("No data_2 found for interface {0}.".format(name))
                    continue

                address_str = data_2.get("addr")
                netmask_str = data_2.get("netmask")

                if not address_str or not netmask_str:
                    self.logger.debug("Address or netmask not found for interface {0}.".format(name))
                    continue

                if isinstance(address_str, bytes):
                    address_str = address_str.decode("utf8")

                if isinstance(netmask_str, bytes):
                    netmask_str = netmask_str.decode("utf8")

                interface_string = "{0}/{1}".format(address_str, netmask_str)

                interface = ipaddress.ip_interface(u(interface_string))

                if interface.is_loopback:
                    self.logger.debug("Interface {0} is a loopback device.".format(name))
                    continue

                if interface.is_link_local:
                    self.logger.debug("Interface {0} is a link-local device.".format(name))
                    continue

                self.address = interface.ip
                self.network_address = interface.network.network_address
                self.broadcast_address = interface.network.broadcast_address
                self.interface_name = name

            if self.address:
                break

        self.logger.debug("Finished scanning interfaces.")

        if not self.address:
            self.network_address = ipaddress.IPv4Address(u('127.0.0.1'))
            self.broadcast_address = ipaddress.IPv4Address(u(MULTICAST_GRP))
            self.interface_name = 'loopback'
            self.address = u('127.0.0.1')

        self.logger.debug("Address: {0}".format(self.address))
        self.logger.debug("Network: {0}".format(self.network_address))
        self.logger.debug("Broadcast: {0}".format(self.broadcast_address))
        self.logger.debug("Interface name: {0}".format(self.interface_name))

    def set_port(self, port_nbr):
        self.port_nbr = port_nbr
    
    def get_address(self):
        return self.address
    
    def get_socket(self):
        return self.udpsock
    
    def send_term(self):
        self.terminated = True

    async def send_beacon(self, transport, data):
        self.logger.debug(f"Send beacon [{data}] from [{(str(self.broadcast_address), self.port_nbr)}]")
        try:
            transport.sendto(data, (str(self.broadcast_address), self.port_nbr))
        except OSError as e:
            
            # network down, just wait, it could come back up again.
            # socket call errors 50 and 51 relate to the network being
            # down or unreachable, the recommended action to take is to 
            # try again so we don't terminate in these cases.
            if e.errno in [ENETDOWN, ENETUNREACH]: pass
            
            # all other cases, we'll terminate
            else:
                self.logger.debug("Network seems gone, exiting zbeacon")
                self.terminated = True
                
        except socket.error:
            self.logger.debug("Network seems gone, exiting zbeacon")
            self.terminated = True

