
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

BEACON_MAX = 255      # Max size of beacon data
MULTICAST_GRP = '225.25.25.25'
ENETDOWN = 50   #socket error, network is down
ENETUNREACH = 51 #socket error, network unreachable

class BeaconInterface():
    def __init__(self, address, network_address, broadcast_address, interface_name):
        self.address = address
        self.network_address = network_address
        self.broadcast_address = broadcast_address
        self.interface_name = interface_name

class BeaconInterfaceUtility():
    def __init__(self, **kwargs):
        self.name = kwargs["config"]["general"]["name"]
        self.logger = logging.getLogger("aspyre").getChild(self.name)
    
    def find_interface(self, interface_name):
        return self._find_interface(interface_name)
    
    def _validate_interface(self, name, data):
        self.logger.debug("Checking out interface {0}.".format(name))
        # For some reason the data we need lives in the "2" section of the interface.
        data_2 = data.get(2)

        if not data_2:
            self.logger.debug("No data_2 found for interface {0}.".format(name))
            return None

        address_str = data_2.get("addr")
        netmask_str = data_2.get("netmask")

        if not address_str or not netmask_str:
            self.logger.debug("Address or netmask not found for interface {0}.".format(name))
            return None

        if isinstance(address_str, bytes):
            address_str = address_str.decode("utf8")

        if isinstance(netmask_str, bytes):
            netmask_str = netmask_str.decode("utf8")

        interface_string = "{0}/{1}".format(address_str, netmask_str)

        interface = ipaddress.ip_interface(u(interface_string))

        if interface.is_link_local:
            self.logger.debug("Interface {0} is a link-local device.".format(name))
            return None

        return BeaconInterface(interface.ip, interface.network.network_address, interface.network.broadcast_address, name)

    def _find_interface(self, interface_name):
        netinf = zhelper.get_ifaddrs()

        self.logger.debug("Available interfaces: {0}".format(netinf))

        # try and find selected interface
        for iface in netinf:
            for _name, _data in iface.items():
                if _name == interface_name:
                    # found chosen interface
                    valid = self._validate_interface(_name, _data)
                    if valid is not None:
                        return valid
                    else:
                        self.logger.error(f"Unable to use interface [{interface_name}]")
        
        self.logger.warning("Searching for any avilable NIC")

        # just find any available interface
        for iface in netinf:
            # Loop over the interfaces and their settings to try to find the broadcast address.
            # ipv4 only currently and needs a valid broadcast address
            for name, data in iface.items():
                if self._validate_interface(name, data):
                    return
        
        raise Exception("No avilable NICs")

class AspyreAsyncBeaconReceiver():
    """
    string
    """
    def __init__(self, name, transmit, peers):
        """
        string
        """
        self._name = name
        self._logger = logging.getLogger("aspyre").getChild(self._name)

        self._transmit = transmit
        self._filter = struct.pack("ccc", b'Z', b'R', b'E')

        self._peers = peers

    def connection_made(self, _):
        """
        string
        """
        self._logger.debug("connection made")
    
    @property
    def transmit(self):
        """
        we want to provide access to the transmit variable so
        that we can set it to the zeroized beacon
        """
        return self._transmit
    
    @transmit.setter
    def transmit(self, transmit):
        """
        we want to provide access to the transmit variable so
        that we can set it to the zeroized beacon
        """
        self._transmit = transmit

    def datagram_received(self, frame, addr):
        """
        string
        """       
        #  If filter is set, check that beacon matches it
        is_valid = False
        if self._filter is not None:
            if len(self._filter) <= len(frame):
                match_data = frame[:len(self._filter)]
                if match_data == self._filter:
                    is_valid = True

        self._logger.debug(f"Received beacon [{frame}] from [{addr}]")

        #  If valid, discard our own broadcasts, which UDP echoes to us
        if is_valid and self._transmit:
            if frame == self._transmit:
                is_valid = False

        #  If still a valid beacon, send on to the API
        if is_valid:
            # even though this is async
            # it doesn't have the async wrapping
            # we'll need to throw this at asyncio to get a task
            asyncio.create_task(self._process_beacon(frame, addr))
    
    # by default we are not expecting a curbe public key
    def _unpack_beacon(self, frame):
        return struct.unpack('cccb16sH', frame)
    
    async def _update_peer(self, beacon, addr, peer_id, port):
        _endpoint = "tcp://%s:%d" %(addr[0], port)
        _peer = await self._peers.require_peer(peer_id, _endpoint)
        _peer.refresh()

    async def _process_beacon(self, frame, addr):
        _beacon = self._unpack_beacon(frame)
        # Ignore anything that isn't a valid beacon
        if _beacon[3] != BEACON_VERSION:
            self._logger.warning("Invalid ZRE Beacon version: {0}".format(_beacon[3]))
            return

        _peer_id = uuid.UUID(bytes=_beacon[4])
        _port = socket.ntohs(_beacon[5])
        # if we receive a beacon with port 0 this means the peer exited
        if _port:
            await self._update_peer(_beacon, addr, _peer_id, _port)
        else:
            # Zero port means peer is going away; remove it if
            # we had any knowledge of it already
            peer = self._peers.peers.get(_peer_id)
            # remove the peer (delete)
            if peer:
                self._logger.debug("Received 0 port beacon, removing peer {0}".format(_peer_id))
                await self._peers.remove_peer(peer)

            else:
                self._logger.warning(self._peers)
                self._logger.warning("We don't know peer id {0}".format(_peer_id))
    
    def error_received(self, exc):
        """
        string
        """
        self._logger.error(f"Error received: {exc}")

    def connection_lost(self, exc):
        """
        string
        """
        self._logger.error(f"Connection closed :: {exc}")

class AspyreAsyncBeaconNoEncryptionReceiver(AspyreAsyncBeaconReceiver):
    def __init__(self, name, transmit, peers):
        super().__init__(name, transmit, peers)

class AspyreAsyncBeaconEncryptionReceiver(AspyreAsyncBeaconReceiver):
    def __init__(self, name, transmit, peers):
        super().__init__(name, transmit, peers)
    
    def _unpack_beacon(self, frame):
        return struct.unpack('cccb16sH40s', frame)
    
    async def _update_peer(self, beacon, addr, peer_id, port):
        endpoint = "tcp://%s:%d" %(addr[0], port)
        _server_public_key = beacon[6]
        peer = await self._peers.require_peer(peer_id, endpoint, _server_public_key)
        peer.refresh()

class AspyreAsyncBeacon():

    def __init__(self, port, peers, **kwargs):
        self.name = kwargs["config"]["general"]["name"]
        self._identity = kwargs["config"]["general"]["identity"]
        self.logger = logging.getLogger("aspyre").getChild(self.name)

        self._port = port

        self._peers = peers

        self.udpsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                                      #  UDP socket for send/recv

        self.port_nbr = kwargs["config"]["beacon"]["port"]             #  UDP port number we work on
        self.interval = kwargs["config"]["beacon"]["interval"]  #  Beacon broadcast interval
        self.ping_at = time.time()    #  Next broadcast time

        self._terminated = False       #  Did caller ask us to quit?
        self.hostname = ""            #  Saved host name

        self._interface_name = kwargs["config"]["beacon"]["interface_name"]

        self.address = None
        self.network_address = None
        self.broadcast_address = None
    
    def _build_beacon(self):
        return struct.pack('cccb16sH', b'Z', b'R', b'E',
                                     BEACON_VERSION, self._identity.bytes,
                                     socket.htons(self._port))

    def _build_zeroized_beacon(self):
        return struct.pack('cccb16sH', b'Z', b'R', b'E',
                                        BEACON_VERSION, self._identity.bytes,
                                        socket.htons(0))
    
    def _build_beacon_receiver(self, transmit):
        return AspyreAsyncBeaconReceiver(self.name, transmit, self._peers)
    
    async def run(self, interface):
        self.start(interface)
        try:
            _transmit = self._build_beacon()
            
            _beaconReceiver = self._build_beacon_receiver(_transmit)
            
            # this will receive asynchronously on its own
            _transport, _protocol = await asyncio.get_event_loop().create_datagram_endpoint(
                lambda: _beaconReceiver,
                sock=self.udpsock)
            
            while not self._terminated:
                # keep looping
                # send the beacon at interval
                await self.send_beacon(interface, _transport, _transmit)
                # sleep interval
                await asyncio.sleep(1.0)
        finally:
            self.logger.debug("Beacon closing...")
            # we still want to force out one last beacon to inform peers
            # we are leaving
            _transmit = self._build_zeroized_beacon()
            _beaconReceiver.transmit = _transmit
            await self.send_beacon(interface, _transport, _transmit)
            self.udpsock.close()
    
    def start(self, interface):
        self._bind_interface(interface)

    def stop(self):
        self._terminated = True
    
    def _bind_interface(self, interface):
        try:
            self.udpsock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.udpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            #  On some platforms we have to ask to reuse the port
            try:
                self.udpsock.setsockopt(socket.SOL_SOCKET,
                                        socket.SO_REUSEPORT, 1)

            except AttributeError:
                pass

            if interface.broadcast_address.is_multicast:
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

                group = socket.inet_aton("{0}".format(interface.broadcast_address))
                mreq = struct.pack('4sl', group, socket.INADDR_ANY)

                self.udpsock.setsockopt(socket.SOL_IP,
                                        socket.IP_ADD_MEMBERSHIP, mreq)

            else:
                # Platform specifics
                if platform.startswith("linux"):
                    # on linux we bind to the broadcast address and send to
                    # the broadcast address
                    self.udpsock.bind((str(interface.broadcast_address),
                                       self.port_nbr))
                else:
                    self.udpsock.bind(("", self.port_nbr))

                self.logger.debug("Set up a broadcast beacon to {0}:{1}".format(interface.broadcast_address, self.port_nbr))
        except socket.error:
            self.logger.exception("Initializing of {0} raised an exception".format(self.__class__.__name__))
    
    async def send_beacon(self, interface, transport, data):
        self.logger.debug(f"Send beacon [{data}] from [{(str(interface.broadcast_address), self.port_nbr)}]")
        try:
            transport.sendto(data, (str(interface.broadcast_address), self.port_nbr))
        except OSError as e:
            
            # network down, just wait, it could come back up again.
            # socket call errors 50 and 51 relate to the network being
            # down or unreachable, the recommended action to take is to 
            # try again so we don't terminate in these cases.
            if e.errno in [ENETDOWN, ENETUNREACH]: pass
            
            # all other cases, we'll terminate
            else:
                self.logger.debug("Network seems gone, exiting zbeacon")
                self._terminated = True
                
        except socket.error:
            self.logger.debug("Network seems gone, exiting zbeacon")
            self._terminated = True

class AspyreAsyncBeaconPlainText(AspyreAsyncBeacon):
    def __init__(self, port, peers, **kwargs):
        super().__init__(port, peers, **kwargs)

class AspyreAsyncBeaconEncrypted(AspyreAsyncBeacon):
    def __init__(self, port, peers, **kwargs):
        super().__init__(port, peers, **kwargs)
        self._server_secret_file = kwargs["config"]["authentication"]["server_secret_file"]
    
    def _build_beacon(self):
        _server_public, _ = zmq.auth.load_certificate(self._server_secret_file)
        return struct.pack('cccb16sH40s', b'Z', b'R', b'E',
                                     BEACON_VERSION, self._identity.bytes,
                                     socket.htons(self._port),
                                     _server_public)

    def _build_zeroized_beacon(self):
        _server_public, _ = zmq.auth.load_certificate(self._server_secret_file)
        return struct.pack('cccb16sH40s', b'Z', b'R', b'E',
                                        BEACON_VERSION, self._identity.bytes,
                                        socket.htons(0),
                                        _server_public)
    
    def _build_beacon_receiver(self, transmit):
        return AspyreAsyncBeaconEncryptionReceiver(self.name, transmit, self._peers)
    