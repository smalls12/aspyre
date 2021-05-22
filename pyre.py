import uuid
import logging

# local modules
from pyre_node import PyreNode
from pyre_event import PyreEvent

import asyncio

import zmq.asyncio
from zmq.asyncio import Context

logger = logging.getLogger(__name__)

raw_input = input  # Python 3

class Pyre(object):

    def __init__(self, receiver, name=None, ctx=None, *args, **kwargs):
        """Constructor, creates a new Zyre node. Note that until you start the
        node it is silent and invisible to other nodes on the network.
        The node name is provided to other nodes during discovery. If you
        specify NULL, Zyre generates a randomized node name from the UUID.

        Args:
            name (str): The name of the node

        Kwargs:
            ctx: PyZMQ Context, if not specified a new context will be created
        """
        super(Pyre, self).__init__(*args, **kwargs)
        ctx = kwargs.get('ctx')
        if ctx == None:
            ctx = zmq.Context()
        self._ctx = ctx
        self._uuid = None
        self._receiver = receiver
        self._name = name
        self.engine = None
        self.verbose = True
                
    async def __aenter__(self):
        await self.start(self._receiver)
        return self
    
    async def __aexit__(self, type, value, traceback):
        await self.stop()

    #def __del__(self):
        # We need to explicitly destroy the actor
        # to make sure our node thread is stopped
        #self.actor.destroy()

    def __bool__(self):
        "Determine whether the object is valid by converting to boolean"
        return True  #TODO

    def uuid(self):
        """Return our node UUID string, after successful initialization"""
        if not self._uuid:
            self._uuid = uuid.UUID(bytes=self.node.identity.bytes)
        return self._uuid

    # Return our node name, after successful initialization
    def name(self):
        """Return our node name, after successful initialization"""
        if not self._name:
            self._name = self.node.name
        return self._name

    def set_header(self, key, value):
        """Set node header; these are provided to other nodes during discovery
        and come in each ENTER message."""
        self.node.header.update({key: value})

    def set_verbose(self):
        """Set verbose mode; this tells the node to log all traffic as well as
        all major events."""
        self.node.verbose = True

    def set_port(self, port_nbr):
        """Set UDP beacon discovery port; defaults to 5670, this call overrides
        that so you can create independent clusters on the same network, for
        e.g. development vs. production. Has no effect after zyre_start()."""
        self.node.beacon_port = port_nbr

    def set_interval(self, interval):
        """Set UDP beacon discovery interval, in milliseconds. Default is instant
        beacon exploration followed by pinging every 1,000 msecs."""
        self.node.interval = interval

    def set_interface(self, value):
        """Set network interface for UDP beacons. If you do not set this, CZMQ will
        choose an interface for you. On boxes with several interfaces you should
        specify which one you want to use, or strange things can happen."""
        logging.debug("set_interface not implemented") #TODO
    
    async def start(self, receiver):
        """Start node, after setting header values. When you start a node it
        begins discovery and connection. Returns 0 if OK, -1 if it wasn't
        possible to start the node."""
        self.node = PyreNode(receiver)

        # Send name, if any, to node backend
        if (self._name):
            self.node.name = self._name

        await self.node.start()
        self.engine = asyncio.create_task(self.node.run()) 

    async def stop(self):
        """Stop node; this signals to other peers that this node will go away.
        This is polite; however you can also just destroy the node without
        stopping it."""
        await self.node.stop()
        await self.engine

    async def join(self, groupname):
        """Join a named group; after joining a group you can send messages to
        the group and all Zyre nodes in that group will receive them."""
        await self.node.join(groupname)

    async def leave(self, groupname):
        """Leave a group"""
        await self.node.leave(groupname)

    # Send message to single peer; peer ID is first frame in message
    async def whisper(self, peer, content):
        """Send message to single peer, specified as a UUID string
        Destroys message after sending"""
        await self.node.whisper(peer, content)

    async def shout(self, groupname, content):
        """Send message to a named group
        Destroys message after sending"""
        await self.node.shout(groupname, content)

    def get_peers(self):
        """Return list of current peer ids."""
        return self.node.get_peers()

    def peers_by_group(self, groupname):
        """Return list of current peer ids."""
        return list(self.node.require_peer_group(groupname).peers.keys())

    def endpoint(self):
        """Return own endpoint"""
        return self.node.endpoint

    def recent_events(self):
        """Iterator that yields recent `PyreEvent`s"""
        while self.socket().get(zmq.EVENTS) & zmq.POLLIN:
            yield PyreEvent(self)

    def events(self):
        """Iterator that yields `PyreEvent`s indefinitely"""
        while True:
            yield PyreEvent(self)

    def peer_address(self, peer):
        """Return the endpoint of a connected peer."""
        return self.node.peers.get(peer).get_endpoint()

    def peer_header_value(self, peer, name):
        """Return the value of a header of a conected peer.
        Returns null if peer or key doesn't exist."""
        return self.node.peers.get(peer).get_header(name)

    def peer_headers(self, peer):
        """Return the value of a header of a conected peer.
        Returns null if peer or key doesn't exist."""
        return self.node.peers.get(peer).get_headers()

    def own_groups(self):
        """Return list of currently joined groups."""
        return list(self.node.own_groups.keys())

    def peer_groups(self):
        """Return list of groups known through connected peers."""
        return list(self.node.peer_groups.keys())

    # Return node socket, for direct polling of socket
    def socket(self):
        """Return socket for talking to the Zyre node, for polling"""
        return self.inbox

    @staticmethod
    def version():
        return __version_info__
