import uuid
import logging
import asyncio
import zmq.asyncio
from zmq.asyncio import Context

# local modules
from .pyre_node import PyreNode
from .pyre_event import PyreEvent

class Pyre():

    def __init__(self, name=None, ctx=None, *args, **kwargs):
        """Constructor, creates a new Zyre node. Note that until you start the
        node it is silent and invisible to other nodes on the network.
        The node name is provided to other nodes during discovery. If you
        specify NULL, Zyre generates a randomized node name from the UUID.

        Args:
            name (str): The name of the node

        Kwargs:
            ctx: PyZMQ Context, if not specified a new context will be created
        """
        self._identity = uuid.uuid4()                # Our UUID as object
        self._name = name
        if self._name is None:
            self._name = str(self._identity)[:6]          # Our public name (default=first 6 uuid chars)        
        self._logger = logging.getLogger("aspyre").getChild(self._name)

        ctx = kwargs.get('ctx')
        if ctx is None:
            ctx = Context.instance()
        self._ctx = ctx
        self._node = None
        self._inbox = None
        self._listening = False

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, type, value, traceback):
        await self.stop()

    def __del__(self):
        self._inbox.close()

    def uuid(self):
        """Return our node UUID string, after successful initialization"""
        return self._identity

    # Return our node name, after successful initialization
    def name(self):
        """Return our node name, after successful initialization"""
        return self._name

    def set_header(self, key, value):
        """Set node header; these are provided to other nodes during discovery
        and come in each ENTER message."""
        self._node.header.update({key: value})

    def set_port(self, port_nbr):
        """Set UDP beacon discovery port; defaults to 5670, this call overrides
        that so you can create independent clusters on the same network, for
        e.g. development vs. production. Has no effect after zyre_start()."""
        self._node.beacon_port = port_nbr

    def set_interval(self, interval):
        """Set UDP beacon discovery interval, in milliseconds. Default is instant
        beacon exploration followed by pinging every 1,000 msecs."""
        self._node.interval = interval

    def set_interface(self, value):
        """Set network interface for UDP beacons. If you do not set this, CZMQ will
        choose an interface for you. On boxes with several interfaces you should
        specify which one you want to use, or strange things can happen."""
        self.logger.debug("set_interface not implemented") #TODO

    async def start(self):
        """Start node, after setting header values. When you start a node it
        begins discovery and connection. Returns 0 if OK, -1 if it wasn't
        possible to start the node."""
        self._node = PyreNode(self._identity, self._name)
        
        self._inbox = self._ctx.socket(zmq.PULL)
        self._inbox.connect(f"inproc://events-{self._identity}")

        await self._node.start()

        return self

    async def stop(self):
        """Stop node; this signals to other peers that this node will go away.
        This is polite; however you can also just destroy the node without
        stopping it."""
        if self._node.engine_running:
            await self._node.stop()
            self._inbox.disconnect(f"inproc://events-{self._identity}")

    '''
    this will block the caller
    but won't block the asynchronous context

    keep receiving messages until the engine stops or
    specifically requested to stop vie ::stop_listening
    '''
    async def listen(self, receiver):
        self._listening = True
        while self._node.engine_running and self._listening:
            try:
                await receiver(self, await self.recv())
            except asyncio.TimeoutError:
                pass

    '''
    this will cause any current ::listen calls to end
    '''
    def stop_listening(self):
        self._listening = False

    # Receive next message from node
    async def recv(self):
        """Receive next message from network; the message may be a control
        message (ENTER, EXIT, JOIN, LEAVE) or data (WHISPER, SHOUT).
        """
        return await asyncio.wait_for(self._inbox.recv_multipart(), timeout=0.5)

    async def join(self, groupname):
        """Join a named group; after joining a group you can send messages to
        the group and all Zyre nodes in that group will receive them."""
        await self._node.join(groupname)

    async def leave(self, groupname):
        """Leave a group"""
        await self._node.leave(groupname)

    # Send message to single peer; peer ID is first frame in message
    async def whisper(self, peer, content):
        """Send message to single peer, specified as a UUID string
        Destroys message after sending"""
        await self._node.whisper(peer, content)

    async def shout(self, groupname, content):
        """Send message to a named group
        Destroys message after sending"""
        await self._node.shout(groupname, content)

    def get_peers(self):
        """Return list of current peer ids."""
        return self._node.get_peers()

    def peers_by_group(self, groupname):
        """Return list of current peer ids."""
        return list(self._node.require_peer_group(groupname).peers.keys())

    def endpoint(self):
        """Return own endpoint"""
        return self._node.endpoint

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
        return self._node.peers.get(peer).get_endpoint()

    def peer_header_value(self, peer, name):
        """Return the value of a header of a conected peer.
        Returns null if peer or key doesn't exist."""
        return self._node.peers.get(peer).get_header(name)

    def peer_headers(self, peer):
        """Return the value of a header of a conected peer.
        Returns null if peer or key doesn't exist."""
        return self._node.peers.get(peer).get_headers()

    def own_groups(self):
        """Return list of currently joined groups."""
        return list(self._node.own_groups.keys())

    def peer_groups(self):
        """Return list of groups known through connected peers."""
        return list(self._node.peer_groups.keys())

    @staticmethod
    def version():
        return __version_info__
