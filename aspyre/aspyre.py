"""
this is the user facing API
"""

import uuid
import logging
import asyncio
import zmq.asyncio
from zmq.asyncio import Context

# local modules
from .database import PeerDatabase, PeerDatabaseEncrypted, GroupDatabase

from .authentication import AspyreAuthenticationContext
from .authentication import AspyreClientCurveAuthentication, AspyreServerCurveAuthentication

from .factory import ClientSocketFactory, ServerSocketFactory
from .factory import EncryptedClientSocketFactory, EncryptedServerSocketFactory

from .beacon import AspyreAsyncBeacon, AspyreAsyncBeaconEncrypted, BeaconInterfaceUtility
from .router import AspyreNodeAsyncRouter
from .reaper import AspyreAsyncReaper

from .node import AspyreAsyncNode

ZRE_DISCOVERY_PORT = 5670
BEACON_TRANSMIT_INTERVAL = 1.0

REAPING_INTERVAL = 1.0

DEFAULT_CONFIG = {
    "config": {
        "general": {
            "identity": None,
            "name": None,
            "ctx": None,
            "headers": None
        },
        "authentication": None,
        "beacon": {
            "interface_name": "eth0",
            "port": ZRE_DISCOVERY_PORT,
            "interval": BEACON_TRANSMIT_INTERVAL
        },
        "reaper": {
            "interval": REAPING_INTERVAL
        },
        "router": {

        }
    }
}

class AspyreImpl():
    """
    responsible for constructing the various components of an aspyre
    instance.
    """
    def __init__(self, **kwargs):
        """
        Constructor

        Kwargs:
            ctx: PyZMQ Context, if not specified a new context will be created
        """
        self._config = DEFAULT_CONFIG
        try:
            for _name, _data in kwargs["config"]["general"].items():
                self._config["config"]["general"][_name] = _data
        except KeyError:
            pass

        try:
            for _name, _data in kwargs["config"]["beacon"].items():
                self._config["config"]["beacon"][_name] = _data
        except KeyError:
            pass
        
        self._identity = uuid.uuid4()
        self._config["config"]["general"]["identity"] = self._identity

        if self._config["config"]["general"]["name"] is None:
            self._config["config"]["general"]["name"] = str(self._identity)[:6]

        self._logger = logging.getLogger("aspyre").getChild(self._config["config"]["general"]["name"])

        if self._config["config"]["general"]["ctx"] is None:
            self._config["config"]["general"]["ctx"] = Context.instance()

        self._node = None
        self._interface = None

        self._authenticator = None

        self._inbox = None
        self._outbox = None

        self._listening = False

        self._own_groups = None
        self._peers = None
        self._peer_groups = None

        self._running = None
        self._running_task = None

    async def __aenter__(self):
        """
        asynchronous context manager entry
        """
        return await self.run()

    async def __aexit__(self, type, value, traceback):
        """
        asynchronous context manager exit
        """
        await self.stop()

    def __del__(self):
        self._inbox.close()

    def uuid(self):
        """
        Return our node UUID string
        """
        return self._identity

    @property
    def name(self):
        """
        Return our node name
        """
        return self._name

    async def run(self):
        """
        responsible for starting the asynchronous engine
        """
        # check for interface
        _beacon_interface_utility = BeaconInterfaceUtility(**self._config)
        self._interface = _beacon_interface_utility.find_interface(self._config["config"]["beacon"]["interface_name"])

        self.start()

        self._running = True
        self._running_task = asyncio.create_task(self._node.run(self._interface))

        return self

    def _setup_socket_factories(self):
        _server_factory = ServerSocketFactory(
            self._config["config"]["general"]["ctx"]
        )

        _client_factory = ClientSocketFactory(
            self._config["config"]["general"]["ctx"]
        )

        return _server_factory, _client_factory

    def _setup_peer_database(self, factory, endpoint):
        # build peer database
        return PeerDatabase(
            factory,
            endpoint,           # seemingly unneccesary
                                # used in the HELLO message
                                # used for filtering a scenario that might never happen?
            self._outbox,       # the channel back to the user
            self._own_groups,   # the groups this node is joined to
            self._peer_groups,  # the groups peers are joined to
            **self._config      # other node configuration details
        )

    def _setup_beacon(self, port):
        return AspyreAsyncBeacon(
            port,               # the beacon needs to know the router endpoint port for broadcasting
            self._peers,        # the beacon will add and remove peers based off of the beacons
                                # it receives
            **self._config      # other node configuration details
        )

    def start(self):
        """Start node, after setting header values. When you start a node it
        begins discovery and connection. Returns 0 if OK, -1 if it wasn't
        possible to start the node."""
        _server_factory, _client_factory = self._setup_socket_factories()

        _socket = _server_factory.get_socket()

        _port = _socket.bind_to_random_port(f"tcp://{self._interface.address}")
        if _port < 0:
            # Die on bad interface or port exhaustion
            raise Exception("Random port assignment for incoming messages failed.")

        _endpoint = "tcp://%s:%d" %(self._interface.address, _port)

        # build node outbox
        self._outbox = self._config["config"]["general"]["ctx"].socket(zmq.PUSH)
        self._outbox.bind("inproc://events-{}".format(self._identity))

        # build node inbox
        self._inbox = self._config["config"]["general"]["ctx"].socket(zmq.PULL)
        self._inbox.connect("inproc://events-{}".format(self._identity))

        # build database of groups we belong to
        self._own_groups = GroupDatabase(
            **self._config
        )

        # build database of groups peers belong to
        self._peer_groups = GroupDatabase(
            **self._config
        )

        # build peer database
        self._peers = self._setup_peer_database(
            _client_factory,
            _endpoint
        )

        # build the beacon
        _beacon = self._setup_beacon(_port)

        # build router
        _router = AspyreNodeAsyncRouter(
            _socket,            # take the router socket and wrap it for send/receive
            _endpoint,
            self._outbox,       # the channel back to the user
            self._peers,        # the HELLO could come in before the beacon
                                # will create peers in this case
            self._peer_groups,
            **self._config      # other node configuration details
        )

        # build reaper
        _reaper = AspyreAsyncReaper(
            self._peers,        # check every peer if it needs to be ping'd
            **self._config      # other node configuration details
        )

        # build node
        print(self._config)
        self._node = AspyreAsyncNode(
            _beacon,
            _router,
            _reaper,
            self._outbox,
            self._own_groups,
            self._peers,
            self._peer_groups,
            **self._config      # other node configuration details
        )

    async def stop(self):
        """Stop node; this signals to other peers that this node will go away.
        This is polite; however you can also just destroy the node without
        stopping it."""
        if self._running:
            await self._node.stop()
            await self._running_task
            self._inbox.disconnect("inproc://events-{}".format(self._identity))

    async def listen(self, receiver):
        """
        this will block the caller
        but won't block the asynchronous context

        keep receiving messages until the engine stops or
        specifically requested to stop vie ::stop_listening
        """
        self._listening = True
        while self._listening:
            try:
                await receiver(self, await self.recv())
            except asyncio.TimeoutError:
                pass

    def stop_listening(self):
        """
        this will cause any current ::listen calls to end
        """
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
        return list(self._peers.peers.keys())

    def peers_by_group(self, groupname):
        """Return list of current peer ids."""
        return list(self._peer_groups[groupname].peers.keys())

    def endpoint(self):
        """Return own endpoint"""
        return self._node._endpoint

    def peer_address(self, peer):
        """Return the endpoint of a connected peer."""
        return self._peers.peers.get(peer).get_endpoint()

    def peer_header_value(self, peer, name):
        """Return the value of a header of a conected peer.
        Returns null if peer or key doesn't exist."""
        return self._peers.peers.get(peer).get_header(name)

    def peer_headers(self, peer):
        """Return the value of a header of a conected peer.
        Returns null if peer or key doesn't exist."""
        return self._peers.peers.get(peer).get_headers()

    def own_groups(self):
        """Return list of currently joined groups."""
        return list(self._own_groups.groups.keys())

    def peer_groups(self):
        """Return list of groups known through connected peers."""
        return list(self._peer_groups.groups.keys())

class Aspyre(AspyreImpl):
    """
    just an alias for the base implementation
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

class AspyreEncrypted(AspyreImpl):
    """
    overload the base implementation for encryption
    """
    def __init__(self, authentication, **kwargs):
        super().__init__(**kwargs)
        self._config["config"]["authentication"] = authentication

    def _setup_socket_factories(self):
        _authentication_context = AspyreAuthenticationContext(
            self._config["config"]["general"]["ctx"],
            self._interface,
            self._config["config"]["authentication"]["public_keys_dir"]
        )

        _server_authenticator = AspyreServerCurveAuthentication(
            self._config["config"]["general"]["ctx"],
            self._config["config"]["authentication"]["server_secret_file"]
        )

        _client_authenticator = AspyreClientCurveAuthentication(
            self._config["config"]["general"]["ctx"],
            self._config["config"]["authentication"]["client_secret_file"]
        )

        _server_factory = EncryptedServerSocketFactory(
            self._config["config"]["general"]["ctx"],
            _server_authenticator
        )

        _client_factory = EncryptedClientSocketFactory(
            self._config["config"]["general"]["ctx"],
            _client_authenticator
        )

        return _server_factory, _client_factory

    def _setup_peer_database(self, factory, endpoint):
        # build peer database
        return PeerDatabaseEncrypted(
            factory,
            endpoint,           # seemingly unneccesary
                                # used in the HELLO message
                                # used for filtering a scenario that might never happen?
            self._outbox,       # the channel back to the user
            self._own_groups,   # the groups this node is joined to
            self._peer_groups,  # the groups peers are joined to
            **self._config      # other node configuration details
        )

    def _setup_beacon(self, port):
        """
        overload the beacon setup for retrieving the server
        public key from the beacon data
        """
        return AspyreAsyncBeaconEncrypted(
            port,               # the beacon needs to know the router endpoint port for broadcasting
            self._peers,        # the beacon will add and remove peers based off of the beacons
                                # it receives
            **self._config      # other node configuration details
        )
