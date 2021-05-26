"""
string
"""

import uuid
import logging
import struct
import socket
import time
import json
import asyncio

import zmq.asyncio
from zmq.asyncio import Context

from .zbeacon import ZAsyncBeacon
from .zre_msg import ZreMsg
from .pyre_peer import PyrePeer
from .pyre_group import PyreGroup

# hmm ?
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

BEACON_VERSION = 1
ZRE_DISCOVERY_PORT = 5670
REAP_INTERVAL = 1.0  # Once per second

class PyreNodeBeaconReceiver():
    """
    string
    """
    def __init__(self, name, callback):
        """
        string
        """
        self._name = name
        self._logger = logging.getLogger("aspyre").getChild(self._name)
        self.callback = callback

    def connection_made(self, _):
        """
        string
        """
        self._logger.debug("connection made")

    def datagram_received(self, frame, addr):
        """
        string
        """
        # even though this is async
        # it doesn't have the async wrapping
        # we'll need to throw this at asyncio to get a task
        asyncio.create_task(self.callback(frame, addr))

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

class PyreNode():
    """
    string
    """
    def __init__(self, identity, name):
        """
        string
        """
        self._ctx = Context.instance()

        self._terminated = False                    # API shut us down
        self._beacon_port = ZRE_DISCOVERY_PORT       # Beacon port number
        self._interval = 0                           # Beacon interval 0=default
        self._beacon = None                          # Beacon actor
        self._beacon_receiver = None

        self._transmit = None
        self._filter = b""

        self._identity = identity                    # Our UUID as object
        self._name = name                            # Our public name (default=first 6 uuid chars)
        self._logger = logging.getLogger("aspyre").getChild(self._name)

        self._bound = False
        self._inbox = None
        self._outbox = None

        self._endpoint = ""                          # Our public endpoint
        self._port = 0                               # Our inbox port, if any
        self._status = 0                             # Our own change counter

        self._peers = {}                             # Hash of known peers, fast lookup
        self._peer_groups = {}                       # Groups that our peers are in
        self._own_groups = {}                        # Groups that we are in
        self._headers = {}                           # Our header values

        self._transport = None
        self._protocol = None

        self._engine_running = False
        self._engine = None

    @property
    def engine_running(self):
        """
        string
        """
        return self._engine_running

    @engine_running.setter
    def engine_running(self, var):
        """
        string
        """
        self._engine_running = var

    async def start(self):
        """
        string
        """
        # TODO: If application didn't bind explicitly, we grab an ephemeral port
        # on all available network interfaces. This is orthogonal to
        # beaconing, since we can connect to other peers and they will
        # gossip our endpoint to others.
        self._logger.debug("Start")
        if self._beacon_port:
            # Start beacon discovery
            self._beacon = ZAsyncBeacon(self._name)
            self._beacon_receiver = PyreNodeBeaconReceiver(self._name, self.recv_beacon)

            self._beacon.start(self._identity, self._beacon_port)

            # Our hostname is provided by zbeacon
            hostname = self._beacon.get_address()

            #if self._interval:
            #   self._beacon.set_interval(self._interval)

            self._inbox = self._ctx.socket(zmq.ROUTER)         # Our inbox socket (ROUTER)
            try:
                self._inbox.setsockopt(zmq.ROUTER_HANDOVER, 1)
            except AttributeError as e:
                logging.warning(f"ROUTER_HANDOVER needs zmq version >=4.1 but installed is {zmq.zmq_version()}")

            # Our hostname is provided by zbeacon
            self._port = self._inbox.bind_to_random_port("tcp://*")
            if self._port < 0:
                # Die on bad interface or port exhaustion
                raise Exception("Random port assignment for incoming messages failed.")
            else:
                self._bound = True
            
            self._transmit = struct.pack('cccb16sH', b'Z', b'R', b'E',
                                         BEACON_VERSION, self._identity.bytes,
                                         socket.htons(self._port))

            self._filter = struct.pack("ccc", b'Z', b'R', b'E')

            self._endpoint = "tcp://%s:%d" %(hostname, self._port)

            # this will receive asynchronously on its own
            self._transport, self._protocol = await asyncio.get_event_loop().create_datagram_endpoint(
                lambda: self._beacon_receiver,
                sock=self._beacon.get_socket())            

            self._outbox = self._ctx.socket(zmq.PUSH)
            self._outbox.bind(f"inproc://events-{self._identity}")

            self._engine_running = True
            self._engine = asyncio.create_task(self.run())

    async def stop(self):
        """
        string
        """
        self._logger.debug("Stopping")
        # this will stop the beacon, reaper and router receiver
        self._terminated = True
        # we still want to force out one last beacon to inform peers
        # we are leaving
        self._transmit = struct.pack('cccb16sH', b'Z', b'R', b'E',
                                     BEACON_VERSION, self._identity.bytes,
                                     socket.htons(0))
        await self._beacon.send_beacon(self._transport, self._transmit)
        # wait for engine to finish running
        await self._engine
        # close beacon socket
        self._beacon = None

        try:
            self._outbox.unbind(f"inproc://events-{self._identity}")
        except zmq.error.ZMQError as e:
            pass
        finally:
            self._outbox.close()

        # additional cleanup
        self._peers = None
        self._peer_groups = None
        self._own_groups = None

    def bind(self, endpoint):
        """
        string
        """
        self._logger.warning("Not implemented")

    async def join(self, groupname):
        """
        string
        """
        grp = self._own_groups.get(groupname)
        if not grp:
            # Only send if we're not already in group
            grp = PyreGroup(self._name, groupname)
            self._own_groups[groupname] = grp
            msg = ZreMsg(ZreMsg.JOIN)
            msg.set_group(groupname)
            self._status += 1
            msg.set_status(self._status)

            for peer in self._peers.values():
                await peer.send(msg)

            self._logger.debug("Node is joining group {0}".format(groupname))

    async def leave(self, groupname):
        """
        string
        """
        grp = self._own_groups.get(groupname)
        if grp:
            # Only send if we're actually in group
            msg = ZreMsg(ZreMsg.LEAVE)
            msg.set_group(groupname)
            self._status += 1
            msg.set_status(self._status)

            for peer in self._peers.values():
                await peer.send(msg)

            self._own_groups.pop(groupname)

            self._logger.debug("Node is leaving group {0}".format(groupname))

    async def shout(self, groupname, content):
        """
        string
        """
        # Get group to send message to
        msg = ZreMsg(ZreMsg.SHOUT)
        msg.set_group(groupname)
        msg.content = content  # request may contain multipart message

        if self._peer_groups.get(groupname):
            await self._peer_groups[groupname].send(msg)
        else:
            self._logger.warning("Group {0} not found.".format(groupname))

    async def whisper(self, peer_id, content):
        """
        string
        """
        # Send frame on out to peer's mailbox, drop message
        # if peer doesn't exist (may have been destroyed)
        if self._peers.get(peer_id):
            msg = ZreMsg(ZreMsg.WHISPER)
            msg.set_address(peer_id)
            msg.content = content
            await self._peers[peer_id].send(msg)
        else:
            self._logger.warning(f"Unknown peer [{peer_id}]")

    def get_peers(self):
        """
        string
        """
        return self._peers.keys()

    async def purge_peer(self, peer, endpoint):
        """
        string
        """
        if peer.get_endpoint() == endpoint:
            await self.remove_peer(peer)
            peer.disconnect()
            self._logger.debug("Purge peer: {0}{1}".format(peer, endpoint))

    # Find or create peer via its UUID string
    async def require_peer(self, peer_identity, endpoint):
        """
        string
        """
        peer = self._peers.get(peer_identity)
        if not peer:
            # Purge any previous peer on same endpoint
            for _, _peer in self._peers.copy().items():
                await self.purge_peer(_peer, endpoint)

            peer = PyrePeer(self._ctx, self._name, peer_identity)
            self._peers[peer_identity] = peer
            peer.set_origin(self._name)
            peer.connect(self._identity, endpoint)

            # Handshake discovery by sending HELLO as first message
            message = ZreMsg(ZreMsg.HELLO)
            message.set_endpoint(self._endpoint)
            message.set_groups(self._own_groups.keys())
            message.set_status(self._status)
            message.set_name(self._name)
            message.set_headers(self._headers)
            await peer.send(message)

        return peer

    #  Remove a peer from our data structures
    async def remove_peer(self, peer):
        """
        string
        """
        # Tell the calling application the peer has gone
        await self._outbox.send_multipart([
            "EXIT".encode('utf-8'),
            peer.get_identity().bytes,
            peer.get_name().encode('utf-8')
        ])
        self._logger.debug("({0}) EXIT name={1}".format(peer, peer.get_endpoint()))
        # Remove peer from any groups we've got it in
        for grp in self._peer_groups.values():
            grp.leave(peer)
        # To destroy peer, we remove from peers hash table (dict)
        self._peers.pop(peer.get_identity())

    # Find or create group via its name
    def require_peer_group(self, groupname):
        """
        string
        """
        grp = self._peer_groups.get(groupname)
        if not grp:
            # somehow a dict containing peers is passed if
            # I don't force the peers arg to an empty dict
            grp = PyreGroup(self._name, groupname, peers={})
            self._peer_groups[groupname] = grp

        return grp

    async def join_peer_group(self, peer, groupname):
        """
        string
        """
        grp = self.require_peer_group(groupname)
        grp.join(peer)
        # Now tell the caller about the peer joined group
        await self._outbox.send_multipart([
            "JOIN".encode('utf-8'),
            peer.get_identity().bytes,
            peer.get_name().encode('utf-8'),
            groupname.encode('utf-8')
        ])
        self._logger.debug(f"({self._name}) JOIN name={peer.get_name()} group={groupname}")
        return grp

    async def leave_peer_group(self, peer, groupname):
        """
        string
        """
        # Tell the caller about the peer joined group
        await self._outbox.send_multipart([
            "LEAVE".encode('utf-8'),
            peer.get_identity().bytes,
            peer.get_name().encode('utf-8'),
            groupname.encode('utf-8')
        ])
        # Now remove the peer from the group
        grp = self.require_peer_group(groupname)
        grp.leave(peer)
        self._logger.debug(f"({self._name}) LEAVE name={peer.get_name()} group={groupname}")

    # Here we handle messages coming from other peers
    async def run_router_receiver(self):
        """
        string
        """
        while not self._terminated:
            try:
                frames = await asyncio.wait_for(self._inbox.recv_multipart(), timeout=0.5)
                if frames is None:
                    continue
                # frames = await input_socket.recv_multipart()
            except asyncio.TimeoutError:
                continue

            zmsg = ZreMsg()
            zmsg.parse(frames)
            # Router socket tells us the identity of this peer
            # First frame is sender identity
            message_id = zmsg.id
            address = zmsg.get_address()
            self._logger.debug(f"Received {zmsg.get_command()} Message from {address}")
            # On HELLO we may create the peer if it's unknown
            # On other commands the peer must already exist
            peer = self._peers.get(address)
            if message_id == ZreMsg.HELLO:
                if peer:
                    # remove fake peers
                    if peer.get_ready():
                        await self.remove_peer(peer)
                    elif peer.endpoint == self._endpoint:
                        # We ignore HELLO, if peer has same endpoint as current node
                        continue

                peer = await self.require_peer(address, zmsg.get_endpoint())
                peer.set_ready(True)

            # Ignore command if peer isn't ready
            if not peer or not peer.get_ready():
                self._logger.warning("Peer {0} isn't ready".format(peer))
                continue

            if peer.messages_lost(zmsg):
                self._logger.warning(f"{self._identity} messages lost from {peer.identity}")
                await self.remove_peer(peer)
                continue

            # Now process each command
            if message_id == ZreMsg.HELLO:
                # Store properties from HELLO command into peer
                peer.set_name(zmsg.get_name())
                peer.set_headers(zmsg.get_headers())

                # Now tell the caller about the peer
                await self._outbox.send_multipart([
                    "ENTER".encode('utf-8'),
                    peer.get_identity().bytes,
                    peer.get_name().encode('utf-8'),
                    json.dumps(peer.get_headers()).encode('utf-8'),
                    peer.get_endpoint().encode('utf-8')
                ])
                self._logger.debug(f"({self._name}) ENTER name={peer.get_name()} endpoint={peer.get_endpoint()}")

                # Join peer to listed groups
                for grp in zmsg.get_groups():
                    await self.join_peer_group(peer, grp)
                # Now take peer's status from HELLO, after joining groups
                peer.set_status(zmsg.get_status())
            elif message_id == ZreMsg.WHISPER:
                # Pass up to caller API as WHISPER event
                await self._outbox.send_multipart([
                    "WHISPER".encode('utf-8'),
                    peer.get_identity().bytes,
                    peer.get_name().encode('utf-8')
                ] + zmsg.content)
            elif message_id == ZreMsg.SHOUT:
                # Pass up to caller API as WHISPER event
                await self._outbox.send_multipart([
                    "SHOUT".encode('utf-8'),
                    peer.get_identity().bytes,
                    peer.get_name().encode('utf-8'),
                    zmsg.get_group().encode('utf-8')
                ] + zmsg.content)
            elif message_id == ZreMsg.PING:
                await peer.send(ZreMsg(id=ZreMsg.PING_OK))
            elif message_id == ZreMsg.JOIN:
                await self.join_peer_group(peer, zmsg.get_group())
                assert zmsg.get_status() == peer.get_status()
            elif message_id == ZreMsg.LEAVE:
                await self.leave_peer_group(peer, zmsg.get_group())
                assert zmsg.get_status() == peer.get_status()
            # Activity from peer resets peer timers
            peer.refresh()

    async def recv_beacon(self, frame, addr):
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
            beacon = struct.unpack('cccb16sH', frame)
            # Ignore anything that isn't a valid beacon
            if beacon[3] != BEACON_VERSION:
                self._logger.warning("Invalid ZRE Beacon version: {0}".format(beacon[3]))
                return

            peer_id = uuid.UUID(bytes=beacon[4])
            #print("peerId: %s", peer_id)
            port = socket.ntohs(beacon[5])
            # if we receive a beacon with port 0 this means the peer exited
            if port:
                endpoint = "tcp://%s:%d" %(addr[0], port)
                peer = await self.require_peer(peer_id, endpoint)
                peer.refresh()
            else:
                # Zero port means peer is going away; remove it if
                # we had any knowledge of it already
                peer = self._peers.get(peer_id)
                # remove the peer (delete)
                if peer:
                    self._logger.debug("Received 0 port beacon, removing peer {0}".format(peer_id))
                    await self.remove_peer(peer)

                else:
                    self._logger.warning(self._peers)
                    self._logger.warning("We don't know peer id {0}".format(peer_id))

    # TODO: Handle gossip dat

    # We do this once a second:
    # - if peer has gone quiet, send TCP ping
    # - if peer has disappeared, expire it
    async def ping_peer(self, peer_id):
        """
        string
        """
        peer = self._peers.get(peer_id)
        if time.time() > peer.expired_at:
            self._logger.debug(f"({self._name}) peer expired name={peer.get_name()} endpoint={peer.get_endpoint()}")
            await self.remove_peer(peer)
        elif time.time() > peer.evasive_at:
            # If peer is being evasive, force a TCP ping.
            # TODO: do this only once for a peer in this state;
            # it would be nicer to use a proper state machine
            # for peer management.
            self._logger.debug(f"({self._name}) peer seems dead/slow name={peer.get_name()} endpoint={peer.get_endpoint()}")
            msg = ZreMsg(ZreMsg.PING)
            await peer.send(msg)

    # --------------------------------------------------------------------------
    # This is the actor that runs a single node; it uses one thread, creates
    # a zyre_node object at start and destroys that when finishing.
    async def run_reaper(self):
        """
        string
        """
        while not self._terminated:
            # keep looping
            # Ping all peers and reap any expired ones
            for peer_id in self._peers.copy().keys():
                await self.ping_peer(peer_id)
            # sleep interval
            await asyncio.sleep(REAP_INTERVAL)
            
    async def run_beacon(self):
        """
        string
        """
        while not self._terminated:
            # keep looping
            # send the beacon at interval
            await self._beacon.send_beacon(self._transport, self._transmit)
            # sleep interval
            await asyncio.sleep(1.0)

    async def run(self):
        """
        string
        """
        try:
            tasks = [
                self.run_beacon(),          # periodically send beacon
                self.run_reaper(),          # periodically poke peers
                self.run_router_receiver()  # receive incoming messages from peers
            ]

            await asyncio.gather(*tasks)
        finally:
            self._engine_running = False
