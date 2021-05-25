import uuid
import logging
import struct
import socket
import time

from .zbeacon import ZAsyncBeacon
from .zre_msg import ZreMsg
from .pyre_peer import PyrePeer
from .pyre_group import PyreGroup

import json

import asyncio

import zmq.asyncio
from zmq.asyncio import Context

# hmm ?
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

BEACON_VERSION = 1
ZRE_DISCOVERY_PORT = 5670
REAP_INTERVAL = 1.0  # Once per second

class PyreNodeBeaconReceiver():
    def __init__(self, name, callback):
        self._name = name
        self.logger = logging.getLogger("aspyre").getChild(self._name)
        self.callback = callback
    
    def connection_made(self, transport):
        self.logger.debug("connection made")

    def datagram_received(self, frame, addr):
        # even though this is async
        # it doesn't have the async wrapping
        # we'll need to throw this at asyncio to get a task
        task = asyncio.create_task(self.callback(frame, addr))
    
    def error_received(self, exc):
        self.logger.error('Error received:', exc)

    def connection_lost(self, exc):
        self.logger.error("Connection closed")

class PyreNode():

    def __init__(self, identity, name, *args, **kwargs):
        
        self._ctx = Context.instance()

        self._terminated = False                    # API shut us down
        self.beacon_port = ZRE_DISCOVERY_PORT       # Beacon port number
        self.interval = 0                           # Beacon interval 0=default
        self.beacon = None                          # Beacon actor
        self.beacon_receiver = None

        self.engine_running = False

        self.transmit = None
        self.filter = b""

        self._identity = identity                    # Our UUID as object
        self._name = name                            # Our public name (default=first 6 uuid chars)
        self.logger = logging.getLogger("aspyre").getChild(self._name)

        self.bound = False
        self.inbox = None
        self._outbox = None
       
        self.endpoint = ""                          # Our public endpoint
        self.port = 0                               # Our inbox port, if any
        self.status = 0                             # Our own change counter
        self.peers = {}                             # Hash of known peers, fast lookup
        self.peer_groups = {}                       # Groups that our peers are in
        self.own_groups = {}                        # Groups that we are in
        self.headers = {}                           # Our header values

        self.transport = None
        self.protocol = None

        self.engine = None

        # TODO: gossip stuff
        # self.start()

    def __del__(self):
        pass

    async def start(self):
        # TODO: If application didn't bind explicitly, we grab an ephemeral port
        # on all available network interfaces. This is orthogonal to
        # beaconing, since we can connect to other peers and they will
        # gossip our endpoint to others.
        self.logger.debug("Start")
        if self.beacon_port:
            # Start beacon discovery
            self.beacon = ZAsyncBeacon(self._name)
            self.beacon_receiver = PyreNodeBeaconReceiver(self._name, self.recv_beacon)

            self.beacon.start(self._identity, self.beacon_port)

            # Our hostname is provided by zbeacon
            hostname = self.beacon.get_address()

            #if self.interval:
            #   self.beacon.set_interval(self.interval)

            self.inbox = self._ctx.socket(zmq.ROUTER)         # Our inbox socket (ROUTER)
            try:
                self.inbox.setsockopt(zmq.ROUTER_HANDOVER, 1)
            except AttributeError as e:
                logging.warning("can't set ROUTER_HANDOVER, needs zmq version >=4.1 but installed is {0}".format(zmq.zmq_version()))

            # Our hostname is provided by zbeacon
            self.port = self.inbox.bind_to_random_port("tcp://*")
            if self.port < 0:
                # Die on bad interface or port exhaustion
                logging.critical("Random port assignment for incoming messages failed. Exiting.")
                sys.exit(-1)
            else:
                self.bound = True
            
            self.transmit = struct.pack('cccb16sH', b'Z', b'R', b'E',
                           BEACON_VERSION, self._identity.bytes,
                           socket.htons(self.port))
                    
            self.filter = struct.pack("ccc", b'Z', b'R', b'E')

            self.endpoint = "tcp://%s:%d" %(hostname, self.port)

            # this will receive asynchronously on its own
            self.transport, self.protocol = await asyncio.get_event_loop().create_datagram_endpoint(
                lambda: self.beacon_receiver,
                sock=self.beacon.get_socket())            
            
            self._outbox = self._ctx.socket(zmq.PUSH)
            self._outbox.bind(f"inproc://events-{self._identity}")
            
            self.engine_running = True
            self.engine = asyncio.create_task(self.run())

    async def stop(self):
        self.logger.debug("Stopping")
        # this will stop the beacon, reaper and router receiver
        self._terminated = True
        # we still want to force out one last beacon to inform peers
        # we are leaving
        self.transmit = struct.pack('cccb16sH', b'Z',b'R',b'E',
                    BEACON_VERSION, self._identity.bytes,
                    socket.htons(0))
        await self.beacon.send_beacon(self.transport, self.transmit)
        # wait for engine to finish running
        await self.engine
        # close beacon socket
        self.beacon = None

        try:
            self._outbox.unbind(f"inproc://events-{self._identity}")
        except zmq.error.ZMQError as e:
            pass
        finally:
            self._outbox.close()

        # additional cleanup
        self.peers = None
        self.peer_groups = None
        self.own_groups = None

    def bind(self, endpoint):
        self.logger.warning("Not implemented")
    
    async def join(self, groupname):
        grp = self.own_groups.get(groupname)
        if not grp:
            # Only send if we're not already in group
            grp = PyreGroup(self._name, groupname)
            self.own_groups[groupname] = grp
            msg = ZreMsg(ZreMsg.JOIN)
            msg.set_group(groupname)
            self.status += 1
            msg.set_status(self.status)

            for peer in self.peers.values():
                await peer.send(msg)

            self.logger.debug("Node is joining group {0}".format(groupname))
    
    async def leave(self, groupname):
        grp = self.own_groups.get(groupname)
        if grp:
            # Only send if we're actually in group
            msg = ZreMsg(ZreMsg.LEAVE)
            msg.set_group(groupname)
            self.status += 1
            msg.set_status(self.status)

            for peer in self.peers.values():
                await peer.send(msg)

            self.own_groups.pop(groupname)

            self.logger.debug("Node is leaving group {0}".format(groupname))

    async def shout(self, groupname, content):
        # Get group to send message to
        msg = ZreMsg(ZreMsg.SHOUT)
        msg.set_group(groupname)
        msg.content = content  # request may contain multipart message

        if self.peer_groups.get(groupname):
            await self.peer_groups[groupname].send(msg)
        else:
            self.logger.warning("Group {0} not found.".format(groupname))
    
    async def whisper(self, peer_id, content):
        # Send frame on out to peer's mailbox, drop message
        # if peer doesn't exist (may have been destroyed)
        if self.peers.get(peer_id):
            msg = ZreMsg(ZreMsg.WHISPER)
            msg.set_address(peer_id)
            msg.content = content
            await self.peers[peer_id].send(msg)
        else:
            self.logger.warning(f"Unknown peer [{peer_id}]")
    
    def get_peers(self):
        return self.peers.keys()

    async def purge_peer(self, peer, endpoint):
        if (peer.get_endpoint() == endpoint):
            await self.remove_peer(peer)
            peer.disconnect()
            self.logger.debug("Purge peer: {0}{1}".format(peer,endpoint))

    # Find or create peer via its UUID string
    async def require_peer(self, peer_identity, endpoint):
        peer = self.peers.get(peer_identity)
        if not peer:
            # Purge any previous peer on same endpoint
            for _, _peer in self.peers.copy().items():
                await self.purge_peer(_peer, endpoint)

            peer = PyrePeer(self._ctx, self._name, peer_identity)
            self.peers[peer_identity] = peer
            peer.set_origin(self._name)
            peer.connect(self._identity, endpoint)

            # Handshake discovery by sending HELLO as first message
            message = ZreMsg(ZreMsg.HELLO)
            message.set_endpoint(self.endpoint)
            message.set_groups(self.own_groups.keys())
            message.set_status(self.status)
            message.set_name(self._name)
            message.set_headers(self.headers)
            await peer.send(message)

        return peer

    #  Remove a peer from our data structures
    async def remove_peer(self, peer):
        # Tell the calling application the peer has gone
        await self._outbox.send_multipart([
            "EXIT".encode('utf-8'),
            peer.get_identity().bytes,
            peer.get_name().encode('utf-8')
        ])
        self.logger.debug("({0}) EXIT name={1}".format(peer, peer.get_endpoint()))
        # Remove peer from any groups we've got it in
        for grp in self.peer_groups.values():
            grp.leave(peer)
        # To destroy peer, we remove from peers hash table (dict)
        self.peers.pop(peer.get_identity())

    # Find or create group via its name
    def require_peer_group(self, groupname):
        grp = self.peer_groups.get(groupname)
        if not grp:
            # somehow a dict containing peers is passed if
            # I don't force the peers arg to an empty dict
            grp = PyreGroup(self._name, groupname, peers={})
            self.peer_groups[groupname] = grp

        return grp

    async def join_peer_group(self, peer, groupname):
        grp = self.require_peer_group(groupname)
        grp.join(peer)
        # Now tell the caller about the peer joined group
        await self._outbox.send_multipart([
            "JOIN".encode('utf-8'),
            peer.get_identity().bytes,
            peer.get_name().encode('utf-8'),
            groupname.encode('utf-8')
        ])
        self.logger.debug("({0}) JOIN name={1} group={2}".format(self._name, peer.get_name(), groupname))
        return grp

    async def leave_peer_group(self, peer, groupname):
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
        self.logger.debug("({0}) LEAVE name={1} group={2}".format(self._name, peer.get_name(), groupname))

    # Here we handle messages coming from other peers
    async def run_router_receiver(self):
        while not self._terminated:
            try:
                frames = await asyncio.wait_for(self.inbox.recv_multipart(), timeout=0.5)
                if frames is None:
                    continue
                # frames = await input_socket.recv_multipart()
            except asyncio.TimeoutError:
                continue
                        
            zmsg = ZreMsg()
            zmsg.parse(frames)
            # Router socket tells us the identity of this peer
            # First frame is sender identity
            id = zmsg.id
            address = zmsg.get_address()
            self.logger.debug(f"Received {zmsg.get_command()} Message from {address}")
            # On HELLO we may create the peer if it's unknown
            # On other commands the peer must already exist
            peer = self.peers.get(address)
            if id == ZreMsg.HELLO:
                if (peer):
                    # remove fake peers
                    if peer.get_ready():
                        await self.remove_peer(peer)
                    elif peer.endpoint == self.endpoint:
                        # We ignore HELLO, if peer has same endpoint as current node
                        continue

                peer = await self.require_peer(address, zmsg.get_endpoint())
                peer.set_ready(True)
            
            # Ignore command if peer isn't ready
            if not peer or not peer.get_ready():
                self.logger.warning("Peer {0} isn't ready".format(peer))
                continue

            if peer.messages_lost(zmsg):
                self.logger.warning("{0} messages lost from {1}".format(self._identity, peer.identity))
                await self.remove_peer(peer)
                continue
            
            # Now process each command
            if id == ZreMsg.HELLO:
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
                self.logger.debug("({0}) ENTER name={1} endpoint={2}".format(self._name, peer.get_name(), peer.get_endpoint()))

                # Join peer to listed groups
                for grp in zmsg.get_groups():
                    await self.join_peer_group(peer, grp)
                # Now take peer's status from HELLO, after joining groups
                peer.set_status(zmsg.get_status())
            elif id == ZreMsg.WHISPER:
                # Pass up to caller API as WHISPER event
                await self._outbox.send_multipart([
                    "WHISPER".encode('utf-8'),
                    peer.get_identity().bytes,
                    peer.get_name().encode('utf-8')
                ] + zmsg.content)
            elif id == ZreMsg.SHOUT:
                # Pass up to caller API as WHISPER event
                await self._outbox.send_multipart([
                    "SHOUT".encode('utf-8'),
                    peer.get_identity().bytes,
                    peer.get_name().encode('utf-8'),
                    zmsg.get_group().encode('utf-8')
                ] + zmsg.content)
            elif id == ZreMsg.PING:
                peer.send(ZreMsg(id=ZreMsg.PING_OK))
            elif id == ZreMsg.JOIN:
                await self.join_peer_group(peer, zmsg.get_group())
                assert(zmsg.get_status() == peer.get_status())
            elif id == ZreMsg.LEAVE:
                await self.leave_peer_group(peer, zmsg.get_group())
                assert(zmsg.get_status() == peer.get_status())
            # Activity from peer resets peer timers
            peer.refresh()

    async def recv_beacon(self, frame, addr):
        #  If filter is set, check that beacon matches it
        is_valid = False
        if self.filter is not None:
            if len(self.filter) <= len(frame):
                match_data = frame[:len(self.filter)]
                if (match_data == self.filter):
                    is_valid = True
        
        self.logger.debug(f"Received beacon [{frame}] from [{addr}]")

        #  If valid, discard our own broadcasts, which UDP echoes to us
        if is_valid and self.transmit:
            if frame == self.transmit:                
                is_valid = False

        #  If still a valid beacon, send on to the API
        if is_valid:
            beacon = struct.unpack('cccb16sH', frame)
            # Ignore anything that isn't a valid beacon
            if beacon[3] != BEACON_VERSION:
                self.logger.warning("Invalid ZRE Beacon version: {0}".format(beacon[3]))
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
                peer = self.peers.get(peer_id)
                # remove the peer (delete)
                if peer:
                    self.logger.debug("Received 0 port beacon, removing peer {0}".format(peer_id))
                    await self.remove_peer(peer)

                else:
                    self.logger.warning(self.peers)
                    self.logger.warning("We don't know peer id {0}".format(peer_id))

    # TODO: Handle gossip dat

    # We do this once a second:
    # - if peer has gone quiet, send TCP ping
    # - if peer has disappeared, expire it
    async def ping_peer(self, peer_id):
        peer = self.peers.get(peer_id)
        if time.time() > peer.expired_at:
            self.logger.debug("({0}) peer expired name={1} endpoint={2}".format(self._name, peer.get_name(), peer.get_endpoint()))
            await self.remove_peer(peer)
        elif time.time() > peer.evasive_at:
            # If peer is being evasive, force a TCP ping.
            # TODO: do this only once for a peer in this state;
            # it would be nicer to use a proper state machine
            # for peer management.
            self.logger.debug("({0}) peer seems dead/slow name={1} endpoint={2}".format(self._name, peer.get_name(), peer.get_endpoint()))
            msg = ZreMsg(ZreMsg.PING)
            await peer.send(msg)

    # --------------------------------------------------------------------------
    # This is the actor that runs a single node; it uses one thread, creates
    # a zyre_node object at start and destroys that when finishing.
    async def run_reaper(self):
        while not self._terminated:
            # keep looping
            # Ping all peers and reap any expired ones
            for peer_id in self.peers.copy().keys():
                await self.ping_peer(peer_id)
            # sleep interval
            await asyncio.sleep(REAP_INTERVAL)
            
    async def run_beacon(self):
        while not self._terminated:
            # keep looping
            # send the beacon at interval
            await self.beacon.send_beacon(self.transport, self.transmit)
            # sleep interval
            await asyncio.sleep(1.0)
    
    '''
    run in a background task
    ie. a task not being checked until the very end
    '''
    async def run(self):
        try:
            tasks = [
                self.run_beacon(),          # periodically send beacon
                self.run_reaper(),          # periodically poke peers
                self.run_router_receiver()  # receive incoming messages from peers
            ]
            
            # done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            # if pending:
            #     await self.stop()
            #     await asyncio.gather(*pending)
                
            #     self.engine_running = False
            #     raise Exception("Error")
            
            await asyncio.gather(*tasks)
        finally:
            self.engine_running = False
