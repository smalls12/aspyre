"""
Encompasses all of the concerns of the router socket within the node.

The router is responsible for receiving all messages from peers, excluding
beacons.

These messages include HELLO, SHOUT, LEAVE, etc.
"""

import zmq
import logging
import asyncio
import json

from zmq.auth.asyncio import AsyncioAuthenticator

from .message import ZreMsg

class AspyreNodeAsyncRouter():
    def __init__(self, socket, endpoint, outbox, peers, peer_groups, **kwargs):
        self._name = kwargs["config"]["general"]["name"]
        self._logger = logging.getLogger("aspyre").getChild(self._name)

        self._socket = socket

        self._endpoint = endpoint

        self._outbox = outbox

        self._peers = peers

        self._peer_groups = peer_groups

        self._terminated = False

    async def run(self):
        """continuously receives on the socket"""
        self.start()

        while not self._terminated:
            self._logger.debug("Receiving...")
            try:
                frames = await asyncio.wait_for(self._socket.recv_multipart(), timeout=0.5)
                if frames is not None:
                    await self._handle_message(frames)
            except asyncio.TimeoutError:
                continue
        
        self._logger.debug("Router closing...")

    def start(self):
        """any setup required prior to running"""
        pass

    def stop(self):
        """any cleanup required after stopping"""
        self._terminated = True
    
    async def join_peer_group(self, peer, groupname):
        """
        upon reception of a HELLO or JOIN
        update our internal database which keeps track of
        the groups each peer has joined
        """
        _group = self._peer_groups.require_group(groupname)
        _group.join(peer)

        # Now tell the caller about the peer joined group
        await self._outbox.send_multipart([
            "JOIN".encode('utf-8'),
            peer.get_identity().bytes,
            peer.get_name().encode('utf-8'),
            groupname.encode('utf-8')
        ])

        self._logger.debug(f"({self._name}) JOIN name={peer.get_name()} group={groupname}")

        return _group
    
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
        _group = self._peer_groups.require_group(groupname)
        _group.leave(peer)

        self._logger.debug(f"({self._name}) LEAVE name={peer.get_name()} group={groupname}")

    # Here we handle messages coming from other peers
    async def _handle_message(self, frames):
        """
        called by the ::run function
        performs the precessing on a message
        """
        _zmsg = ZreMsg()
        _zmsg.parse(frames)
        # Router socket tells us the identity of this peer
        # First frame is sender identity
        message_id = _zmsg.id
        address = _zmsg.get_address()
        self._logger.debug(f"Received {_zmsg.get_command()} Message from {address}")
        # On HELLO we may create the peer if it's unknown
        # On other commands the peer must already exist
        peer = self._peers.peers.get(address)
        if message_id == ZreMsg.HELLO:
            if peer:
                # remove fake peers
                if peer.get_ready():
                    await self.remove_peer(peer)
                elif peer.endpoint == self._endpoint:
                    # We ignore HELLO, if peer has same endpoint as current node
                    return

            # so we got a HELLO from a peer we don't know, maybe the beacon was late
            peer = await self._peers.require_peer(address, _zmsg.get_endpoint())
            peer.set_ready(True)

        # Ignore command if peer isn't ready
        if not peer or not peer.get_ready():
            self._logger.warning("Peer {0} isn't ready".format(peer))
            return

        if peer.messages_lost(_zmsg):
            self._logger.warning(f"{self._identity} messages lost from {peer.identity}")
            await self.remove_peer(peer)
            return

        # Now process each command
        if message_id == ZreMsg.HELLO:
            # Store properties from HELLO command into peer
            peer.set_name(_zmsg.get_name())
            peer.set_headers(_zmsg.get_headers())

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
            for grp in _zmsg.get_groups():
                await self.join_peer_group(peer, grp)
            # Now take peer's status from HELLO, after joining groups
            peer.set_status(_zmsg.get_status())
        elif message_id == ZreMsg.WHISPER:
            # Pass up to caller API as WHISPER event
            await self._outbox.send_multipart([
                "WHISPER".encode('utf-8'),
                peer.get_identity().bytes,
                peer.get_name().encode('utf-8')
            ] + _zmsg.content)
        elif message_id == ZreMsg.SHOUT:
            # Pass up to caller API as WHISPER event
            await self._outbox.send_multipart([
                "SHOUT".encode('utf-8'),
                peer.get_identity().bytes,
                peer.get_name().encode('utf-8'),
                _zmsg.get_group().encode('utf-8')
            ] + _zmsg.content)
        elif message_id == ZreMsg.PING:
            await peer.send(ZreMsg(id=ZreMsg.PING_OK))
        elif message_id == ZreMsg.PING_OK:
            peer.receive_ping_ok()
        elif message_id == ZreMsg.JOIN:
            await self.join_peer_group(peer, _zmsg.get_group())
            assert _zmsg.get_status() == peer.get_status()
        elif message_id == ZreMsg.LEAVE:
            await self.leave_peer_group(peer, _zmsg.get_group())
            assert _zmsg.get_status() == peer.get_status()
        # Activity from peer resets peer timers
        peer.refresh()
