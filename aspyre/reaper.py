import asyncio
import time
import logging

from .message import ZreMsg

class AspyreAsyncReaper():
    def __init__(self, peers, **kwargs):
        self._name = kwargs["config"]["general"]["name"]
        self._logger = logging.getLogger("aspyre").getChild(self._name)

        self._interval = kwargs["config"]["reaper"]["interval"]

        self._terminated = False
        self._peers = peers

    async def run(self):
        """
        string
        """
        self._terminated = False
        while not self._terminated:
            self._logger.debug("reaping...")
            # keep looping
            # Ping all peers and reap any expired ones
            for peer_id in self._peers.peers.copy().keys():
                await self.ping_peer(peer_id)
            # sleep interval
            await asyncio.sleep(self._interval)

    def start(self, interface):
        pass

    def stop(self):
        self._terminated = True

    # We do this once a second:
    # - if peer has gone quiet, send TCP ping
    # - if peer has disappeared, expire it
    async def ping_peer(self, peer_id):
        """
        string
        """
        peer = self._peers.peers.get(peer_id)
        if time.time() > peer.expired_at:
            self._logger.debug(f"({self._name}) peer expired name={peer.get_name()} endpoint={peer.get_endpoint()}")
            await self.remove_peer(peer)
        elif time.time() > peer.evasive_at:
            # If peer is being evasive, force a TCP ping.
            # TODO: do this only once for a peer in this state;
            # it would be nicer to use a proper state machine
            # for peer management.

            # going to send a ping to the client
            # if we still have yet to receive an OK from previous
            # ping(s) then client is being evasive
            if peer.pings_sent != 0:
                await self._outbox.send_multipart([
                    "SILENT".encode('utf-8'),
                    peer.get_identity().bytes,
                    peer.get_name().encode('utf-8')])
            else:
                await self._outbox.send_multipart([
                    "EVASIVE".encode('utf-8'),
                    peer.get_identity().bytes,
                    peer.get_name().encode('utf-8')])

            self._logger.debug(f"({self._name}) peer seems dead/slow name={peer.get_name()} endpoint={peer.get_endpoint()}")
            await peer.send_ping(ZreMsg(ZreMsg.PING))
