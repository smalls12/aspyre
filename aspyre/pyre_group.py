import logging

logger = logging.getLogger(__name__)

import asyncio

import zmq.asyncio
from zmq.asyncio import Context

class PyreGroup(object):

    def __init__(self, name, peers={}):
        self.name = name
        # TODO perhaps warn if peers is not a set type
        self.peers = peers

    #def __del__(self):

    def __repr__(self):
        ret = "GROUPNAME={0}:\n".format(self.name)
        for key, val in self.peers.items():
            ret += "\t{0} {1}\n".format(key, val.name)
        return ret

    # Add peer to group
    def join(self, peer):
        self.peers[peer.get_identity()] = peer
        peer.set_status(peer.get_status() + 1)

    # Remove peer from group
    def leave(self, peer):
        peer_identity = peer.get_identity()
        if peer_identity in self.peers:
            self.peers.pop(peer.get_identity())

        else:
            logger.debug("Peer {0} is not in group {1}.".format(peer, self.name))

        peer.set_status(peer.get_status() + 1)

    # Send message to all peers in group
    async def send(self, msg):
        for p in self.peers.values():
            await p.send(msg)