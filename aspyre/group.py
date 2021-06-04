"""
string
"""
import logging
import asyncio

class PyreGroup():
    """
    string
    """
    def __init__(self, name, groupname, peers={}):
        self.name = name
        self.logger = logging.getLogger("aspyre").getChild(self.name)
        self.groupname = groupname
        self.peers = peers

    def __del__(self):
        pass

    def __repr__(self):
        ret = "GROUPNAME={0}:\n".format(self.groupname)
        for key, val in self.peers.items():
            ret += "\t{0} {1}\n".format(key, val.name)
        return ret

    # Add peer to group
    def join(self, peer):
        """
        string
        """
        self.peers[peer.get_identity()] = peer
        peer.set_status(peer.get_status() + 1)

    # Remove peer from group
    def leave(self, peer):
        """
        string
        """
        peer_identity = peer.get_identity()
        if peer_identity in self.peers:
            self.peers.pop(peer.get_identity())

        else:
            self.logger.debug("Peer {0} is not in group {1}.".format(peer, self.groupname))

        peer.set_status(peer.get_status() + 1)

    # Send message to all peers in group
    async def send(self, msg):
        """
        string
        """
        for _peer in self.peers.values():
            await _peer.send(msg)
