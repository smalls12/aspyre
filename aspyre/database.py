"""
string
"""

import logging

from .message import ZreMsg
from .peer import AspyrePeer, AspyrePeerEncrypted
from .group import PyreGroup

class PeerDatabaseImpl():
    """
    string
    """
    def __init__(self, factory, endpoint, outbox, own_groups, peer_groups, **kwargs):
        """
        string
        """
        self._name = kwargs["config"]["general"]["name"]
        self._identity = kwargs["config"]["general"]["identity"]
        self._logger = logging.getLogger("aspyre").getChild(self._name)

        self._ctx = kwargs["config"]["general"]["ctx"]
        self._factory = factory
        self._endpoint = endpoint
        self._outbox = outbox
        self._own_groups = own_groups
        self._peer_groups = peer_groups
        self._status = 0
        self._headers = {}
        self._peers = {}

    @property
    def peers(self):
        """
        string
        """
        return self._peers

    async def _purge_peer(self, peer, endpoint):
        """
        string
        """
        if peer.get_endpoint() == endpoint:
            await self.remove_peer(peer)
            peer.disconnect()
            self._logger.debug("Purge peer: {0}{1}".format(peer, endpoint))

    async def _send_hello_to_new_peer(self, peer):
        """
        string
        """
        # Handshake discovery by sending HELLO as first message
        _zmsg = ZreMsg(ZreMsg.HELLO)
        _zmsg.set_endpoint(self._endpoint)
        _zmsg.set_groups(self._own_groups.groups.keys())
        _zmsg.set_status(self._status)
        _zmsg.set_name(self._name)
        _zmsg.set_headers(self._headers)
        await peer.send(_zmsg)

    # Find or create peer via its UUID string
    async def initialize_peer(self, peer_identity, endpoint):
        """
        build the peer, if not already built
        """
        _peer = self._peers.get(peer_identity)
        if not _peer:
            # Purge any previous peer on same endpoint
            for _, __peer in self._peers.copy().items():
                await self._purge_peer(__peer, endpoint)

            _peer = AspyrePeer(self._factory, self._outbox, self._name, peer_identity)
            self._peers[peer_identity] = _peer

        return _peer

    # Find or create peer via its UUID string
    async def require_peer(self, peer_identity, endpoint):
        """
        build the peer, if not already built
        connect, it not already connected
        """
        _peer = await self.initialize_peer(peer_identity, endpoint)
        if not _peer.connected:
            _peer.set_origin(self._name)
            _peer.connect(self._identity, endpoint)

            await self._send_hello_to_new_peer(_peer)

        return _peer

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
        for _group in self._peer_groups.groups.values():
            _group.leave(peer)

        # To destroy peer, we remove from peers hash table (dict)
        self._peers.pop(peer.get_identity())

class PeerDatabase(PeerDatabaseImpl):
    """
    """
    def __init__(self, factory, endpoint, outbox, own_groups, peer_groups, **kwargs):
        """
        string
        """
        super().__init__(factory, endpoint, outbox, own_groups, peer_groups, **kwargs)

class PeerDatabaseEncrypted(PeerDatabaseImpl):
    """
    """
    def __init__(self, factory, endpoint, outbox, own_groups, peer_groups, **kwargs):
        """
        string
        """
        super().__init__(factory, endpoint, outbox, own_groups, peer_groups, **kwargs)

    # Find or create peer via its UUID string
    async def initialize_peer(self, peer_identity, endpoint):
        """
        build the peer, if not already built
        """
        _peer = self._peers.get(peer_identity)
        if not _peer:
            # Purge any previous peer on same endpoint
            for _, __peer in self._peers.copy().items():
                await self._purge_peer(__peer, endpoint)

            _peer = AspyrePeerEncrypted(self._factory, self._outbox, self._name, peer_identity)
            self._peers[peer_identity] = _peer

        return _peer

    # Find or create peer via its UUID string
    async def require_peer(self, peer_identity, endpoint, key):
        """
        build the peer, if not already built
        connect, it not already connected
        """
        _peer = await self.initialize_peer(peer_identity, endpoint)
        if not _peer.connected:
            _peer.set_origin(self._name)
            _peer.connect(self._identity, endpoint, key)

            await self._send_hello_to_new_peer(_peer)

        return _peer

class GroupDatabase():
    """
    string
    """
    def __init__(self, **kwargs):
        """
        string
        """
        self._name = kwargs["config"]["general"]["name"]
        self.logger = logging.getLogger("aspyre").getChild(self._name)

        self._groups = {}

    @property
    def groups(self):
        """
        string
        """
        return self._groups

    # Find or create group via its name
    def require_group(self, groupname):
        """
        string
        """
        grp = self._groups.get(groupname)
        if not grp:
            # somehow a dict containing peers is passed if
            # I don't force the peers arg to an empty dict
            grp = PyreGroup(self._name, groupname, peers={})
            self._groups[groupname] = grp

        return grp
