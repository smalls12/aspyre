import logging

from .zre_msg import ZreMsg
from .pyre_peer import AspyrePeer

class PeerDatabase():
    def __init__(self, socket, outbox, own_groups, peer_groups, **kwargs):
        self._name = kwargs["config"]["general"]["name"]
        self._identity = kwargs["config"]["general"]["identity"]
        self._logger = logging.getLogger("aspyre").getChild(self._name)

        self._ctx = kwargs["config"]["general"]["ctx"]

        self._socket = socket

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
    
    # Find or create peer via its UUID string
    async def require_peer(self, peer_identity, endpoint):
        """
        string
        """
        _peer = self._peers.get(peer_identity)
        if not _peer:
            # Purge any previous peer on same endpoint
            for _, __peer in self._peers.copy().items():
                await self._purge_peer(__peer, endpoint)

            _peer = AspyrePeer(self._ctx, self._name, peer_identity)
            self._peers[peer_identity] = _peer
            _peer.set_origin(self._name)
            _peer.connect(self._identity, endpoint)

            # Handshake discovery by sending HELLO as first message
            _zmsg = ZreMsg(ZreMsg.HELLO)
            _zmsg.set_endpoint(self._socket.endpoint)
            _zmsg.set_groups(self._own_groups.groups.keys())
            _zmsg.set_status(self._status)
            _zmsg.set_name(self._name)
            _zmsg.set_headers(self._headers)
            await _peer.send(_zmsg)

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
