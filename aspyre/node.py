"""
string
"""

import logging
import asyncio

import zmq.asyncio

from .message import ZreMsg
from .group import PyreGroup

# hmm ?
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

BEACON_VERSION = 1
ZRE_DISCOVERY_PORT = 5670
REAP_INTERVAL = 1.0  # Once per second

class AspyreAsyncNode():
    """
    string
    """
    def __init__(self, beacon, router, reaper, mailbox, own_groups, peers, peer_groups, **kwargs):
        """
        string
        """
        self._ctx = kwargs["config"]["general"]["ctx"]

        self._terminated = False

        self._beacon_interface_name = kwargs["config"]["beacon"]["interface_name"]
        self._beacon = beacon
        self._beacon_receiver = None

        self._transmit = None
        self._filter = b""

        self._identity = kwargs["config"]["general"]["identity"]
        self._name = kwargs["config"]["general"]["name"]
        self._logger = logging.getLogger("aspyre").getChild(self._name)

        self._router = router
        self._endpoint = ""
        self._port = 0

        self._reaper = reaper

        self._outbox = mailbox

        self._status = 0

        self._own_groups = own_groups
        self._peers = peers
        self._peer_groups = peer_groups

        self._headers = {}

    async def run(self, interface):
        """
        start three concurrent tasks for each of
        the three discrete components of aspyre
        1. the beacon
        2. the reaper
        3. the router        
        """
        try:
            tasks = [
                self._beacon.run(interface),         # periodically send beacon
                self._reaper.run(),         # periodically poke peers
                self._router.run()          # receive incoming messages from peers
            ]

            # this method will block the current task until
            # the three tasks above complete
            await asyncio.gather(*tasks)
        finally:
            pass

    async def stop(self):
        """
        string
        """
        self._logger.debug("Stopping")
        # this will stop the beacon, reaper and router receiver
        self._terminated = True

        # stopping each of these components should release ::run above
        self._beacon.stop()
        self._reaper.stop()
        self._router.stop()

    async def join(self, groupname):
        """
        this informs all peers that this node is joining the
        specified group
        """
        if not self._own_groups.groups.get(groupname):
            # Only send if we're not already in group
            _group = PyreGroup(self._name, groupname)
            self._own_groups.groups[groupname] = _group

            _zmsg = ZreMsg(ZreMsg.JOIN)
            _zmsg.set_group(groupname)
            self._status += 1
            _zmsg.set_status(self._status)

            for peer in self._peers.peers.values():
                await peer.send(_zmsg)

            self._logger.debug("Node is joining group {0}".format(groupname))

    async def leave(self, groupname):
        """
        this informs all peers that this node is leaving the
        specified group
        """
        if self._own_groups.groups.get(groupname):
            # Only send if we're actually in group
            _zmsg = ZreMsg(ZreMsg.LEAVE)
            _zmsg.set_group(groupname)
            self._status += 1
            _zmsg.set_status(self._status)

            for peer in self._peers.peers.values():
                await peer.send(_zmsg)
            
            self._own_groups.groups.pop(groupname)

            self._logger.debug("Node is leaving group {0}".format(groupname))

    async def shout(self, groupname, content):
        """
        this sends a message to the group
        """
        # only send if other peers have indicated joinging that group
        # otherwise there would be no one to send to
        _group = self._peer_groups.groups.get(groupname)
        if _group:
            _zmsg = ZreMsg(ZreMsg.SHOUT)
            _zmsg.set_group(groupname)
            _zmsg.content = content

            await _group.send(_zmsg)
        else:
            self._logger.warning("Group {0} not found.".format(groupname))

    async def whisper(self, peer_id, content):
        """
        this sends a message to a specific peer
        """
        _peer = self._peers.peers.get(peer_id)
        if _peer:
            _zmsg = ZreMsg(ZreMsg.WHISPER)
            _zmsg.set_address(peer_id)
            _zmsg.content = content

            await _peer.send(_zmsg)
        else:
            self._logger.warning(f"Unknown peer [{peer_id}]")

    def get_peers(self):
        """
        string
        """
        return self._peers.peers.keys()    
