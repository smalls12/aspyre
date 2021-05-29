import logging

from .pyre_group import PyreGroup

class GroupDatabase():
    def __init__(self, **kwargs):
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