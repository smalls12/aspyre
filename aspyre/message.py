"""
These are the zre_msg messages
    HELLO - Greet a peer so it can connect back to us
        sequence      number 2  Cyclic sequence number
        endpoint      string
        groups        strings
        status        number 1
        name          string
        headers       dictionary
    WHISPER - Send a message to a peer
        sequence      number 2
        content       frame
    SHOUT - Send a message to a group
        sequence      number 2
        group         string
        content       frame
    JOIN - Join a group
        sequence      number 2
        group         string
        status        number 1
    LEAVE - Leave a group
        sequence      number 2
        group         string
        status        number 1
    PING - Ping a peer that has gone silent
        sequence      number 2
    PING_OK - Reply to a peer's ping
        sequence      number 2
"""

import struct
import uuid
import logging

STRING_MAX = 255

class ZreMsg():
    """
    string
    """

    VERSION = 2
    HELLO = 1
    WHISPER = 2
    SHOUT = 3
    JOIN = 4
    LEAVE = 5
    PING = 6
    PING_OK = 7

    def __init__(self, id=None, *args, **kwargs):
        """
        string
        """
        self.logger = logging.getLogger("aspyre")
        self.address = ""
        self.id = id
        self.sequence = 0
        self.endpoint = ""
        self.groups = ()
        self.group = None
        self.status = 0
        self.name = ""
        self.headers = {}
        self.content = b""
        self.struct_data = kwargs.get("data", b'')
        self._needle = 0
        self._ceil = len(self.struct_data)

    def parse(self, frames):
        """
        string
        """
        self.address = frames.pop(0)
        # we drop the first byte: TODO ref!
        try:
            self.address = uuid.UUID(bytes=self.address[1:])
        except ValueError:
            self.logger.debug("Peer identity frame empty or malformed")
            return None

        # Read and parse command in frame
        self.struct_data = frames.pop(0)
        if not self.struct_data:
            return None

        # Get and check protocol signature
        if self._needle != 0:
            self.logger.debug("Message already decoded for protocol signature")

        self._ceil = len(self.struct_data)

        signature = self._get_number2()
        if signature != (0xAAA0 | 1):
            self.logger.debug("Invalid signature {}".format(signature))
            return None

        # Get message id and parse per message type
        self.id = self._get_number1()

        version = self._get_number1()
        if version != 2:
            self.logger.debug("Invalid version {}".format(version))
            return None

        if self.id == ZreMsg.HELLO:
            self.unpack_hello()

        elif self.id == ZreMsg.WHISPER:
            self.sequence = self._get_number2()
            if len(frames):
                self.content = frames

        elif self.id == ZreMsg.SHOUT:
            self.sequence = self._get_number2()
            self.group = self._get_string()
            if len(frames):
                self.content = frames

        elif self.id == ZreMsg.JOIN:
            self.sequence = self._get_number2()
            self.group = self._get_string()
            self.status = self._get_number1()

        elif self.id == ZreMsg.LEAVE:
            self.sequence = self._get_number2()
            self.group = self._get_string()
            self.status = self._get_number1()

        elif self.id == ZreMsg.PING:
            self.sequence = self._get_number2()

        elif self.id == ZreMsg.PING_OK:
            self.sequence = self._get_number2()

        else:
            self.logger.debug("Message type {} unknown".format(self.id))

    # Send the zre_msg to the output, and destroy it
    def build(self):
        """
        string
        """
        # clear data
        self.struct_data = b''
        self._needle = 0

        # add signature
        self._put_number2(0xAAA0 | 1)

        # add id
        self._put_number1(self.id)
        #print(self.struct_data)
        # add version
        self._put_number1(2)

        if self.id == ZreMsg.HELLO:
            self.pack_hello()

        elif self.id == ZreMsg.WHISPER:
            self._put_number2(self.sequence)
            # add content in a new frame

        elif self.id == ZreMsg.SHOUT:
            self._put_number2(self.sequence)
            self._put_string(self.group)
            # add content in a new frame

        elif self.id == ZreMsg.JOIN:
            self._put_number2(self.sequence)
            self._put_string(self.group)
            self._put_number1(self.status)

        elif self.id == ZreMsg.LEAVE:
            self._put_number2(self.sequence)
            self._put_string(self.group)
            self._put_number1(self.status)

        elif self.id == ZreMsg.PING:
            self._put_number2(self.sequence)

        elif self.id == ZreMsg.PING_OK:
            self._put_number2(self.sequence)

        else:
            self.logger.debug("Message type {0} unknown".format(self.id))

        messages = []

        # Now send the data frame
        messages.append(self.struct_data)
        if self.content:
            if isinstance(self.content, list):
                messages += self.content
            else:
                messages.append(self.content)

        return messages

    # Get/set the message address
    def get_address(self):
        """
        string
        """
        return self.address

    def set_address(self, address):
        """
        string
        """
        self.address = address

    # Get the zre_msg id and printable command
    def get_id(self):
        """
        string
        """
        return self.id

    def set_id(self, id):
        """
        string
        """
        self.logger.warning("E: set_id NOT IMPLEMENTED")

    def get_command(self):
        """
        string
        """
        if self.id == ZreMsg.HELLO:
            return "HELLO"
        if self.id == ZreMsg.WHISPER:
            return "WHISPER"
        if self.id == ZreMsg.SHOUT:
            return "SHOUT"
        if self.id == ZreMsg.JOIN:
            return "JOIN"
        if self.id == ZreMsg.LEAVE:
            return "LEAVE"
        if self.id == ZreMsg.PING:
            return "PING"
        if self.id == ZreMsg.PING_OK:
            return "PING_OK"

    def get_name(self):
        """
        string
        """
        return self.name

    def set_name(self, name):
        """
        string
        """
        self.name = name

    # Get/set the sequence field
    def get_sequence(self):
        """
        string
        """
        return self.sequence

    def set_sequence(self, sequence):
        """
        string
        """
        self.sequence = sequence

    # Get/set the endpoint field
    def get_endpoint(self):
        """
        string
        """
        return self.endpoint

    def set_endpoint(self, endpoint):
        """
        string
        """
        self.endpoint = endpoint

    # Get/set the groups field
    def get_groups(self):
        """
        string
        """
        return self.groups

    def set_groups(self, groups):
        """
        string
        """
        self.groups = groups

    # Iterate through the groups field, and append a groups value
    # TODO: do we need this in python? l186 zre_msg.h

    #  Get/set the status field
    def get_status(self):
        """
        string
        """
        return self.status

    def set_status(self, status):
        """
        string
        """
        self.status = status

    # Get/set the headers field
    def get_headers(self):
        """
        string
        """
        return self.headers

    def set_headers(self, headers):
        """
        string
        """
        self.headers = headers

    # Get/set a value in the headers dictionary
    # TODO: l208 zre_msg.h

    # Get/set the group field
    def get_group(self):
        """
        string
        """
        return self.group

    def set_group(self, group):
        """
        string
        """
        self.group = group

    def _get_string(self):
        """
        string
        """
        _string_len = self._get_number1()
        _string = struct.unpack_from(str(_string_len) + 's', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('s' * _string_len)
        return _string[0].decode('UTF-8')

    def _get_number1(self):
        """
        string
        """
        num = struct.unpack_from('>B', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('>B')
        return num[0]

    def _get_number2(self):
        """
        string
        """
        num = struct.unpack_from('>H', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('>H')
        return num[0]

    def _get_number4(self):
        """
        string
        """
        num = struct.unpack_from('>I', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('>I')
        return num[0]

    def _get_number8(self):
        """
        string
        """
        num = struct.unpack_from('>Q', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('>Q')
        return num[0]

    def _get_long_string(self):
        """
        string
        """
        _string_len = self._get_number4()
        _string = struct.unpack_from(str(_string_len) + 's', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('s' * _string_len)
        return _string[0].decode('UTF-8')

    def _put_string(self, string):
        """
        string
        """
        self._put_number1(len(string))
        _data = struct.pack('%is' % len(string), string.encode('UTF-8'))
        self.struct_data += _data

    def _put_number1(self, number):
        """
        string
        """
        _data = struct.pack('>B', number)
        self.struct_data += _data

    def _put_number2(self, number):
        """
        string
        """
        _data = struct.pack('>H', number)
        self.struct_data += _data

    def _put_number4(self, number):
        """
        string
        """
        _data = struct.pack('>I', number)
        self.struct_data += _data

    def _put_number8(self, number):
        """
        string
        """
        _data = struct.pack('>Q', number)
        self.struct_data += _data

    def _put_long_string(self, string):
        """
        string
        """
        self._put_number4(len(string))
        _data = struct.pack('%is' % len(string), string.encode('UTF-8'))
        self.struct_data += _data

    def unpack_hello(self):
        """unpack a zre hello packet

        sequence      number 2
        endpoint      string
        groups        strings
        status        number 1
        name          string
        headers       dictionary
        """
        #self._needle = 0
        self.sequence = self._get_number2()
        #print(self.sequence)
        #print("needle is at: %i"% self._needle )
        self.endpoint = self._get_string()
        #print(self.ipaddress)
        #print("needle is at: %i"% self._needle )
        group_len = self._get_number4()
        #print("needle is at: %i"% self._needle )
        #print("grouplen: ", group_len)
        self.groups = []
        for x in range(group_len):
            self.groups.append(self._get_long_string())
        #print(self.groups)
        #print("post_group: needle is at: %i"% self._needle )
        self.status = self._get_number1()
        self.name = self._get_string()
        headers_len = self._get_number4()
        self.headers = {}
        for x in range(headers_len):
            key = self._get_string()
            val = self._get_long_string()
            self.headers.update({key: val})
            #import ast
            #for hdr in hdrlist:
            #    # TODO: safer to use ast.literal_eval
            #    headers.update(ast.literal_eval(hdr))
        #print(self.headers)

    def pack_hello(self):
        """Pack a zre hello packet

        sequence      number 2
        endpoint      string
        groups        strings
        status        number 1
        name          string
        headers       dictionary
        """
        # clear data
        #self.struct_data = b''
        #print(len(self.struct_data))
        #self._put_number2(0xAAA0)
        #self._put_number1(self.id)
        self._put_number2(self.sequence)
        self._put_string(self.endpoint)
        self._put_number4(len(self.groups))
        for g in self.groups:
            self._put_long_string(g)
        self._put_number1(self.status)
        self._put_string(self.name)
        self._put_number4(len(self.headers))
        for key, val in self.headers.items():
            self._put_string(key)
            self._put_long_string(val)

if __name__ == '__main__':
    logger = logging.getLogger(__name__)

    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    # self._put_long_string("%s=%s" % (key, val))  # undefined: self, key, val

    testdata = struct.pack('>Hb9sII2sI2sI2sbb4sIb1sI1sb1sI1s',
                           11,         # sequence
                           9,          # str length
                           b"192:20123",   # endpoint
                           20123,      # mailbox
                           3,          # groups len
                           2, b"g1",   # length + groupname
                           2, b"g2",   # length + groupname
                           2, b"g3",   # length + groupname
                           4,          # status
                           4, b"NAME", # name
                           2,          # header len
                           1, b"a",    # length + dict
                           1, b"z",    # length + dict
                           1, b"b",    # length + dict
                           1, b"b"     # length + dict
    )

    logger.debug("New ZRE HELLO message")
    m = ZreMsg(ZreMsg.HELLO, data=testdata)

    logger.debug("Unpack a HELLO message")
    m.unpack_hello()

    logger.debug("Pack a HELLO message")
    m.pack_hello()

    logger.debug("Unpack the packed HELLO message")
    m.unpack_hello()
