"""
string
"""

import zmq

class SocketFactoryImpl():
    """
    string
    """
    def __init__(self, context):
        """
        string
        """
        self._context = context

    def get_socket(self, type):
        """
        string
        """
        return self._context.socket(type)

class SocketFactory(SocketFactoryImpl):
    """
    string
    """
    def __init__(self, context):
        """
        string
        """
        super().__init__(context)

class ServerSocketFactory(SocketFactory):
    """
    string
    """
    def __init__(self, context):
        """
        string
        """
        super().__init__(context)

    def get_socket(self):
        """
        string
        """
        return super().get_socket(zmq.ROUTER)

class ClientSocketFactory(SocketFactory):
    """
    string
    """
    def __init__(self, context):
        """
        string
        """
        super().__init__(context)

    def get_socket(self):
        """
        string
        """
        return super().get_socket(zmq.DEALER)

class EncryptedSocketFactory(SocketFactoryImpl):
    """
    string
    """
    def __init__(self, context, authenticator):
        """
        string
        """
        super().__init__(context)
        self._authenticator = authenticator

class EncryptedServerSocketFactory(EncryptedSocketFactory):
    """
    string
    """
    def __init__(self, context, authenticator):
        """
        string
        """
        super().__init__(context, authenticator)

    def get_socket(self):
        """
        string
        """
        _socket = super().get_socket(zmq.ROUTER)
        self._authenticator.authenticate(_socket)
        return _socket

class EncryptedClientSocketFactory(EncryptedSocketFactory):
    """
    string
    """
    def __init__(self, context, authenticator):
        """
        string
        """
        super().__init__(context, authenticator)

    def get_socket(self, server_public_key):
        """
        string
        """
        _socket = super().get_socket(zmq.DEALER)
        self._authenticator.authenticate(_socket, server_public_key)
        return _socket
