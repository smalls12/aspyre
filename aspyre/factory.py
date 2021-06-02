import zmq

# ---------------------------------------------------
# 
class SocketFactoryImpl():
    """
    """
    def __init__(self, context):
        """
        """
        self._context = context

    def get_socket(self, type):
        """
        """
        return self._context.socket(type)

# ---------------------------------------------------
#
class SocketFactory(SocketFactoryImpl):
    """
    """
    def __init__(self, context):
        """
        """
        super().__init__(context)

class ServerSocketFactory(SocketFactory):
    """
    """
    def __init__(self, context):
        """
        """
        super().__init__(context)

    def get_socket(self):
        """
        """
        return super().get_socket(zmq.ROUTER)

class ClientSocketFactory(SocketFactory):
    """
    """
    def __init__(self, context):
        """
        """
        super().__init__(context)

    def get_socket(self):
        """
        """
        return super().get_socket(zmq.DEALER)

# ---------------------------------------------------
#
class EncryptedSocketFactory(SocketFactoryImpl):
    """
    """
    def __init__(self, context, authenticator):
        """
        """
        super().__init__(context)
        self._authenticator = authenticator

class EncryptedServerSocketFactory(EncryptedSocketFactory):
    """
    """
    def __init__(self, context, authenticator):
        """
        """
        super().__init__(context, authenticator)

    def get_socket(self):
        """
        """
        _socket = super().get_socket(zmq.ROUTER)
        self._authenticator.authenticate(_socket)
        return _socket

class EncryptedClientSocketFactory(EncryptedSocketFactory):
    """
    """
    def __init__(self, context, authenticator):
        """
        """
        super().__init__(context, authenticator)

    def get_socket(self, server_public_key):
        """
        """
        _socket = super().get_socket(zmq.DEALER)
        self._authenticator.authenticate(_socket, server_public_key)
        return _socket
