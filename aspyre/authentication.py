import zmq
from zmq.auth.asyncio import AsyncioAuthenticator

class AspyreAuthenticationContext():
    """
    the authntication context which authenticates the entire
    zeromq context
    """
    def __init__(self, context, interface, public_keys_dir):
        # Start an authenticator for this context.
        self._auth = AsyncioAuthenticator(context)
        self._auth.start()
        # self._auth.allow(interface.address)

        # Tell authenticator to use the certificate in a directory
        self._auth.configure_curve(domain='*', location=public_keys_dir)
    
    def __del__(self):
        self._auth.stop()
    
class AspyreAuthentication():
    """
    base class for getting authenticated sockets
    """
    def __init__(self, context):
        self._context = context
    
    @property
    def context(self):
        """
        grab the underlying zeromq context
        """
        return self._context
    
    def authenticate(self, socket):
        pass

class AspyreNoAuthentication(AspyreAuthentication):
    """
    """
    def __init__(self, context):
        super().__init__(context)

class AspyreServerNoAuthentication(AspyreNoAuthentication):
    """
    """
    def __init__(self, context):
        super().__init__(context)

class AspyreClientNoAuthentication(AspyreNoAuthentication):
    """
    """
    def __init__(self, context):
        super().__init__(context)
    
    def authenticate(self, socket, server_public_key):
        pass

class AspyreCurveAuthentication(AspyreAuthentication):
    """
    base class for getting authenticated sockets
    """
    def __init__(self, context):
        super().__init__(context)
    
    def authenticate(self, socket, secret_file):
        """
        """
        _public_key, _secret_key = zmq.auth.load_certificate(secret_file)
        socket.curve_secretkey = _secret_key
        socket.curve_publickey = _public_key

class AspyreServerCurveAuthentication(AspyreCurveAuthentication):
    """
    """
    def __init__(self, context, server_secret_file):
        super().__init__(context)
        self._server_secret_file = server_secret_file
    
    def authenticate(self, socket):
        """
        """
        super().authenticate(socket, self._server_secret_file)
        socket.curve_server = True

class AspyreClientCurveAuthentication(AspyreCurveAuthentication):
    """
    """
    def __init__(self, context, client_secret_file):
        super().__init__(context)
        self._client_secret_file = client_secret_file
    
    def authenticate(self, socket, server_public_key):
        """
        """
        super().authenticate(socket, self._client_secret_file)
        socket.curve_serverkey = server_public_key

class SocketFactory():
    """
    """
    def __init__(self, context, authenticator):
        self._context = context
        self._authenticator = authenticator

    def get_socket(self, type):
        """
        """
        return self._context.socket(type)

class ServerSocketFactory(SocketFactory):
    """
    """
    def __init__(self, context, authenticator):
        super().__init__(context, authenticator)

    def get_socket(self):
        """
        """
        _socket = super().get_socket(zmq.ROUTER)
        self._authenticator.authenticate(_socket)
        return _socket

class ClientSocketFactory(SocketFactory):
    """
    """
    def __init__(self, context, authenticator):
        super().__init__(context, authenticator)

    def get_socket(self, server_public_key):
        """
        """
        _socket = super().get_socket(zmq.DEALER)
        self._authenticator.authenticate(_socket, server_public_key)
        return _socket
