"""
encapsulates the different authentication methods of aspyre
"""

import zmq
from zmq.auth.asyncio import AsyncioAuthenticator

# ---------------------------------------------------
# authentication context
class AspyreAuthenticationContext():
    """
    the authentication context which authenticates the entire
    zeromq context

    only required when using encryption
    """
    def __init__(self, context, interface, public_keys_dir):
        """
        sets up the authneticator
        starts the authenticator
        configures curve
        """
        # Start an authenticator for this context.
        self._auth = AsyncioAuthenticator(context)
        self._auth.start()
        # self._auth.allow(interface.address)

        # Tell authenticator to use the certificate in a directory
        self._auth.configure_curve(domain='*', location=public_keys_dir)

    def __del__(self):
        """
        stop the authenticator
        """
        self._auth.stop()
# ---------------------------------------------------

# ---------------------------------------------------
# authentication implementations
class AspyreAuthenticationImpl():
    """
    base class for authenticating sockets
    """
    def __init__(self, context):
        """
        store the zeromq context
        """
        self._context = context

    @property
    def context(self):
        """
        grab the zeromq context
        """
        return self._context

    def authenticate(self, socket):
        """
        authenticate the socket ( default : None )
        """
        pass

# ---------------------------------------------------
# no authentication
class AspyreNoAuthenticationImpl(AspyreAuthenticationImpl):
    """
    no authentication implementation
    """
    def __init__(self, context):
        """
        initialize the base implementation
        """
        super().__init__(context)

class AspyreServerNoAuthentication(AspyreNoAuthenticationImpl):
    """
    server alias for base no authentication implementation
    """
    def __init__(self, context):
        """
        initialize the base no authentication implementation
        """
        super().__init__(context)

class AspyreClientNoAuthentication(AspyreNoAuthenticationImpl):
    """
    client alias for base no authentication implementation
    """
    def __init__(self, context):
        """
        initialize the base no authentication implementation
        """
        super().__init__(context)

    def authenticate(self, socket, server_public_key):
        """
        overload the authentication to accept a public key
        """
        pass

# ---------------------------------------------------
# encryption
class AspyreCurveAuthenticationImpl(AspyreAuthenticationImpl):
    """
    curve encryption authentication implementation
    """
    def __init__(self, context):
        """
        initialize the base implementation
        """
        super().__init__(context)

    def authenticate(self, socket, secret_file):
        """
        overload to authenticate the incoming socket with
        curve encryption
        """
        _public_key, _secret_key = zmq.auth.load_certificate(secret_file)
        socket.curve_secretkey = _secret_key
        socket.curve_publickey = _public_key

class AspyreServerCurveAuthentication(AspyreCurveAuthenticationImpl):
    """
    server alias for base curve encryption authentication implementation
    """
    def __init__(self, context, server_secret_file):
        """
        initialize the base implementation
        store the server public key file
        """
        super().__init__(context)
        self._server_secret_file = server_secret_file

    def authenticate(self, socket):
        """
        overload authenticate to not only call the base curve envryption
        authentication, but also configure the socket to be the curve
        server
        """
        super().authenticate(socket, self._server_secret_file)
        socket.curve_server = True

class AspyreClientCurveAuthentication(AspyreCurveAuthenticationImpl):
    """
    client alias for base curve encryption authentication implementation
    """
    def __init__(self, context, client_secret_file):
        """
        initialize the base implementation
        store the client secret file
        """
        super().__init__(context)
        self._client_secret_file = client_secret_file

    def authenticate(self, socket, server_public_key):
        """
        overload authenticate to not only call the base curve envryption
        authentication, but also configure the socket for the curve
        server key
        """
        super().authenticate(socket, self._client_secret_file)
        socket.curve_serverkey = server_public_key
