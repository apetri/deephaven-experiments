#!/usr/bin/env ipython -i

from deephaven_server.server import Server

# Start a server with 4GB RAM on port 10000 and the default PSK authentication
s = Server(port=10000, jvm_args=["-Xmx4g","-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler","-Dprocess.info.system-info.enabled=false"])
s.start()