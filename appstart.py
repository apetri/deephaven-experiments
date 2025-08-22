import sys,argparse
from deephaven_server.server import Server

# Start a server with 4GB RAM on port 10000 and the default PSK authentication
JVM_ARGS = [
    "-Xmx4g",
    "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
    "-Dprocess.info.system-info.enabled=false"
]

parser = argparse.ArgumentParser()
parser.add_argument("-a","--app",dest="app",action="store",type=str,default=None,help="full path to application")
parser.add_argument("-p","--port",dest="port",action="store",type=int,default=10000,help="server port")

def main():

    cmd_args = parser.parse_args(sys.argv[1:])
    if cmd_args.app is None:
        parser.print_help()
        sys.exit(1)

    s = Server(port=cmd_args.port, jvm_args=JVM_ARGS + [f"-Ddeephaven.application.dir={cmd_args.app}"])
    s.start()

if __name__=="__main__":
    main()