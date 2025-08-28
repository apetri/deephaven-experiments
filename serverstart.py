#!/usr/bin/env ipython -i

import importlib
import pandas as pd
import numpy as np

from deephaven_server.server import Server

# Start a server with 8GB RAM on port 10000 and the default PSK authentication
s = Server(port=10000, jvm_args=["-Xmx8g","-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler","-Dprocess.info.system-info.enabled=false"])
s.start()

from deephaven import new_table,empty_table,agg,ui
from deephaven.table import Table
from deephaven.column import int_col,float_col,string_col

import deephaven.numpy as dhnp
import deephaven.pandas as dhpd
import deephaven.updateby as dhuby

import deephaven.plot.express as dx

import data.dbclient

from globalscope import *