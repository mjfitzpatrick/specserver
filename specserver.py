#!/usr/bin/env python
#
#  SPECSERVER -- Service endpoints for the Spectroscopic Data Service

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = 'v1.0.0'


# Uncomment for DataDog stats
'''
try:
    from ddtrace import patch_all
    patch_all()
except:
    pass
'''


#  Usage:
#       specserver [-p <port> | --port=<port>]
#
#  Endpoints:
#
#   QUERY METHODS
#       /query      GET     Submit a query
#          :         :         :
#          :         :         :
#          :         :         :
#
#       /profiles   GET     Get service profiles
#       /services   GET     Get names of supported data service contexts

#
#   SERVICE ADMIN METHODS
#       /           GET     Sevice aliveness test endpoint
#       /ping       GET     Sevice aliveness test endpoint
#       /available  GET     Check service availability
#       /metadata   GET     Get service metadata /metadata?service=<service>
#       /shutdown   GET     Shutdown the service (requires root auth)
#       /debug      GET     Toggle the service debug flag


import os
import sys
import json
import argparse
import asyncio
import socket

from aiohttp import web

# Import the Async implementation
from svr_async import app as svr_async
from svr_sync import app as svr_async

config = {}			# Global configuration data

is_py3 = sys.version_info.major == 3


# Default config file
DEF_CONFIG = '/opt/services/specserver/spec.conf'

# External config file (i.e. not in source tree)
EXT_CONFIG = '/opt/services/lib/spec.conf'


#  PARSECONFIG -- Parse the configuration file.
#
def parseConfig(file):
    '''Parse the configuration file.
    '''
    global config

    print('Opening config file: ' + file)
    if os.path.exists(file):
        config = json.load(open(file))
        if is_py3:
            profiles = list(config['profiles'].keys())    # Py3 version
        else:
            profiles = config['profiles'].keys()          # Py2 version

        def_profile = 'default'
        this_host = socket.gethostname().split('.')[0]    # simple host name
        if this_host in profiles:
            def_profile = this_host
            cfg = config['profiles'][def_profile]
            config['profiles']['default'].update(cfg)
    else:
        raise Exception("No such config file: " + file)


#  CREATE_PARSER -- Commandline argument parser.
#
def create_parser():
    '''Commandline argument parser.
    '''
    parser = argparse.ArgumentParser(
        description=("Launch asynchronous (aiohttp) or "
                     "synchronous (flask) spectro server"))

    parser.add_argument("-s", "--sync", action="store_true")
    parser.add_argument("--config", default="spec.conf", type=str)
    parser.add_argument("--host", default="gp07.datalab.noao.edu", type=str)
    parser.add_argument("--port", default=6998, type=int)

    return parser


#  Application MAIN
#
def main():
    parsed = create_parser().parse_args()

    # Parse the configuration file specified by '--config' or else use
    # the default in the current directory.
    if os.path.exists(parsed.config):
        parseConfig(parsed.config)
    elif os.path.exists(EXT_CONFIG):
        parseConfig(EXT_CONFIG)
    elif os.path.exists(DEF_CONFIG):
        parseConfig(DEF_CONFIG)
    else:
        raise Exception('No config file found.')

    if parsed.sync:
        print("Starting Flask server")
        svr_sync.run(
            host=parsed.host,
            port=parsed.port,
            threaded=True
        )
    else:
        async def start_async_server():
            runner = web.AppRunner(svr_async)
            await runner.setup()
            site = web.TCPSite(
                runner, parsed.host, parsed.port)
            await site.start()
            print(f"Serving up app on {parsed.host}:{parsed.port}")
            return runner, site

        print("Starting async server")
        loop = asyncio.get_event_loop()
        runner, site = loop.run_until_complete(start_async_server())
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            loop.run_until_complete(runner.cleanup())


if __name__ == '__main__':
    main()
