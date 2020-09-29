#!/usr/bin/env python
#
#  SPECSERVER -- Service endpoints for the Spectroscopic Data Service

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = 'v1.0.0'

try:
    from ddtrace import patch_all
    patch_all()
except:
    pass

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
import csv
import json
import re
import argparse
import asyncio

from aiohttp import web

# Import the Async implementation
from svr_async import app as svr_async


def create_parser():
    '''Commandline argument parser.
    '''
    parser = argparse.ArgumentParser(
        description=("Launch asynchronous (aiohttp) or "
                     "synchronous (flask) spectro server"))

    parser.add_argument("-s", "--sync", action="store_true")
    parser.add_argument("--host", default="localhost", type=str)
    parser.add_argument("--port", default=6999, type=int)

    return parser


def main():
    parsed = create_parser().parse_args()
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


