#!/usr/bin/env python
#
#  B_FLASK -- Benchmark (sync) services using Flask
#

from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = '20200807'  # yyyymmdd


import os
import sys
import time
import optparse

from numpy.lib import recfunctions as rfn
from tempfile import NamedTemporaryFile

import asyncio
from aiohttp import web

__all__ = ["app"]

routes = web.RouteTableDef()


import io
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import h5py
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
import numpy as np
import json

is_py3 = sys.version_info.major == 3


DEBUG = False                   # Debug flag

# _files
#   100M -rw-r--r-- 1 fitz fitz 100M Aug 13 14:52 junk100M
#    10M -rw-r--r-- 1 fitz fitz  10M Aug 13 14:52 junk10M
#   128K -rw-r--r-- 1 fitz fitz 128K Aug 13 14:53 junk128K
#    12K -rw-r--r-- 1 fitz fitz  12K Aug 13 14:53 junk12K
#   1.0M -rw-r--r-- 1 fitz fitz 1.0M Aug 13 14:52 junk1M
#   125M -rw-r--r-- 1 fitz fitz 125M Aug 13 14:56 spec1000


# Test toplevel endpoint
@routes.get('/')
async def hello():
    return "Hello World from Benchmark Service! %s\n" % __version__



mtype = 'application/octet-stream'

@routes.get('/junk125M')
async def junk125m():
    with open('/home/fitz/specdal/bench/junk125M', 'r') as fd:
        _bytes = fd.read()
    return web.response(body=_bytes)

@routes.get('/junk128K')
async def junk128k():
    with open('/home/fitz/specdal/bench/junk128K', 'r') as fd:
        _bytes = fd.read()
    return web.response(body=_bytes)

@routes.get('/junk12K')
async def junk12k():
    with open('/home/fitz/specdal/bench/junk12K', 'r') as fd:
        _bytes = fd.read()
    return web.response(body=_bytes)




# COADD -- 
#
@routes.get('/coadd')
async def coadd():
    ''' 
    '''
    release = request.args.get('release', 'dr16')
    run2d = request.args.get('run2d', None)
    plate = int(request.args.get('plate', None))
    mjd = int(request.args.get('mjd', None))
    fiber = int(request.args.get('fiber', 1))
    sample = int(request.args.get('sample', 1))
    bands = request.args.get('bands', 'all')
    debug_flag = request.args.get('debug', '')

    survey = 'sdss' if not run2d.startswith('v') else 'eboss'

    root = '/ssd0/sdss/%s/%s/spectro/redux/' % (release,survey)
    path = root + '%s/%04i/' % (run2d, plate)
    fname = path + 'spec-%04i-%05i-%04i.npy' %  (plate,mjd,fiber)

    print(fname)
    if bands == 'all':
        with open(fname, 'r') as fd:
            _bytes = fd.read()
        return web.response(body=_bytes)
    else:
        data = np.load(str(fname))
        dbands = data[[c for c in list(data.dtype.names) if c in bands]]
        dbands = rfn.repack_fields(dbands)

        tmp_file = NamedTemporaryFile(delete=False, dir='/tmp').name
        np.save(tmp_file, dbands, allow_pickle=False)
        with open(tmp_file+'.npy', 'r') as fd:
            _bytes = fd.read()
        os.unlink(tmp_file)
        return web.response(body=_bytes)



# ########################################################################
#  Application MAIN
#
async def start_async_app():
    runner = web.AppRunner(app_async)
    await runner.setup()
    site = web.TCPSite(
        runner, 'localhost', 6999)
    await site.start()
    print(f"Serving up app on  http://localhost:6999")
    return runner, site


if __name__ == '__main__':
    #  Parse the arguments
    parser = optparse.OptionParser()
    parser.add_option ('--port', '-p', action="store", dest="port",
                        help="Listener port number", default=6999)

    options, args = parser.parse_args()

    #  Start the application running on the requested port
    print ("Starting app....")
    app = web.Application()
    app.add_routes(routes)


    loop = asyncio.get_event_loop()
    runner, site = loop.run_until_complete(start_async_app())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.run_until_complete(runner.cleanup())

