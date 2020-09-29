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
from flask import Flask, Response, request, send_file
from flask_cors import CORS

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


# Flask Application
app = Flask(__name__)
CORS(app)

DEBUG = False                   # Debug flag

# _files
#   100M -rw-r--r-- 1 fitz fitz 100M Aug 13 14:52 junk100M
#    10M -rw-r--r-- 1 fitz fitz  10M Aug 13 14:52 junk10M
#   128K -rw-r--r-- 1 fitz fitz 128K Aug 13 14:53 junk128K
#    12K -rw-r--r-- 1 fitz fitz  12K Aug 13 14:53 junk12K
#   1.0M -rw-r--r-- 1 fitz fitz 1.0M Aug 13 14:52 junk1M
#   125M -rw-r--r-- 1 fitz fitz 125M Aug 13 14:56 spec1000


# Test toplevel endpoint
@app.route('/')
def hello():
    return "Hello World from Benchmark Service! %s\n" % __version__



mtype = 'application/octet-stream'

@app.route('/junk125M')
def junk125m():
        return send_file('/home/fitz/specdal/bench/junk125M', mimetype=mtype)

@app.route('/junk128K')
def junk128k():
        return send_file('/home/fitz/specdal/bench/junk128K', mimetype=mtype)

@app.route('/junk12K')
def junk12k():
        return send_file('/home/fitz/specdal/bench/junk12K', mimetype=mtype)




# ########################################################################
#  Application MAIN
#

if __name__ == '__main__':
    #  Parse the arguments
    parser = optparse.OptionParser()
    parser.add_option ('--port', '-p', action="store", dest="port",
                        help="Listener port number", default=6999)

    options, args = parser.parse_args()

    #  Start the application running on the requested port
    app.debug = True
    app.run ('0.0.0.0', int(options.port))

