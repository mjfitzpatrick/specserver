#!/usr/bin/env python
#
#  BENCH -- Benchmark services


from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = '20200807'  # yyyymmdd


import os
import sys
import time
import optparse
from flask import Flask
from flask import Response
from flask import request
from flask import send_file
from flask_cors import CORS

from io import BytesIO
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import h5py
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
import numpy as np
import json
import PIL.Image as Image


is_py3 = sys.version_info.major == 3

try:
    from urllib import unquote_plus               # Python 2
except ImportError:
    from urllib.parse import unquote_plus         # Python 3


# Flask Application
app = Flask(__name__)
CORS(app)

# Task configuration.
conf = {}                	# global configuration
DEF_CONFIG = ''                 # Default config file
EXT_CONFIG = ''                 # External config file (i.e. not in source tree)

DEBUG = False                   # Debug flag


# Test toplevel endpoint
@app.route('/')
def hello():
    return "Hello World from SDSS DAL Service! %s\n" % __version__



#------------------------------------------------------------------
# EXCEPTION AND REPSONSE HANDLERS
#------------------------------------------------------------------

class _InvalidRequest (Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self, message)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        """ Method to return a JSON formatting of the error.
        """
        rv = dict(self.payload or ())
        rv['message'] = self.message
        rv['code'] = self.status_code
        return rv

@app.errorhandler (_InvalidRequest)
def handle_invalid_request (error):
    response = app.make_response (('Error: ' + error.message,
	error.status_code, ''))
    return response


def stringResponse(s):
    '''stringResponse -- Force a return value to be type 'string' for all
                         Python versions.
    '''
    strval = s
    if is_py3 and isinstance(s,bytes):
        strval = str(s.decode())
    elif not is_py3 and (isinstance(s,bytes) or isinstance(s,unicode)):
        strval = str(s)
    else:
        strval = s

    return strval



#------------------------------------------------------------------
# TIMING ENDPOINTS
#------------------------------------------------------------------

mtype = 'application/octet-stream'

@app.route('/junk8G')
def junk8g():
        return send_file('/ssd0/tmp/junk8G', mimetype=mtype)

@app.route('/junk4G')
def junk4g():
        return send_file('/ssd0/tmp/junk4G', mimetype=mtype)

@app.route('/junk2G')
def junk2g():
        return send_file('/ssd0/tmp/junk2G', mimetype=mtype)

@app.route('/junk1G')
def junk1g():
        return send_file('/ssd0/tmp/junk1G', mimetype=mtype)

@app.route('/junk131M')
def junk131m():
        return send_file('/ssd0/tmp/junk131M', mimetype=mtype)

@app.route('/junk13M')
def junk13m():
        return send_file('/ssd0/tmp/junk13M', mimetype=mtype)

@app.route('/junk1M')
def junk1m():
        return send_file('/ssd0/tmp/junk1M', mimetype=mtype)

@app.route('/junk128K')
def junk128k():
        return send_file('/ssd0/tmp/junk128K', mimetype=mtype)

@app.route('/test_app', methods=['GET', 'POST'])
def test_app():
    from requests_toolbelt import MultipartEncoder

    f1, f2 = '/ssd0/tmp/junk128K','/ssd0/tmp/junk128K'
    m = MultipartEncoder(fields={
       'descr' : 'This is a test description',
       'json'  : '{"name" : "spec-123-01234-234.fits", "png" : "none"}',
       'file1' : (f1, open(f1, 'rb'), 'application/octet-stream'),
       'file2' : (f2, open(f2, 'rb'), 'application/octet-stream')
    })
    return Response(m.to_string(), mimetype=m.content_type)


#------------------------------------------------------------------
# ENDPOINTS
#------------------------------------------------------------------

# COADD -- 
#
@app.route('/coadd', methods = ['GET', 'POST'])
def coadd():
    """ 
    """
    if request.method == 'POST':              # Get the login user name
        release = request.form['release']
        run2d = request.form['run2d']
        plate = request.form['plate']
        mjd = request.form['mjd']
        fiber = request.form['fiber']
        sample = request.form['sample']
        if 'bands' in request.form:
            bands = request.form['bands']
        else:
            bands = 'all'
        debug_flag = request.form['debug']
    else:
        release = request.args.get('release', 'dr16')
        run2d = request.args.get('run2d', None)
        plate = request.args.get('plate', None)
        mjd = request.args.get('mjd', None)
        fiber = int(request.args.get('fiber', 1))
        sample = int(request.args.get('sample', 1))
        bands = request.args.get('bands', 'all')
        debug_flag = request.args.get('debug', '')

    run2d = int(run2d)
    plate = int(plate)
    mjd = int(mjd)

    root = '/ssd0/sdss/%s/sdss/spectro/redux/' % release
    path = root + '%d/%04i/' % (run2d, plate)
    fname = path + 'spec-%04i-%05i-%04i.npy' %  (plate,mjd,fiber)

    if bands == 'all':
        return send_file(fname, mimetype='application/octet-stream')
    else:
        from numpy.lib import recfunctions as rfn
        from tempfile import NamedTemporaryFile

        data = np.load(str(fname))
        cols = data.dtype.name
        dbands = data[[c for c in list(data.dtype.names) if c in bands]]
        dbands = rfn.repack_fields(dbands)
        #bobj = BytesIO()
        #np.save(bobj, dbands, allow_pickle=False)
        #result =  send_file(bobj, mimetype='application/octet-stream')
        #return result

        tmp_file = NamedTemporaryFile(delete=False, dir='/tmp').name
        np.save(tmp_file, dbands, allow_pickle=False)
        result =  send_file(tmp_file+'.npy', mimetype='application/octet-stream')
        os.unlink(tmp_file)
        return result



# PREVIEW -- 
#
@app.route('/preview', methods = ['GET', 'POST'])
def preview():
    """ 
    """
    if request.method == 'POST':              # Get the login user name
        release = request.form['release']
        run2d = int(request.form['run2d'])
        plate = int(request.form['plate'])
        mjd = int(request.form['mjd'])
        fiber = request.form['fiber']
    else:
        release = request.args.get('release', 'dr16')
        run2d = int(request.args.get('run2d', 0))
        plate = int(request.args.get('plate', 0))
        mjd = int(request.args.get('mjd', 0))
        fiber = int(request.args.get('fiber', 1))

    root = '/ssd0/sdss/%s/sdss/spectro/redux/' % release
    path = root + '%d/%04i/' % (run2d, plate)
    fname = path + 'spec-%04i-%05i-%04i.png' %  (plate,mjd,fiber)

    return send_file(fname, mimetype='application/octet-stream')



def pil_grid(images, max_horiz=np.iinfo(int).max):
    '''Examples:  pil_grid(imgs)      horizontal
                  pil_grid(imgs,3)    3-col grid
                  pil_grid(imgs,1)    vertical
    '''
    n_images = len(images)
    n_horiz = min(n_images, max_horiz)
    h_sizes = [0] * n_horiz
    v_sizes = [0] * ((n_images/ n_horiz) + (1 if n_images % n_horiz > 0 else 0))
    for i, im in enumerate(images):
        h, v = i % n_horiz, i // n_horiz
        h_sizes[h] = max(h_sizes[h], im.size[0])
        v_sizes[v] = max(v_sizes[v], im.size[1])
    h_sizes, v_sizes = np.cumsum([0] + h_sizes), np.cumsum([0] + v_sizes)
    im_grid = Image.new('RGB', (h_sizes[-1], v_sizes[-1]), color='white')
    for i, im in enumerate(images):
        im_grid.paste(im, (h_sizes[i % n_horiz], v_sizes[i // n_horiz]))
    return im_grid


# PPLOT -- 
#
@app.route('/pplot', methods = ['GET', 'POST'])
def pplot():
    """ Plate plot
    """
    if request.method == 'POST':              # Get the login user name
        release = request.form['release']
        run2d = int(request.form['run2d'])
        plate = int(request.form['plate'])
        mjd = int(request.form['mjd'])
        fiber_start = request.form['start']
        fiber_end = request.form['end']
        ncols = request.form['ncols']
    else:
        release = request.args.get('release', 'dr16')
        run2d = int(request.args.get('run2d', 0))
        plate = int(request.args.get('plate', 0))
        mjd = int(request.args.get('mjd', 0))
        fiber_start = int(request.args.get('start', 1))
        fiber_end = int(request.args.get('end', 64))
        ncols = int(request.args.get('ncols', 4))

    root = '/ssd0/sdss/%s/sdss/spectro/redux/' % release
    path = root + '%d/%04i/' % (run2d, plate)

    imgs = []
    for p in range(fiber_start, fiber_end):
        fname = path + 'spec-%04i-%05i-%04i.png' %  (plate,mjd,p)
        imgs.append(Image.open(fname))

    ret_img = pil_grid (imgs, ncols)
    ret_img.save('/tmp/grid.png')

    return send_file('/tmp/grid.png', mimetype='application/octet-stream')



# PLATE -- Get all spectra for a given plate.
#
@app.route('/plate', methods = ['GET', 'POST'])
def plate():
    """ 
    """
    if request.method == 'POST':              # Get the login user name
        run2d = request.form['run2d']
        plate = request.form['plate']
        mjd = request.form['mjd']
        fmt = 'hdf5'
        debug_flag = request.form['debug']
    else:
        run2d = request.args.get('run2d', None)
        plate = request.args.get('plate', None)
        mjd = request.args.get('mjd', None)
        fmt = request.args.get('fmt', 'hdf5')
        debug_flag = request.args.get('debug', '')

    run2d, plate, mjd = 26, 1263, 52708		# FIXME

    if fmt == 'spPlate':
        from astropy.io import fits
        path = "/net/mss1/archive/hlsp/sdss/dr8/sdss/spectro/redux/26/%04i/spPlate-%04i-%05i.fits" % (plate,plate,mjd)
        hdulist = fits.open(path)
        arr = hdulist[0].data

    elif fmt == 'hdf':
        arr = np.empty((3856,640), float)
        for fiber in range (1, 640):
            path = "/dr8/sdss/spectro/redux/%d/spectra/%04i/%05i/%i/DATA/flux" % (run2d,plate,mjd,fiber)
            _spec = hfd[path][:]
            #arr[:][fiber] = _spec

    print ('len = ' + str(len(arr)))

    return send_file(BytesIO(arr), mimetype='application/octet-stream')


# ATTR -- Get the named attribute for the spectrum.
#
@app.route('/attr', methods = ['GET', 'POST'])
def attr():
    """ 
    """
    if request.method == 'POST':              # Get the login user name
        run2d = request.form['run2d']
        plate = request.form['plate']
        mjd = request.form['mjd']
        fiber = int(request.form['fiber'])
        name = request.form['name']
        debug_flag = request.form['debug']
    else:
        run2d = request.args.get('run2d', None)
        plate = request.args.get('plate', None)
        mjd = request.args.get('mjd', None)
        fiber = int(request.args.get('fiber', 1))
        name = request.args.get('name', '')
        debug_flag = request.args.get('debug', '')

    run2f = 26		# FIXME
    plate = 1263	# FIXME
    mjd = 52708		# FIXME
    path = "/dr8/sdss/spectro/redux/%d/spectra/%04i/%05i/%i" % \
            (run2d,plate,mjd,fiber)

    value = hfd[path].attrs.get(name)
    return stringResponse(value)


# ATTRS -- Get the spectrum attributes as a JSON string.
#
@app.route('/attrs', methods = ['GET', 'POST'])
def attrs():
    """ 
    """
    if request.method == 'POST':              # Get the login user name
        run2d = request.form['run2d']
        plate = request.form['plate']
        mjd = request.form['mjd']
        fiber = int(request.form['fiber'])
        name = request.form['name']
        debug_flag = request.form['debug']
    else:
        run2d = request.args.get('run2d', None)
        plate = request.args.get('plate', None)
        mjd = request.args.get('mjd', None)
        fiber = int(request.args.get('fiber', 1))
        name = request.args.get('name', '')
        debug_flag = request.args.get('debug', '')

    run2d = 26		# FIXME
    plate = 1263	# FIXME
    mjd = 52708		# FIXME
    path = "/dr8/sdss/spectro/redux/%d/spectra/%04i/%05i/%i" % \
            (run2d,plate,mjd,fiber)

    resp = {}
    for k,v in hfd[path].attrs.items():
        if isinstance(v, np.bytes_):
            resp[k] = stringResponse(v)
        else:
            resp[k] = v.item()		# convert to native type
    return json.dumps(resp)



# ########################################################################
# SHUTDOWN -- Exit the task and clean up.
#
#  Utility shutdown procedure
def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError ('Not running with the Werkzeug Server')
    func()

@app.route('/shutdown', methods = ['GET'])
def shutdown():
    if valid():
        shutdown_server()
        return "Auth Manager shutting down ....\n"
    else:
        raise _InvalidRequest ('The provided DataLab token is invalid.', 401)

# DEBUG -- Toggle authmanager debugging information printed to logs.
#
@app.route('/debug', methods = ['GET'])
def debug():
    global DEBUG
    DEBUG = not DEBUG			# toggle boolean value
    return str(DEBUG)

# PARSECONFIG -- Parse the task configuration file.  This is a simple
# <keyw>=<val> file with one option per line.
#
def parseConfig(file):
    rawconf = open(file).read()
    lines = rawconf.strip().split("\n")
    for line in lines:
        parts = line.strip().split("=")
        if '{' in parts[1]:
          conf[parts[0].strip()] = '='.join(parts[1:])
        else:
          conf[parts[0].strip()] = parts[1].strip()



# ########################################################################
#  Application MAIN
#
if __name__ == '__main__':
    #  Parse the arguments
    parser = optparse.OptionParser()
    parser.add_option ('--port', '-p', action="store", dest="port",
                        help="Listener port number", default=6999)
    parser.add_option ('--config', '-c', dest="config",
                        help="Configuration file", default="sdss_dal.conf")

    options, args = parser.parse_args()

    # Parse the configuration file specified by '--config' or else use
    # the default in the current directory.
    if os.path.exists(options.config):
        parseConfig(options.config)
    elif os.path.exists(EXT_CONFIG):
        parseConfig(EXT_CONFIG)
    elif os.path.exists(DEF_CONFIG):
        parseConfig(DEF_CONFIG)
    #else:
    #    raise Exception ('No config file found.')


    #fname = 'plate-1263.hdf5'
    #fname = '/dl1/users/fitz/dr8.hdf5'
    #hfd = h5py.File (fname,'r')

    #  Start the application running on the requested port
    app.debug = True
    print ('port = ' + str(options.port))
    app.run ('0.0.0.0', int(options.port))

else:
    # For an Nginx/UWSGI deployment.
    if os.path.exists(EXT_CONFIG):
        parseConfig(EXT_CONFIG)
    elif os.path.exists(DEF_CONFIG):
        parseConfig(DEF_CONFIG)
    #else:
    #    raise Exception ('No config file found.')
