#!/usr/bin/env python
#
#  SVR_ASYNC -- ASync implementation of the spectro service.
#

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = '20200907'  # yyyymmdd


import os
import sys
import time
import json
import optparse
import logging
from PIL import Image
from io import BytesIO

import numpy as np
from numpy.lib import recfunctions as rfn
from tempfile import NamedTemporaryFile
import matplotlib as mpl
from astropy.visualization import ZScaleInterval

import asyncio
from aiohttp import web

import Registry


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


DEBUG = False                   # Debug flag
config = {}			# Global config file
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
    import socket
    global config

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
        raise Exception ("No such config file: " + file)


# Parse the configuration file specified by '--config' or else use
# the default in the current directory.
if os.path.exists('spec.conf'):
    parseConfig('spec.conf')
elif os.path.exists(EXT_CONFIG):
    parseConfig(EXT_CONFIG)
elif os.path.exists(DEF_CONFIG):
    parseConfig(DEF_CONFIG)



# =======================================
#  Service Admin Endpoints
# =======================================

# Test toplevel endpoints
@routes.get('/spec')
async def hello(request):
    '''Simple ping acknowledgement.
    '''
    return web.Response(text="Hello from Spectro Service! %s\n" % __version__)

@routes.get('/ping')
async def ping(request):
    '''Simple ping acknowledgement.
    '''
    return web.Response(text="OK")


# AVAILABLE -- Return service availability.
#
@routes.get('/spec/available')
async def available(request):
    '''Get service availability.  This method is simply meant to
       return the availability status of the service itself,
    '''
    return web.Response(text="True") # return string repr, not a boolean


# SHUTDOWN -- Shut down the service.
#
@routes.get('/spec/shutdown')
async def shutdown(request):
    '''Shutdown the service.
    '''
    def shutdown_server():
       '''Exit the task and clean up.
       '''
       pass

    shutdown_server()
    return web.Response(text="Spectro service shutting down ....\n")


# DEBUG -- Toggle debugging flag in the service.
#
@routes.get('/spec/debug')
async def debug(request):
    '''Toggle debugging flag in the service.
    '''
    global debug
    debug = not debug
    return web.Response(text=str(debug))


# PROFILES -- List the available profiles.
#
@routes.get('/spec/profiles')
def profiles(request):
    '''List the available profiles.
    '''
    # Process the service parameters.
    profile = request.query['profile']
    fmt = request.query['format']

    if profile is None or profile == '' or profile.lower() == 'none':
        profiles = config['profiles'].keys()            # Py2 version

        if fmt == 'csv':
            return ",".join(profiles)
        elif fmt == 'text':
            txt = ''
            for p in profiles:
                prof = config['profiles'][p]
                if prof['type'] in ['public','external']:
                    txt = txt + ("%16s   %s\n" % (p,str(prof['description'])))
            return web.Response(text=txt)
    else:
        raw = config['profiles'][profile].copy()
        del raw['vosEndpoint']                 # delete secrets from the copy
        del raw['vosRootDir']
        return web.Response(text=json.dumps(raw))


# CONTEXTS -- List the available contexts.
#
@routes.get('/spec/contexts')
def contexts(request):
    '''List the available contexts.
    '''
    # Process the service parameters.
    context = request.query['context']
    fmt = request.query['format']

    if context is None or context == '' or context.lower() == 'none':
        context = config['contexts'].keys()            # Py2 version

        if fmt == 'csv':
            return ",".join(context)
        elif fmt == 'json':
            return web.Response(text=json.dumps(config['contexts']))
        elif fmt == 'text':
            txt = ''
            for p in context:
                conf = config['contexts'][p]
                if conf['type'] in ['public','external']:
                    txt = txt + ("%16s   %s\n" % (p,str(conf['description'])))
            return web.Response(text=txt)
    else:
        raw = config['contexts'][context]
        return web.Response(text=json.dumps(raw))


# CATALOGS -- List the available catalogs for a given dataset context.
#
@routes.get('/spec/catalogs')
def catalogs(request):
    '''List the available catalogs for a given dataset context.
    '''
    # Process the service parameters.
    context = request.query['context']
    profile = request.query['profile']
    fmt = request.query['format']

    catalogs = config['contexts'][context]['catalogs'].keys()

    if fmt == 'csv':
        txt = 'catalog_name,description\n'
        for p in catalogs:
            cat = config['contexts'][context]['catalogs'][p]
            txt = txt + ("%s,%s\n" % (p, cat))
    elif fmt == 'text':
        txt = "Catalogs used by '%s' context:\n\n" % context
        for p in catalogs:
            cat = config['contexts'][context]['catalogs'][p]
            txt = txt + ("%30s   %s\n" % (p, cat))
    return web.Response(text=txt)


# VALIDATE -- Validate a client parameter.
#
@routes.get('/spec/validate')
def validate(request):
    '''Validate a client parameter.
    '''
    # Process the service parameters.
    what = request.query['what']	# what to validate (context|profile)
    value = request.query['value']	# value to validate

    if what == 'context':
        resp = 'OK' if value in config['contexts'].keys() else 'Error'
    elif what == 'profile':
        resp = 'OK' if value in config['profiles'].keys() else 'Error'
    else:
        resp = 'Error: unknown validation "%s"' % what

    return web.Response(text=resp)


# =======================================
# Data Service Endpoints
# =======================================

# QUERY -- Query for spectra.
#
@routes.get('/spec/query')
async def query(request):
    '''Query the context catalog for information.
    '''
    try:
        id = request.query['id']
        fields = request.query['fields']
        catalog = request.query['catalog']
        cond = request.query['cond']
        context = request.query['context']
        profile = request.query['profile']
        debug = (request.query['debug'].lower() == 'true')
        verbose = (request.query['verbose'].lower() == 'true')
    except Exception as e:
        logging.error ('Param Error: ' + str(e))
        return web.Response(text='Param Error: ' + str(e))

    st_time = time.time()

    # Instantiate the service based on the context.
    svc = _getSvc(context)
    svc.debug = debug
    svc.verbose = verbose

    # Call the dataset-specific query method.  This allows the service to
    # do any data-specific formatting.  The result is always returned as a
    # csv string.
    return web.Response(text=svc.query(id, fields, catalog, cond))


# GETSPEC -- Get a spectra from the data service.
#
@routes.post('/spec/getSpec')
async def getSpec(request):
    ''' 
    '''
    params = await request.post()
    try:
        id_list = params['id_list']
        values = params['values']                       # NYI
        cutout = params['cutout']                       # NYI
        fmt = params['format']
        align = (params['align'].lower() == 'true')
        w0 = float(params['w0'])
        w1 = float(params['w1'])
        context = params['context']
        profile = params['profile']
        debug = (params['debug'].lower() == 'true')
        verbose = (params['verbose'].lower() == 'true')
    except Exception as e:
        logging.error ('Param Error: ' + str(e))
        return web.Response(text='Param Error: ' + str(e))

    st_time = time.time()

    # Instantiate the dataset service based on the context.
    svc = _getSvc(context)
    svc.debug = debug
    svc.verbose = verbose

    # From the service call we get a string which we'll need to map to
    # an array of identifiers valid for the service.
    ids = svc.expandIDList(id_list)
    if debug:
        print('GETSPEC ----------')
        print('len ids = ' + str(len(ids)))
        print('ty ids = ' + str(type(ids)))
        print('ty ids elem = ' + str(type(ids[0])))

    # If called from something other than the client API we might not know
    # the wavelength limits of the collection, so compute it here so we can
    # still align properly.
    if w0 in [None, 0.0] and w1 in [None, 0.0] and align:
        w0, w1, nspec = _listSpan(svc, ids)
        
    res = None
    align = (w0 != w1)
    nspec = 0
    ptime = 0.0
    for id in ids:
        p0 = time.time()
        nspec = nspec + 1
        if fmt.lower() == 'fits':
            fname = svc.dataPath(id, 'fits')
            data = svc.readFile(str(fname))
            return web.Response(body=data)
        else:
            fname = svc.dataPath(id, 'npy')
            data = svc.getData(str(fname))

        if values != 'all':
            # Extract the subset of values.
            dvalues = data[[c for c in list(data.dtype.names) if c in values]]
            data = rfn.repack_fields(dvalues)

        if not align:
            f = data
        else:
            wmin, wmax = data['loglam'][0], data['loglam'][-1]
            disp = float((wmax - wmin) / float(len(data['loglam'])))
            lpad = int(np.around(max((wmin - w0) / disp, 0.0)))
            rpad = int(np.around(max((w1 - wmax) / disp, 0.0)))
            if lpad == 0 and rpad == 0:
                f = data
            else:
                f = np.pad(data, (lpad,rpad), mode='constant',
                           constant_values=0)
                f['loglam'] = np.linspace(w0,w1,len(f)) # patch wavelength array

            if debug:
                print(str(id))
                print(fname)
                print('wmin,wmax = (%g,%g)  disp=%g' % (wmin,wmax,disp))
                print('w0,w1 = (%g,%g)  pad = (%d,%d)' % (w0,w1,lpad,rpad))
                print('len f = %d   len data = %d' % (len(f),len(data)))

        if res is None:
            res = f
        else:
            res = np.vstack((res,f))
        p1 = time.time()
        ptime = ptime + (p1 - p0)

    if debug:
        print('res type: ' + str(type(res)) + ' shape: ' + str(res.shape))

    # Convert the array to bytes for return.
    fd = BytesIO()
    np.save(fd, res, allow_pickle=False)
    _bytes = fd.getvalue()

    en_time = time.time()
    logging.info ('getSpec time: %g  NSpec: %d  Bytes: %d' % \
                  (en_time-st_time,nspec,len(_bytes)))

    return web.Response(body=_bytes)


# PREVIEW -- Get a preview plot of a spectrum.
#
@routes.get('/spec/preview')
async def preview(request):
    ''' Get a preview plot of a spectrum.
    '''
    try:
        spec_id = request.query['id']
        context = request.query['context']
        profile = request.query['profile']
    except Exception as e:
        print ('ERROR: ' + str(e))

    # Instantiate the dataset service based on the context parameter.
    svc = _getSvc(context)
    fname = svc.previewPath(spec_id)

    return web.Response(body=svc.readFile(fname))


# GRIDPLOT -- Return an image which is a grid plot of preview spectra.
#
@routes.post('/spec/plotGrid')
async def gridplot(request):
    ''' Return an image which is a grid plot of preview spectra.
    '''

    def pil_grid(images, max_horiz=np.iinfo(int).max):
        '''Examples:  pil_grid(imgs)      horizontal
                      pil_grid(imgs,3)    3-col grid
                      pil_grid(imgs,1)    vertical
        '''
        n_images = len(images)
        n_horiz = min(n_images, max_horiz)
        h_sizes = [0] * n_horiz
        v_sizes = [0] * (int(n_images/n_horiz) + \
                         (1 if n_images % n_horiz > 0 else 0))
        for i, im in enumerate(images):
            h, v = i % n_horiz, i // n_horiz
            h_sizes[h] = max(h_sizes[h], im.size[0])
            v_sizes[v] = max(v_sizes[v], im.size[1])
        h_sizes, v_sizes = np.cumsum([0] + h_sizes), np.cumsum([0] + v_sizes)
        im_grid = Image.new('RGB', (h_sizes[-1],v_sizes[-1]),color='white')
        for i, im in enumerate(images):
            im_grid.paste(im, (h_sizes[i % n_horiz], v_sizes[i // n_horiz]))
        return im_grid

    st_time = time.time()

    # Process the service request arguments.
    params = await request.post()
    try:
        id_list = params['id_list']
        ncols = int(params['ncols'])
        context = params['context']
        profile = params['profile']
        debug = (params['debug'].lower() == 'true')
        verbose = (params['verbose'].lower() == 'true')
    except Exception as e:
        logging.error ('Param Error: ' + str(e))
        return web.Response(text='Param Error: ' + str(e))

    if debug:
        logging.info ('gridPlot Params: ' + str(dict(params)))

    ids = map(np.uint64, id_list[1:-1].split(','))
    svc = _getSvc(context)
    imgs = []
    for p in ids:
        fname = svc.previewPath(p)
        imgs.append(Image.open(fname))

    ret_img = pil_grid (imgs, max_horiz=ncols)

    retval = BytesIO()
    ret_img.save(retval, format='PNG')

    en_time = time.time()
    logging.info ('plotGrid time: %g' % (en_time - st_time))
    return web.Response(body=retval.getvalue())


# LISTSPAN -- Return the span in wavelength of an aligned list of IDs. 
#
@routes.post('/spec/listSpan')
async def listSpan(request):
    ''' Return the span in wavelength of an aligned list of IDs. 
    '''
    # Process the service request arguments.
    params = await request.post()
    try:
        id_list = params['id_list']
        context = params['context']
        profile = params['profile']
        debug = (params['debug'].lower() == 'true')
        verbose = (params['verbose'].lower() == 'true')
    except Exception as e:
        logging.error ('Param Error: ' + str(e))
        return web.Response(text='Param Error: ' + str(e))

    svc = _getSvc(context)
    ids = svc.expandIDList(id_list)

    st_time = time.time()
    w0,w1,nspec = _listSpan(svc, ids)
    en_time = time.time()
    logging.info ('listSpan time: %g  NSpec: %d' % (en_time-st_time,nspec))

    return web.Response(text='{"w0" : %f, "w1" : %f }' % (w0,w1))


# STACKEDIMAGE -- Return an image of stacked spectral flux arrays.
#
@routes.post('/spec/stackedImage')
async def stackedImage(request):
    ''' Return an image of stacked spectral flux arrays.
    '''
    # Process the service request arguments.
    params = await request.post()
    try:
        id_list = params['id_list']
        thick = int(params['thickness'])
        inverse = (params['inverse'].lower() == 'true')
        cmap = params['cmap']
        xscale = float(params['xscale'])
        yscale = float(params['yscale'])
        width = int(params['width'])
        height = int(params['height'])
        context = params['context']
        profile = params['profile']
        debug = (params['debug'].lower() == 'true')
        verbose = (params['verbose'].lower() == 'true')
    except Exception as e:
        logging.error ('Param Error: ' + str(e))
        return web.Response(text='Param Error: ' + str(e))

    if debug:
        logging.info ('stackImage Params: ' + str(dict(params)))

    st_time = time.time()

    svc = _getSvc(context)
    ids = svc.expandIDList(id_list)
    w0, w1, nspec = _listSpan(svc, ids)

    img_data = None
    ids = map(int, id_list[1:-1].split(','))
    for q in ids:
        fname = svc.dataPath(q, 'npy')
        data = svc.getData(str(fname))

        wmin, wmax = data['loglam'][0], data['loglam'][-1]
        disp = float((wmax - wmin) / float(len(data['loglam'])))
        lpad = int(np.around(max((wmin - w0) / disp, 0.0)))
        rpad = int(np.around(max((w1 - wmax) / disp, 0.0)))

        if lpad == 0 and rpad == 0:
            f = data['flux']
        else:
            f = np.pad(data['flux'], (lpad,rpad), mode='constant',
                       constant_values=0)
        if img_data is None:
            img_data = np.array(f)
        else:
            for i in range(thick):
                img_data = np.vstack((img_data,f))

    # Apply the scaling and requested colormap.
    zscale = ZScaleInterval()
    z1, z2 = zscale.get_limits(img_data)
    rescaled = (255.0 / z2 * (img_data - z1)).astype(np.uint8)
    if inverse:
        rescaled = 255 - rescaled
    if cmap != 'gray':
        cm_cmap = mpl.cm.get_cmap(cmap)
        rescaled = np.uint8(cm_cmap(rescaled) * 255)

    # Create the image.
    image = Image.fromarray(rescaled)
    if xscale != 1.0 or yscale != 1.0:
        new_size = ( int(image.size[0]*xscale), int(image.size[1]*yscale))
        image = image.resize(new_size)
    elif width != 0 or height != 0:
        image = image.resize((height, width))
    if debug: print('scaled image size: ' + str(image.size))

    retval = BytesIO()
    image.save(retval, format='PNG')

    en_time = time.time()
    logging.info ('stackedImage time: %g  NSpec: %d' % (en_time-st_time,nspec))
    return web.Response(body=retval.getvalue())



# =======================================
# Benchmarking Test Endpoints
# =======================================

mtype = 'application/octet-stream'

@routes.get('/spec/junk125M')
async def junk125m(request):
    with open(os.getcwd() + '/bench/junk125M', 'rb') as fd:
        _bytes = fd.read()
    return web.Response(body=_bytes)

@routes.get('/spec/junk128K')
async def junk128k(request):
    with open(os.getcwd() + '/bench/junk128K', 'rb') as fd:
        _bytes = fd.read()
    return web.Response(body=_bytes)

@routes.get('/spec/junk12K')
async def junk12k(request):
    with open(os.getcwd() + '/bench/junk12K', 'rb') as fd:
        _bytes = fd.read()
    return web.Response(body=_bytes)



# =======================================
# Test/Demo Endpoints
# =======================================

@routes.post('/spec/test_post')
async def test_post(request):
    print ('*** type(request) = ' + str(type(request)))
    print ('*** request(post) = ' + str(request.post))
    print ('*** request(query) = ' + str(request.query))
    print ('*** request(headers) = ' + str(request.headers))
    print ('*** request(host) = ' + str(request.host))
    print ('*** request(path) = ' + str(request.path))
    print ('*** request(content_length) = ' + str(request.content_length))
    print ('*** request(query_string) = ' + str(request.query_string))

    data = await request.post()
    print ('*** type(data) = ' + str(type(data)))
    print ('*** data = ' + str(dict(data)))

    hdrs = request.headers
    print ('*** HOST: ' + hdrs['Host'])
    print ('*** Content-Type: ' + hdrs['Content-Type'])
    print ('*** Content-Length: ' + hdrs['Content-Length'])

    try:
        foo = data['foo']
        oof = data['oof']
    except Exception as e:
        print ('Missing Param Error: ' + str(e))
    else:
        print ('foo: ' + foo + '  req: ' + data['foo'])
        print ('oof: ' + oof + '  req: ' + data['oof'])

    return web.Response(text='OK')


@routes.get('/spec/test_json')
async def test_json(request):

    data = np.load('./zz.npy')
    req = {'id' : 2210146812474530816,
           'xdim' : 100,
           'ydim' : 200,
           'data' : BytesIO(data)
          }

    with open(BytesIO(req), 'rb') as fd:
        _bytes = fd.read()

    return web.Response(body=_bytes)
    

# =======================================
# Utility Methods
# =======================================

def _listSpan(svc, id_list):
    ''' Find the min max wavelength span an of ID list.
    '''
    w0 = 100000
    w1 = -99999
    #ids = map(int, id_list[1:-1].split(','))
    #ids = svc.expandIDList(id_list)
    ids = id_list
    nids = 0
    for p in ids:
        nids = nids + 1
        fname = svc.dataPath(p, 'npy')
        data = svc.getData(str(fname))
        w0 = min(w0,data['loglam'][0])
        w1 = max(w1,data['loglam'][-1])
    return w0, w1, nids

def _getSvc(context):
    '''Return the servie sub-class based on the given context.
    '''
    try:
        svc = Registry.services[context]
    except Exception as e:
        logging.error ('_getSvc(%s) ERROR: %s' % (context, str(e)))
        return None
    else:
        return svc


########################################################################
#  Application Start
########################################################################

# Enable basic logging.
logging.basicConfig(level=logging.DEBUG)

# Define the application.
app = web.Application(client_max_size=4096**2)
app.add_routes(routes)

