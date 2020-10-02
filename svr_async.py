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

is_py3 = sys.version_info.major == 3


DEBUG = False                   # Debug flag

release = 'dr16'


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



# =======================================
# Data Service Endpoints
# =======================================

# GETSPEC -- Get a single spectrum 
#
@routes.post('/spec/getSpec')
async def getSpec(request):
    ''' 
    '''
    params = await request.post()
    try:
        id_list = params['id_list']
        bands = params['bands']                         # NYI
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

    # Instantiate the dataset service based on the context parameter.
    svc = getSvc(context)()

    # Convert the request parameter to a list
    if id_list[0] == '[':
         ids = map(int, id_list[1:-1].split(','))
    else:
         ids = np.array([np.uint64(id_list)])

    res = None
    align = (w0 != w1)
    nspec = 0
    for id in ids:
        nspec = nspec + 1
        fname = svc.dataPath(id, 'npy')
        data = np.load(str(fname))

        if not align:
            f = data
        else:
            debug = True
            wmin, wmax = data['loglam'][0], data['loglam'][-1]
            disp = float((wmax - wmin) / float(len(data['loglam'])))
            lpad = int(np.around(max((wmin - w0) / disp, 0.0)))
            rpad = int(np.around(max((w1 - wmax) / disp, 0.0)))
            if lpad == 0 and rpad == 0:
                f = data
            else:
                f = np.pad(data, (lpad,rpad), mode='constant',
                           constant_values=0)
            f['loglam'] = np.linspace(w0,w1,len(f))   # patch wavelength array

            if debug:
                print(fname)
                print ('wmin,wmax = (%g,%g)  disp=%g' % (wmin,wmax,disp))
                print ('w0,w1 = (%g,%g)  pad = (%d,%d)' % (w0,w1,lpad,rpad))
                print ('len f = %d   len data = %d' % (len(f),len(data)))

        if res is None:
            res = f
        else:
            res = np.vstack((res,f))

    if debug:
        print('res type: ' + str(type(res)))
        print('res shape: ' + str(res.shape))

    tmp_file = NamedTemporaryFile(delete=False, dir='/tmp').name
    np.save(tmp_file, res, allow_pickle=False)
    with open(tmp_file+'.npy', 'rb') as fd:
        _bytes = fd.read()
    os.unlink(tmp_file)

    if bands != 'all':
        data = np.load(str(fname))
        dbands = data[[c for c in list(data.dtype.names) if c in bands]]
        dbands = rfn.repack_fields(dbands)

        tmp_file = NamedTemporaryFile(delete=False, dir='/tmp').name
        np.save(tmp_file, dbands, allow_pickle=False)
        with open(tmp_file+'.npy', 'rb') as fd:
            _bytes = fd.read()
        os.unlink(tmp_file)

    en_time = time.time()
    logging.info ('getSpec time: %g  NSpec: %d' % (en_time-st_time,nspec))

    return web.Response(body=_bytes)


# PREVIEW -- Get a preview plot of a spectrum.
#
@routes.get('/spec/preview')
async def preview(request):
    """ Get a preview plot of a spectrum.
    """
    try:
        spec_id = request.query['id']
        context = request.query['context']
        profile = request.query['profile']
    except Exception as e:
        print ('ERROR: ' + str(e))

    # Instantiate the dataset service based on the context parameter.
    svc = getSvc(context)()
    fname = svc.previewPath(spec_id)

    return web.Response(body=svc.readData(fname))


# GRIDPLOT -- Return an image which is a grid plot of preview spectra.
#
@routes.post('/spec/plotGrid')
async def gridplot(request):
    """ Return an image which is a grid plot of preview spectra.
    """

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
        print ('sizes = %d %d' % (h_sizes[-1],v_sizes[-1]))
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

    ids = map(int, id_list[1:-1].split(','))
    svc = getSvc(context)()
    imgs = []
    for p in ids:
        fname = svc.previewPath(p)
        imgs.append(Image.open(fname))

    ret_img = pil_grid (imgs, ncols)

    retval = BytesIO()
    ret_img.save(retval, format='PNG')

    en_time = time.time()
    logging.info ('plotGrid time: %g' % (en_time - st_time))
    return web.Response(body=retval.getvalue())


# LISTSPAN -- Return the span in wavelength of an aligned list of IDs. 
#
@routes.post('/spec/listSpan')
async def listSpan(request):
    """ Return the span in wavelength of an aligned list of IDs. 
    """
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

    st_time = time.time()
    w0,w1,nspec = _listSpan(getSvc(context)(), id_list)
    en_time = time.time()
    logging.info ('listSpan time: %g  NSpec: %d' % (en_time-st_time,nspec))

    return web.Response(text='{"w0" : %f, "w1" : %f }' % (w0,w1))


# STACKEDIMAGE -- Return an image of stacked spectral flux arrays.
#
@routes.post('/spec/stackedImage')
async def stackedImage(request):
    """ Return an image of stacked spectral flux arrays.
    """
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

    svc = getSvc(context)()
    w0, w1, nspec = _listSpan(svc, id_list)

    img_data = None
    ids = map(int, id_list[1:-1].split(','))
    for q in ids:
        fname = svc.dataPath(q, 'npy')
        data = np.load(str(fname))

        wmin, wmax = data['loglam'][0], data['loglam'][-1]
        disp = float((wmax - wmin) / float(len(data['loglam'])))
        lpad = int(np.around(max((wmin - w0) / disp, 0.0)))
        rpad = int(np.around(max((w1 - wmax) / disp, 0.0)))

        if lpad == 0 and rpad == 0:
            f = data
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
    ids = map(int, id_list[1:-1].split(','))
    nids = 0
    for p in ids:
        nids = nids + 1
        fname = svc.dataPath(p, 'npy')
        data = np.load(str(fname))
        w0 = min(w0,data['loglam'][0])
        w1 = max(w1,data['loglam'][-1])
    return w0, w1, nids

def getSvc(context):
    try:
        svc = Registry.services[context]
    except Exception as e:
        logging.error ('getSvc(%s) ERROR: %s' % (context,str(e)))
        return None

    return svc


########################################################################
#  Application Start
########################################################################

# Enable basic logging.
logging.basicConfig(level=logging.DEBUG)

# Define the application.
app = web.Application()
app.add_routes(routes)

