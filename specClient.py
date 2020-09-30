#!/usr/bin/env python
#
# SPECCLIENT -- Client methods for the Spectroscopic Data Service
#

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = 'v1.0.0'


'''
    Client methods for the Spectroscopic Data Service.

    Spectro Client Interface
    ------------------------

        client = getClient  (context='<context>', profile='<profile>')

          status = isAlive  (svc_url=DEF_SERVICE_URL, timeout=2)

               set_svc_url  (svc_url)
     svc_url = get_svc_url  ()

               set_context  (context)
         ctx = get_context  ()
      ctxs = list_contexts  (optval, token=None, contexts=None, fmt='text')
      ctxs = list_contexts  (token=None, contexts=None, fmt='text')

               set_profile  (profile)
        prof = get_profile  ()
     profs = list_profiles  (optval, token=None, profile=None, fmt='text')
     profs = list_profiles  (token=None, profile=None, fmt='text')

           svcs = services  (name=None, fmt=None, profile='default')

    QUERY INTERFACE:
            id_list = query (<region> | <coord, size> | <ra, dec, size>,
                             constraint=<sql_where_clause>,
                             context=None, profile=None, **kw)

    ACCESS INTERFACE:
            list = getSpec  (id_list, fmt='numpy',
                             out=None, align=False, cutout=None,
                             context=None, profile=None, **kw)

    PLOT  INTERFACE:
                      plot  (spec, context=context, profile=profile, **kw)
         status = prospect  (spec, context=context, profile=profile, **kw)
           image = preview  (id, context=context, profile=profile, **kw)
          image = plotGrid  (id_list, nx, ny, page=<N>,
                             context=context, profile=profile, **kw)
      image = stackedImage  (id_list, fmt='png|numpy',
                             align=False, yflip=False,
                             context=context, profile=profile, **kw)

Import via

.. code-block:: python

    from dl import specClient
'''

import os
import sys
import socket
import json
from time import gmtime
from time import strftime
from urllib.parse import urlencode          # Python 3
import numpy as np
import pandas as pd
from io import BytesIO

# Turn off some annoying astropy warnings

import warnings
from astropy.utils.exceptions import AstropyWarning
warnings.simplefilter('ignore', AstropyWarning)

import logging
logging.getLogger("specutils").setLevel(logging.CRITICAL)
from specutils import Spectrum1D

from astropy import units as u

from matplotlib import pyplot as plt      # visualization libs
from specLines import _em_lines
from specLines import _abs_lines

try:
    import pycurl_requests as requests
except ImportError:
    import requests
import pycurl

# Data Lab imports.
from dl import queryClient
from dl import storeClient
from dl.Util import def_token
from dl.Util import multimethod


# Python version check.
is_py3 = sys.version_info.major == 3


# The URL of the service to access.  This may be changed by passing a new
# URL into the set_svc_url() method before beginning.

DEF_SERVICE_ROOT = "https://datalab.noao.edu"

# Allow the service URL for dev/test systems to override the default.
THIS_HOST = socket.gethostname()
if THIS_HOST[:5] == 'dldev':
    DEF_SERVICE_ROOT = "http://dldev.datalab.noao.edu"
elif THIS_HOST[:6] == 'dltest':
    DEF_SERVICE_ROOT = "http://dltest.datalab.noao.edu"

elif THIS_HOST[:5] == 'munch':                          # DELETE ME
    DEF_SERVICE_ROOT = "http://localhost:6999"          # DELETE ME

# Allow the service URL for dev/test systems to override the default.
sock = socket.socket(type=socket.SOCK_DGRAM)     # host IP address
sock.connect(('8.8.8.8', 1))        # Example IP address, see RFC 5737
THIS_IP, _ = sock.getsockname()

DEF_SERVICE_URL = DEF_SERVICE_ROOT + "/spec"
SM_SERVICE_URL  = DEF_SERVICE_ROOT + "/storage"
QM_SERVICE_URL  = DEF_SERVICE_ROOT + "/query"

# Use cURL for requests when possible.
USE_CURL = True

# The requested service "profile".  A profile refers to the specific
# machines and services used by the service.

DEF_SERVICE_PROFILE = "default"

# The requested dataset "context". A context refers to the specific dataset
# being served.  This determines what is allowed within certain methods.

DEF_SERVICE_CONTEXT = "default"

# Use a /tmp/AM_DEBUG file as a way to turn on debugging in the client code.
DEBUG = os.path.isfile('/tmp/SDC_DEBUG')


# ######################################################################
#
#  Spectroscopic Data Client Interface
#
#  This API provides convenience methods that allow an application to
#  import the Client class without having to explicitly instantiate a
#  class object.  The parameter descriptions and example usage is given
#  in the comments for the class methods.  Module methods have their
#  docstrings patched below.
#
# ######################################################################


# ###################################
#  Spectroscopic Data error class
# ###################################

class dlSpecError(Exception):
    '''A throwable error class.
    '''
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message



# -----------------------------
#  Utility Methods
# -----------------------------

# --------------------------------------------------------------------
# SET_SVC_URL -- Set the ServiceURL to call.
#
def set_svc_url(svc_url):
    return spc_client.set_svc_url(svc_url.strip('/'))

# --------------------------------------------------------------------
# GET_SVC_URL -- Get the ServiceURL to call.
#
def get_svc_url():
    return spc_client.get_svc_url()

# --------------------------------------------------------------------
# SET_PROFILE -- Set the service profile to use.
#
def set_profile(profile):
    return spc_client.set_profile(profile)

# --------------------------------------------------------------------
# GET_PROFILE -- Get the service profile to use.
#
def get_profile():
    return spc_client.get_profile()

# --------------------------------------------------------------------
# SET_CONTEXT -- Set the dataset context to use.
#
def set_context(context):
    return spc_client.set_context(profile)

# --------------------------------------------------------------------
# GET_CONTEXT -- Get the dataset context to use.
#
def get_context():
    return spc_client.get_profile()

# --------------------------------------------------------------------
# ISALIVE -- Ping the service to see if it responds.
#
def isAlive(svc_url=DEF_SERVICE_URL, timeout=5):
    return spc_client.isAlive(svc_url=svc_url, timeout=timeout)


# --------------------------------------------------------------------
# LIST_PROFILES -- List the available service profiles.
#
@multimethod('spc',1,False)
def list_profiles(optval, token=None, profile=None, fmt='text'):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return spc_client._list_profiles(token=def_token(optval),
                                         profile=profile, fmt=format)
    else:
        # optval looks like a profile name
        return spc_client._list_profiles(token=def_token(token),
                                         profile=optval, fmt=format)

@multimethod('spc',0,False)
def list_profiles(token=None, profile=None, fmt='text'):
    '''Retrieve the profiles supported by the spectro data  service.

    Usage:
        list_profiles (token=None, profile=None, fmt='text')

    MultiMethod Usage:  
    ------------------
            specClient.list_profiles (token)
            specClient.list_profiles ()

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    profile : str
        A specific profile configuration to list.  If None, a list of
        profiles available to the given auth token is returned.

    format : str
        Result format: One of 'text' or 'json'

    Returns
    -------
    profiles : list/dict
        A list of the names of the supported profiles or a dictionary of
        the specific profile

    Example
    -------
    .. code-block:: python

        profiles = specClient.list_profiles()
        profiles = specClient.list_profiles(token)
    '''
    return spc_client._list_profiles(token=def_token(token),
                                     profile=profile, fmt=fmt)


# --------------------------------------------------------------------
# LIST_CONTEXTS -- List the available dataset contexts.
#
@multimethod('spc',1,False)
def list_contexts(optval, token=None, contexts=None, fmt='text'):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return spc_client._list_contexts(token=def_token(optval),
                                         contexts=contexts, fmt=fmt)
    else:
        # optval looks like a contexts name
        return spc_client._list_contexts(token=def_token(token),
                                         contexts=optval, fmt=fmt)

@multimethod('spc',0,False)
def list_contexts(token=None, profile=None, fmt='text'):
    '''Retrieve the contexts supported by the spectro data service.

    Usage:
        list_contexts (token=None, contexts=None, fmt='text')

    MultiMethod Usage:  
    ------------------
            specClient.list_contexts (token)
            specClient.list_contexts ()

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    contexts : str
        A specific contexts configuration to list.  If None, a list of
        contexts available to the given auth token is returned.

    format : str
        Result format: One of 'text' or 'json'

    Returns
    -------
    contexts : list/dict
        A list of the names of the supported contexts or a dictionary of
        the specific contexts

    Example
    -------
    .. code-block:: python

        contexts = specClient.list_contexts()
        contexts = specClient.list_contexts(token)
    '''
    return spc_client._list_contexts(token=def_token(token),
                                     context=context, fmt=fmt)



#######################################
# Spectroscopic Data Client Methods
#######################################

# --------------------------------------------------------------------
# QUERY -- Query for spectra by position.
#
@multimethod('spc',3,False)
def query(ra, dec, size, constraint=None, out=None,
          context=None, profile=None, **kw):
    return spc_client._query(ra=ra, dec=dec, size=size,
                             pos=None,
                             region=None,
                             constraint=constraint,
                             out=out,
                             context=context, profile=profile, **kw)

@multimethod('spc',2,False)
def query(pos, size, constraint=None, out=None,
          context=None, profile=None, **kw):
    return spc_client._query(ra=None, dec=None, size=size,
                             pos=pos,
                             region=None,
                             constraint=constraint,
                             out=out,
                             context=context, profile=profile, **kw)

@multimethod('spc',1,False)
def query(region, constraint=None, out=None,
          context=None, profile=None, **kw):
    '''Query for a list of spectrum IDs that can then be retrieved from
        the service.

    Usage:
        id_list = query(ra, dec, size, constraint=None, out=None,
                        context=None, profile=None, **kw)
        id_list = query(pos, size, constraint=None, out=None,
                        context=None, profile=None, **kw)
        id_list = query(region, constraint=None, out=None,
                        context=None, profile=None, **kw)

    Parameters
    ----------
    ra : float
        RA search center specified in degrees.

    dec : float
        Dec of search center specified in degrees.

    size : float
        Size of search center specified in degrees.

    pos : Astropy Coord object
        Coordinate of search center specified as an Astropy Coord object.

    region : float
        Array of polygon vertices (in degrees) defining a search region.

    constraint : str
        A valid SQL syntax that can be used as a WHERE constraint in the 
        search query.

    context : str 
        Dataset context.

    profile : str
        Data service profile.

    **kw : dict
        Optional keyword arguments.  Supported keywords currently include:

        For context='sdss_dr16' | 'default':
           fields:
               specobjid           # or 'bestobjid', etc
               tuple               # a plate/mjd/fiber tuple

               Service will always return array of 'specobjid'
               value, the p/m/f tuple is extracted from the
               bitmask value by the client.

           primary:
               True                # query sdss_dr16.specobj
               False               # query sdss_dr16.specobjall
           catalog:
               <schema>.<table>    # alternative catalog to query e.g. a 
                                   # VAC from earlier DR (must support an
                                   # ra/dec search and return a specobjid-
                                   # like value)
        For all contexts:
           verbose = False
               Print verbose messages during retrieval
           debug = False
               Print debug messages during retrieval

    Returns
    -------
    result : array
        An array of spectrum IDs appropriate for the dataset context.

    Example
    -------
       1) Query by position:

        .. code-block:: python
            id_list = spec.query (0.125, 12.123, 0.1)
    '''
    return spc_client._query(ra=None, dec=None, size=None,
                             pos=None,
                             region=region,
                             constraint=constraint,
                             out=out,
                             context=context, profile=profile, **kw)


# --------------------------------------------------------------------
# GETSPEC -- Retrieve spectra for a list of objects.
#
def getSpec(id_list, fmt='numpy', out=None, align=False, cutout=None,
            context=None, profile=None, **kw):
    '''Get spectra for a list of object IDs.

    Usage:
        getSpec(id_list, fmt='numpy', out=None, align=False, cutout=None,
                context=None, profile=None, **kw)

    Parameters
    ----------
    id_list : list object 
        List of object identifiers.

    fmt : str 
        Return format of spectra

    out : 
        Return format of spectra

    align : 
        Return format of spectra

    cutout : 
        Return format of spectra

    context : str
        Dataset context.

    profile : str
        Data service profile.

    **kw : dict
        Optional keyword arguments.  Supported keywords currently include:

           verbose = False
               Print verbose messages during retrieval
           debug = False
               Print debug messages during retrieval

    Returns
    -------
    result : object or array of objects or 'OK' string

    Example
    -------
       1) Retrieve spectra individually:

        .. code-block:: python
            id_list = spec.query (0.125, 12.123, 0.1)
            for id in id_list:
                spec = spec.getSpec (id)
                .... do something

        2) Retrieve spectra in bulk:

        .. code-block:: python
            spec = spec.getSpec (id_list, fmt='numpy')
            .... 'spec' is an array of NumPy objects that may be
                 different sizes
    '''
    return spc_client.getSpec(id_list=id_list, fmt=fmt, out=out, align=align,
                cutout=cutout, context=context, profile=profile, **kw)


# --------------------------------------------------------------------
# PLOT -- Utility to batch plot a single spectrum, display plot directly.
#
def plot(spec, context=None, profile=None, out=None, **kw):
    '''Utility to batch plot a single spectrum.

    Usage:
        spec.plot(id, context=None, profile=None, **kw)

    Parameters
    ----------
    spec : object ID or data array
        Spectrum to be plotted.  If 'spec' is a numpy array or a 
        Spectrum1D object the data are plotted directly, otherwise
        the value is assumed to be an object ID that will be retrieved
        from the service.

    out : str 
        Output filename.  If specified, plot saved as PNG.

    context : str
        Dataset context.

    profile : str
        Data service profile.

    **kw : dict
        Optional keyword arguments.  Supported keywords currently include:

           sky = False
               Overplot sky spectrum (if available)?
           model = False
               Overplot model spectrum (if available)?
           lines = <dict>
               Dictionary of spectral lines to mark.

    Returns
    -------
        Nothing

    Example
    -------
       1) Plot a single spectrum, save to a virtual storage file

        .. code-block:: python
            spec.plot (specID, context='sdss_dr16', out='vos://spec.png')

    '''
    return spc_client.plot(spec, context=context, profile=profile,
                           out=None, **kw)


# --------------------------------------------------------------------
# PROSPECT -- Utility wrapper to launch the interactive PROSPECT tool.
#
def prospect(spec, context=None, profile=None, **kw):
    '''Utility wrapper to launch the interactive PROSPECT tool.

    Usage:
        stat =  prospect(spec, context=None, profile=None, **kw)

    Parameters
    ----------
    spec : object ID or data array
        Spectrum to be plotted.  If 'spec' is a numpy array or a 
        Spectrum1D object the data are plotted directly, otherwise
        the value is assumed to be an object ID that will be retrieved
        from the service.

    context : str 
        Dataset context.

    profile : str 
        Data service profile.

    **kw : dict
        Optional keyword arguments.  Supported keywords currently include:

           TBD

    Returns
    -------
    result : str
        Status 'OK' string or error message.

    Example
    -------
       1) Plot ....

        .. code-block:: python
            stat = spec.prospect (specID)

    '''
    pass


# --------------------------------------------------------------------
# PREVIEW -- Get a preview plot of a spectrum
#
def preview(spec, context=None, profile=None, **kw):
    '''Get a preview plot of a spectrum

    Usage:
        spec.preview(spec, context=None, profile=None, **kw):

    Parameters
    ----------
    id_list : list object 
        List of object identifiers.

    context : str 
        Dataset context.

    profile : str 
        Data service profile.

    **kw : dict
        Optional keyword arguments.  Supported keywords currently include:

           N/A

    Returns
    -------
    image : A PNG image object

    Example
    -------
       1) Display a preview plot a given spectrum:

        .. code-block:: python
            from IPython.display import display, Image
            display(Image(spec.preview(id),
                    format='png', width=400, height=100, unconfined=True))
    '''
    pass
    return spc_client.preview(spec, context=context, profile=profile, **kw)


# --------------------------------------------------------------------
# PLOTGRID -- Get a grid of preview plots of a spectrum list.
#
def plotGrid(id_list, nx, ny, page=0, context=None, profile=None, **kw):
    '''Get a grid of preview plots of a spectrum list.

    Usage:
        image = plotGrid(id_list, nx, ny, page=0,
                         context=None, profile=None, **kw):

    Parameters
    ----------
    id_list : list object 
        List of object identifiers.

    nx : int 
        Number of plots in the X dimension

    ny : int 
        Number of plots in the Y dimension

    page : int 
        Dataset context.

    context : str 
        Dataset context.

    profile : str 
        Data service profile.

    **kw : dict
        Optional keyword arguments.  Supported keywords currently include:

           verbose = False
               Print verbose messages during retrieval
           debug = False
               Print debug messages during retrieval

    Returns
    -------
    image : A PNG image object

    Example
    -------
       1) Display a 5x5 grid of preview plots for a list:

        .. code-block:: python
            npages = np.round((len(id_list) / 25) + (25 / len(id_list))
            for pg in range(npages):
                data = spec.getGridPlot(id_list, 5, 5, page=pg)
                display(Image(data, format='png',
                        width=400, height=100, unconfined=True))
    '''
    return spc_client.plotGrid(id_list, nx, ny, page=page,
                 context=context, profile=profile, **kw)


# --------------------------------------------------------------------
# STACKEDIMAGE -- Get a stacked image of a list of spectra.
#
def stackedImage(id_list, fmt='png', align=False, yflip=False,
                 context=None, profile=None, **kw):
    '''Get ...

    Usage:

    Parameters
    ----------
    id_list : list object 
        Secure token obtained via :func:`authClient.login()`

    context : str 
        Dataset context.

    profile : str 
        Data service profile.

    **kw : dict
        Optional keyword arguments.  Supported keywords currently include:

           verbose = False
               Print verbose messages during retrieval
           debug = False
               Print debug messages during retrieval

    Returns
    -------
    result : ....

    Example
    -------
       1) Query ....

        .. code-block:: python
            id_list = spec.query (0.125, 12.123, 0.1)

    '''
    pass
    return spc_client.stackedImage(id_list, fmt=fmt, align=align, yflip=yflip,
                 context=context, profile=profile, **kw)



#######################################
# Spectroscopic Data Client Class
#######################################

class specClient(object):
    '''
         SPECCLIENT -- Client-side methods to access the Data Lab
                       Spectroscopic Data Service.
    '''

    def __init__(self, context='default', profile='default'):
        '''Initialize the specClient class.
        '''
        self.svc_url = DEF_SERVICE_URL          # service URL
        self.qm_svc_url = QM_SERVICE_URL        # Query Manager service URL
        self.sm_svc_url = SM_SERVICE_URL        # Storage Manager service URL
        self.svc_profile = profile              # service profile
        self.svc_context = context              # dataset context
        self.auth_token = None                  # default auth token

        self.hostip = THIS_IP
        self.hostname = THIS_HOST

        self.debug = DEBUG                      # interface debug flag


    # Standard Data Lab service methods.
    #
    def set_svc_url(self, svc_url):
        '''Set the URL of the Spectroscopic Data Service to be used.

        Parameters
        ----------
        svc_url : str
            Spectroscopic Data service base URL to call.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import specClient
            specClient.set_svc_url("http://localhost:7001/")
        '''
        self.svc_url = spcToString(svc_url.strip('/'))

    def get_svc_url(self):
        '''Return the currently-used Spectroscopic Data Service URL.

        Parameters
        ----------
        None

        Returns
        -------
        service_url : str
            The currently-used Spectroscopic Data Service URL.

        Example
        -------
        .. code-block:: python

            from dl import specClient
            service_url = specClient.get_svc_url()
        '''
        return spcToString(self.svc_url)

    def set_profile(self, profile):
        '''Set the requested service profile.

        Parameters
        ----------
        profile : str
            Requested service profile string.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import specClient
            profile = specClient.client.set_profile("dev")
        '''
        self.svc_profile = spcToString(profile)

    def get_profile(self):
        '''Get the requested service profile.

        Parameters
        ----------
        None

        Returns
        -------
        profile : str
            The currently requested service profile.

        Example
        -------
        .. code-block:: python

            from dl import specClient
            profile = specClient.client.get_profile()
        '''
        return spcToString(self.svc_profile)

    def set_context(self, context):
        '''Set the requested dataset context.

        Parameters
        ----------
        context : str
            Requested dataset context string.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import specClient
            context = specClient.client.set_context("dev")
        '''
        self.svc_context = spcToString(context)

    def get_context(self):
        '''Get the requested dataset context.

        Parameters
        ----------
        None

        Returns
        -------
        context : str
            The currently requested dataset context.

        Example
        -------
        .. code-block:: python

            from dl import specClient
            context = specClient.client.get_context()
        '''
        return spcToString(self.svc_context)

    def isAlive(self, svc_url=DEF_SERVICE_URL):
        '''Check whether the service at the given URL is alive and responding.
           This is a simple call to the root service URL or ping() method.

        Parameters
        ----------
        service_url : str
            The Query Service URL to ping.

        Returns
        -------
        result : bool
            True if service responds properly, False otherwise

        Example
        -------
        .. code-block:: python

            from dl import specClient
            if specClient.isAlive():
                print("Spec Server is alive")
        '''
        url = svc_url
        try:
            r = requests.get(url, timeout=2)
            resp = r.text

            if r.status_code != 200:
                return False
            elif resp is not None and r.text.lower()[:11] != "hello world":
                return False
        except Exception:
            return False

        return True


    ###################################################
    #  UTILITY METHODS
    ###################################################

    @multimethod('_spc',1,True)
    def list_profiles(self, optval, token=None, profile=None, fmt='text'):
        '''Usage:  specClient.client.list_profiles (token, ...)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._list_profiles (token=def_token(optval),
                                        profile=profile, fmt=format)
        else:
            # optval looks like a token
            return self._list_profiles (token=def_token(token), profile=optval,
                                        fmt=fmt)

    @multimethod('_spc',0,True)
    def list_profiles(self, token=None, profile=None, fmt='text'):
        '''Usage:  specClient.client.list_profiles (...)
        '''
        return self._list_profiles(token=def_token(token), profile=profile,
                             fmt=fmt)

    def _list_profiles(self, token=None, profile=None, fmt='text'):
        '''Implementation of the list_profiles() method.
        '''
        headers = self.getHeaders (token)

        dburl = '%s/profiles?' % self.svc_url
        if profile != None and profile != 'None' and profile != '':
            dburl += "profile=%s&" % profile
        dburl += "format=%s" % fmt

        r = requests.get (dburl, headers=headers)
        profiles = spcToString(r.content)
        if '{' in profiles:
            profiles = json.loads(profiles)

        return spcToString(profiles)



    @multimethod('_spc',1,True)
    def list_contexts(self, optval, token=None, profile=None, fmt='text'):
        '''Usage:  specClient.client.list_contexts (token, ...)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._list_contexts (token=def_token(optval),
                                        profile=profile, fmt=fmt)
        else:
            # optval looks like a token
            return self._list_contexts (token=def_token(token), profile=optval,
                                        fmt=fmt)

    @multimethod('_spc',0,True)
    def list_contexts(self, token=None, profile=None, fmt='text'):
        '''Usage:  specClient.client.list_contexts (...)
        '''
        return self._list_contexts(token=def_token(token), profile=profile,
                             format=format)

    def _list_contexts(self, token=None, profile=None, format='text'):
        '''Implementation of the list_contexts() method.
        '''
        headers = self.getHeaders (token)

        dburl = '%s/contexts?' % self.svc_url
        if profile != None and profile != 'None' and profile != '':
            dburl += "profile=%s&" % profile
        dburl += "format=%s" % format

        r = requests.get (dburl, headers=headers)
        contexts = spcToString(r.content)
        if '{' in contexts:
            contexts = json.loads(contexts)

        return spcToString(contexts)



    ###################################################
    #  SERVICE METHODS
    ###################################################

    # --------------------------------------------------------------------
    # QUERY -- Query for spectra by position.
    #
    @multimethod('_spc',3,True)
    def query(self, ra, dec, size, constraint=None, out=None,
              context=None, profile=None, **kw):
        return self._query(ra=ra, dec=dec, size=size,
                           pos=None,
                           region=None,
                           constraint=constraint,
                           out=out,
                           context=context, profile=profile, **kw)
    
    @multimethod('_spc',2,True)
    def query(self, pos, size, constraint=None, out=None,
              context=None, profile=None, **kw):
        return self._query(ra=None, dec=None, size=None,
                           pos=pos,
                           region=None,
                           constraint=constraint,
                           out=out,
                           context=context, profile=profile, **kw)
    
    @multimethod('_spc',1,True)
    def query(self, region, constraint=None, out=None,
              context=None, profile=None, **kw):
        '''Query for a list of spectrum IDs that can then be retrieved from
            the service.
    
        Usage:
            id_list = query(ra, dec, size, constraint=None, out=None,
                            context=None, profile=None, **kw)
            id_list = query(pos, size, constraint=None, out=None,
                            context=None, profile=None, **kw)
            id_list = query(region, constraint=None, out=None,
                            context=None, profile=None, **kw)
    
        Parameters
        ----------
        ra : float
            RA search center specified in degrees.
    
        dec : float
            Dec of search center specified in degrees.
    
        size : float
            Size of search center specified in degrees.
    
        pos : Astropy Coord object
            Coordinate of search center specified as an Astropy Coord object.
    
        region : float
            Array of polygon vertices (in degrees) defining a search region.
    
        out : str
            Save query results to output filename.  May be a 'vos://' URI or
            local filename.  If set to an empty string, the ID list is
            returned as an ascii string.
    
        constraint : str
            A valid SQL syntax that can be used as a WHERE constraint in the 
            search query.
    
        context : str 
            Dataset context.
    
        profile : str
            Data service profile.
    
        **kw : dict
            Optional keyword arguments.  Supported keywords currently include:
    
            For context='sdss_dr16' | 'default':
               fields:
                   specobjid           # or 'bestobjid', etc
                   tuple               # a plate/mjd/fiber/run2d tuple

                   Service will always return array of 'specobjid'
                   value, the p/m/f tuple is extracted from the
                   bitmask value by the client.

               primary:
                   True                # query sdss_dr16.specobj
                   False               # query sdss_dr16.specobjall
               catalog:
                   <schema>.<table>    # alternative catalog to query e.g. a 
                                       # VAC from earlier DR (must support an
                                       # ra/dec search and return a specobjid-
                                       # like value)
            For all contexts:
               timeout = 600           # Query timeout
               token = None            # User auth token
               verbose = False         # Print verbose output
               debug = False           # Print debug messages
    
        Returns
        -------
        result : array
            An array of spectrum IDs appropriate for the dataset context.
    
        Example
        -------
           1) Query by position:
    
            .. code-block:: python
                id_list = spec.query (0.125, 12.123, 0.1)
        '''
        return self._query(ra=None, dec=None, size=None,
                           pos=None,
                           region=region,
                           constraint=constraint,
                           out=out,
                           context=context, profile=profile, **kw)


    def _query(self,
               ra=None, dec=None, size=None,
               pos=None,
               region=None,
               constraint=None, out=None,
               context=None, profile=None, **kw):
        '''Implementation of the query() method.
        '''

        if context in [None, '']: context = self.svc_context
        if profile in [None, '']: profile = self.svc_profile

        # Process optional keyword arguments.
        if context == 'default' or context.startswith('sdss'):
            fields = kw['fields'] if 'fields' in kw else 'specobjid'
            if fields == 'tuple':
                #fields = 'plate,mjd,fiberid,run2d'
                raise dlSpecError('Error: "tuple" not yet supported')
            if fields.find(',') > 0:
                raise dlSpecError('Error: multiple fields not yet supported')
            catalog = kw['catalog'] if 'catalog' in kw else 'sdss_dr16.specobj'
            primary = kw['primary'] if 'primary' in kw else True
            if not primary and catalog == 'sdss_dr16_specobj':
                catalog = 'sdss_dr16.specobjall'
        else:
            fields = kw['fields'] if 'fields' in kw else 'specobjid'
            catalog = kw['catalog'] if 'catalog' in kw else 'sdss_dr16.specobj'

        timeout = kw['timeout'] if 'timeout' in kw else 600
        token = kw['token'] if 'token' in kw else self.auth_token
        verbose = kw['verbose'] if 'verbose' in kw else False
        debug = kw['debug'] if 'debug' in kw else False

        # Set service call headers.
        headers = {'Content-Type': 'text/ascii',
                   'X-DL-TimeoutRequest': str(timeout),
                   'X-DL-ClientVersion': __version__,
                   'X-DL-OriginIP': self.hostip,
                   'X-DL-OriginHost': self.hostname,
                   'X-DL-AuthToken': def_token(None)}  # application/x-sql

        # Build the query URL string.
        base_url = '%s/query?' % self.qm_svc_url

        _size = size
        if region is not None:
            pquery = "q3c_poly_query(ra,dec,ARRAY%s)" % region
        elif pos is not None:
            pquery = "q3c_radial_query(ra,dec,%f,%f,%f)" % \
                         (pos.ra.degree, pos.dec.degree, _size)
        else:
            pquery = "q3c_radial_query(ra,dec,%f,%f,%f)" % (ra, dec, _size)

        # Create the query string for the IDs.
        sql = 'SELECT %s FROM %s WHERE %s' % (fields, catalog, pquery)

        # Add any user-defined constraint.
        if constraint not in [None, '']:
            sql += ' AND %s' % constraint

        # Query for the IDs.
        if debug:
            print ('SQL = ' + sql)
        res = queryClient.query (sql=sql, fmt='csv').split('\n')[1:-1]
        if debug:
            print ('res = ' + str(res))

        id_list = np.array(res, dtype=np.uint64)
        if out in [None, '']:
            print ('out=None  type list = ' + str(type(id_list)))
            #print ('out=None  type list list = ' + str(type(list(id_list))))
            return id_list
        else:
            # Note:  memory expensive for large lists .....
            csv_rows = ["{}".format(i) for i in id_list]
            csv_text = "\n".join(csv_rows)
            if out == '':
                return csv_text
            elif out.startswith('vos://'):
                return storeClient.saveAs(csv_text, out)[0]
            else:
                with open(out, "w") as fd:
                    fd.write(csv_text)
                    fd.write('\n')
                return 'OK'
    
    
    # --------------------------------------------------------------------
    # GETSPEC -- Retrieve spectra for a list of objects.
    #
    def getSpec(self, id_list, fmt='numpy', out=None, align=False,
                cutout=None, context=None, profile=None, **kw):
        '''Get spectra for a list of object IDs.
    
        Usage:
            getSpec(id_list, fmt='numpy', out=None, align=False, cutout=None,
                    context=None, profile=None, **kw)
    
        Parameters
        ----------
        id_list : list object 
            List of object identifiers.
    
        format : 
            Return format of spectra
    
        out : 
            Return format of spectra
    
        align : 
            Return format of spectra
    
        cutout : 
            Return format of spectra
    
        context : str
            Dataset context.
    
        profile : str
            Data service profile.
    
        **kw : dict
            Optional keyword arguments.  Supported keywords currently include:
    
               verbose = False
                   Print verbose messages during retrieval
               debug = False
                   Print debug messages during retrieval
    
        Returns
        -------
        result : object or array of objects or 'OK' string
    
        Example
        -------
           1) Retrieve spectra individually:
    
            .. code-block:: python
                id_list = spec.query (0.125, 12.123, 0.1)
                for id in id_list:
                    spec = spec.getSpec (id)
                    .... do something
    
            2) Retrieve spectra in bulk:
    
            .. code-block:: python
                spec = spec.getSpec (id_list, fmt='numpy')
                .... 'spec' is an array of NumPy objects that may be
                     different sizes
        '''

        if context in [None, '']: context = self.svc_context
        if profile in [None, '']: profile = self.svc_profile

        # Process optional parameters.
        align = kw['align'] if 'align' in kw else False
        cutout = kw['cutout'] if 'cutout' in kw else self.auth_token
        token = kw['token'] if 'token' in kw else self.auth_token
        verbose = kw['verbose'] if 'verbose' in kw else False
        debug = kw['debug'] if 'debug' in kw else False

        # Set service call headers.
        headers = {'Content-Type' : 'application/x-www-form-urlencoded',
                   'X-DL-ClientVersion': __version__,
                   'X-DL-OriginIP': self.hostip,
                   'X-DL-OriginHost': self.hostname,
                   'X-DL-AuthToken': def_token(None)}  # application/x-sql

        print ('in ty sid_list: ' + str(type(id_list)))
        if not (isinstance(id_list, list) or isinstance(id_list, np.ndarray)):
            id_list = np.array([id_list])
        print ('out ty sid_list: ' + str(type(id_list)))

        # Initialize the payload.
        url = '%s/getSpec' % self.svc_url
        data = {'id_list' : list(id_list),
                'bands' : 'all',
                'format' : fmt,
                'align' : align,
                'cutout' : cutout,
                'w0' : 0.0,
                'w1' : 0.0,
                'context' : context,
                'profile' : profile,
                'debug' : debug,
                'verbose' : verbose
              }

        # Get the limits of the collection
        url = '%s/listSpan' % self.svc_url
        resp = requests.post(url, data=data, headers=headers)
        v = json.loads(resp.text)
        data['w0'], data['w1'] = v['w0'], v['w1']

        if align:
            # If we're aligning columns, the server will pad the values
            # and return a common array size.
            resp = requests.post (url, data=data, headers=headers)
            _data = resp.content
        else:
            # If not aligning columns, request each spectrum individually
            # so we can return a list object.
            _data = []
            for id in id_list:
                print (id)
                data['id_list'] = list(np.array([id]))
                resp = requests.post (url, data=data, headers=headers)
                _data.append(resp.content)
            _data = np.array(_data)

        return _data

        if fmt.lower() == 'FITS':
            return _data
        else:
            np_data = np.load(BytesIO(_data), allow_pickle=False)
            if fmt.lower() == 'numpy':
                return np_data
            elif fmt == 'pandas':
                return pd.DataFrame (data=np_data, columns=np_data.dtype.names)
            elif fmt == 'Spectrum1D':
                # FIXME: column names are SDSS-specific
                lamb = 10**np_data['loglam'] * u.AA 
                flux = np_data['flux'] * 10**-17 * u.Unit('erg cm-2 s-1 AA-1')
                spec1d = Spectrum1D(spectral_axis=lamb, flux=flux)
                spec1d.meta['sky'] = np_data['sky']
                spec1d.meta['model'] = np_data['model']
                spec1d.meta['ivar'] = np_data['ivar']
                return spec1d


    # --------------------------------------------------------------------
    # PLOT -- Utility to batch plot a single spectrum, display plot directly.
    #
    def plot(self, spec, context=None, profile=None, out=None, **kw):
        '''Utility to batch plot a single spectrum.
    
        Usage:
            spec.plot(id, context=None, profile=None, **kw)
    
        Parameters
        ----------
        spec : object ID or data array
            Spectrum to be plotted.  If 'spec' is a numpy array or a 
            Spectrum1D object the data are plotted directly, otherwise
            the value is assumed to be an object ID that will be retrieved
            from the service.
    
        out : str 
            Output filename.  If specified, plot saved as PNG.
    
        context : str
            Dataset context.
    
        profile : str
            Data service profile.
    
        **kw : dict
            Optional keyword arguments.  Supported keywords currently include:

                rest_frame - Whether or not to plot the spectra in the
                             rest-frame  (def: True)
                         z - Redshift value
                      xlim - Setting the xrange of the plot
                      ylim - Setting the yrange of the plot
    
                     bands - A comma-delimited string of which bands to plot,
                             a combination of 'flux,model,sky,ivar'
                mark_lines - Which lines to mark.  No lines marked if None or
                             an empty string, otherwise one of 'em|abs|all|both'
                      grid - Plot grid lines (def: True)
                      dark - Dark-mode plot colors (def: True)
                  em_lines - List of emission lines to plot.  If not given,
                             all the lines in the default list will be plotted.
                 abs_lines - Lines of absorption lines to plot.  If not given,
                             all the lines in the default list will be plotted.
                 spec_args - Plotting kwargs for the spectrum
                model_args - Plotting kwargs for the model
                 ivar_args - Plotting kwargs for the ivar
                  sky_args - Plotting kwargs for the sky
    
        Returns
        -------
            Nothing
    
        Example
        -------
           1) Plot a single spectrum, save to a virtual storage file
    
            .. code-block:: python
                spec.plot (specID, context='sdss_dr16', out='vos://spec.png')
        '''

        if context in [None, '']: context = self.svc_context
        if profile in [None, '']: profile = self.svc_profile

        # See whether we've been passed a spectrum ID or a data.
        if isinstance(spec, int) or \
           isinstance(spec, tuple) or \
           isinstance(spec, str):
               data = spc_client.getSpec(spec, context=context, profile=profile)
               wavelength = 10.0**data['loglam']
               flux = data['flux']
               model = data['model']
               sky = data['sky']
               ivar = data['ivar']
        else:
            if isinstance(spec, np.ndarray) or \
               isinstance(spec, pd.core.frame.DataFrame):
                   wavelength = 10.0**spec['loglam']
                   flux = spec['flux']
                   model = spec['model']
                   sky = spec['sky']
                   ivar = spec['ivar']
            elif isinstance (spec, Spectrum1D):
                wavelength = spec.spectral_axis#.value
                flux = spec.flux#.value
                model = spec.meta['model']*10**-17 * u.Unit('erg cm-2 s-1 AA-1')
                sky = spec.meta['sky']*10**-17 * u.Unit('erg cm-2 s-1 AA-1')
                ivar = spec.meta['ivar']

        self._plotSpec(wavelength, flux, model=model, sky=sky, ivar=ivar, **kw)
    
    
    # --------------------------------------------------------------------
    # PROSPECT -- Utility wrapper to launch the interactive PROSPECT tool.
    #
    def prospect(self, spec, context=None, profile=None, **kw):
        '''Utility wrapper to launch the interactive PROSPECT tool.
    
        Usage:
            stat =  prospect(spec, context=None, profile=None, **kw)
    
        Parameters
        ----------
        spec : object ID or data array
            Spectrum to be plotted.  If 'spec' is a numpy array or a 
            Spectrum1D object the data are plotted directly, otherwise
            the value is assumed to be an object ID that will be retrieved
            from the service.
    
        context : str 
            Dataset context.
    
        profile : str 
            Data service profile.
    
        **kw : dict
            Optional keyword arguments.  Supported keywords currently include:
    
               TBD
    
        Returns
        -------
        result : str
            Status 'OK' string or error message.
    
        Example
        -------
           1) Plot ....
    
            .. code-block:: python
                stat = spec.prospect (specID)
        '''

        if context in [None, '']: context = self.svc_context
        if profile in [None, '']: profile = self.svc_profile

        pass
    
    
    # --------------------------------------------------------------------
    # PREVIEW -- Get a preview plot of a spectrum
    #
    def preview(self, spec, context=None, profile=None, **kw):
        '''Get a preview plot of a spectrum
    
        Usage:
            spec.preview(spec, context=None, profile=None, **kw):
    
        Parameters
        ----------
        spec : objectID 
            Object identifiers.
    
        context : str 
            Dataset context.
    
        profile : str 
            Data service profile.
    
        **kw : dict
            Optional keyword arguments.  Supported keywords currently include:
    
               N/A
    
        Returns
        -------
        image : A PNG image object
    
        Example
        -------
           1) Display a preview plot a given spectrum:
    
            .. code-block:: python
                from IPython.display import display, Image
                display(Image(spec.preview(id),
                        format='png', width=400, height=100, unconfined=True))
        '''

        if context in [None, '']: context = self.svc_context
        if profile in [None, '']: profile = self.svc_profile

        url = self.svc_url + '/preview?id=%s' % str(spec)
        url = url + '&context=%s&profile=%s' % (context, profile)
        try:
            if USE_CURL:
                return self.curl_get(url)
            else:
                return requests.get(url, timeout=2).content
        except Exception as e:
            raise Exception("Error getting plot data: " + str(e))

    
    
    # --------------------------------------------------------------------
    # PLOTGRID -- Get a grid of preview plots of a spectrum list.
    #
    def plotGrid(self, id_list, nx, ny, page=0,
                 context=None, profile=None, **kw):
        '''Get a grid of preview plots of a spectrum list.
    
        Usage:
            image = plotGrid(id_list, nx, ny, page=0,
                             context=None, profile=None, **kw):
    
        Parameters
        ----------
        id_list : list object 
            List of object identifiers.
    
        nx : int 
            Number of plots in the X dimension
    
        ny : int 
            Number of plots in the Y dimension
    
        page : int 
            Dataset context.
    
        context : str 
            Dataset context.
    
        profile : str 
            Data service profile.
    
        **kw : dict
            Optional keyword arguments.  Supported keywords currently include:
    
               verbose = False
                   Print verbose messages during retrieval
               debug = False
                   Print debug messages during retrieval
        Returns
        -------
        image : A PNG image object
    
        Example
        -------
           1) Display a 5x5 grid of preview plots for a list:
    
            .. code-block:: python
                npages = np.round((len(id_list) / 25) + (25 / len(id_list))
                for pg in range(npages):
                    data = spec.getGridPlot(id_list, 5, 5, page=pg)
                    display(Image(data, format='png',
                            width=400, height=100, unconfined=True))
        '''

        if context in [None, '']: context = self.svc_context
        if profile in [None, '']: profile = self.svc_profile

        # Process optional parameters.
        token = kw['token'] if 'token' in kw else self.auth_token
        verbose = kw['verbose'] if 'verbose' in kw else False
        debug = kw['debug'] if 'debug' in kw else False

        # Set service call headers.
        headers = {'Content-Type' : 'application/x-www-form-urlencoded',
                   'X-DL-ClientVersion': __version__,
                   'X-DL-OriginIP': self.hostip,
                   'X-DL-OriginHost': self.hostname,
                   'X-DL-AuthToken': def_token(None)}  # application/x-sql

        # Build the query URL string.
        url = '%s/plotGrid' % self.svc_url

        if isinstance(id_list, list) or isinstance(id_list, np.ndarray):
            n_ids = len(id_list)
            sz_grid = nx * ny
            if sz_grid >= n_ids:         # Use the whole list.
                ids = id_list
                p_start = 0
                p_end = len(id_list) - 1
            else:
                p_start = page * sz_grid
                p_end = min(n_ids, p_start + sz_grid)
                ids = id_list[p_start:p_end]
        else:
            ids = id_list

        # Initialize the payload.
        data = {'id_list' : ids,
                'ncols' : ny,
                'context' : context,
                'profile' : profile,
                'debug' : debug,
                'verbose' : verbose
              }

        resp = requests.post (url, data=data, headers=headers)
        return BytesIO(resp.content)


    # --------------------------------------------------------------------
    # STACKEDIMAGE -- Get a stacked image of a list of spectra.
    #
    def stackedImage(self, id_list, fmt='png', align=False, yflip=False,
                     context=None, profile=None, **kw):
        '''Get ...
    
        Usage:
    
        Parameters
        ----------
        id_list : list object 
            Secure token obtained via :func:`authClient.login()`
    
        context : str 
            Dataset context.
    
        profile : str 
            Data service profile.
    
        **kw : dict
            Optional keyword arguments.  Supported keywords currently include:
    
               verbose = False
                   Print verbose messages during retrieval
               debug = False
                   Print debug messages during retrieval
        Returns
        -------
        result : ....
    
        Example
        -------
           1) Query ....
    
            .. code-block:: python
                id_list = spec.query (0.125, 12.123, 0.1)
        '''

        if context in [None, '']: context = self.svc_context
        if profile in [None, '']: profile = self.svc_profile

        # Process optional parameters.
        scale = kw['scale'] if 'scale' in kw else (1.0,1.0)
        if isinstance(scale,float):
            xscale = yscale = scale
        else:
            xscale = scale[0]
            yscale = scale[1]
        thickness = kw['thickness'] if 'thickness' in kw else 1
        inverse = kw['inverse'] if 'inverse' in kw else False
        cmap = kw['cmap'] if 'cmap' in kw else 'gray'
        width = kw['width'] if 'width' in kw else 0
        height = kw['height'] if 'height' in kw else 0
        token = kw['token'] if 'token' in kw else self.auth_token
        verbose = kw['verbose'] if 'verbose' in kw else False
        debug = kw['debug'] if 'debug' in kw else False

        # Set service call headers.
        headers = {'Content-Type' : 'application/x-www-form-urlencoded',
                   'X-DL-ClientVersion': __version__,
                   'X-DL-OriginIP': self.hostip,
                   'X-DL-OriginHost': self.hostname,
                   'X-DL-AuthToken': def_token(None)}  # application/x-sql

        # Build the query URL string.
        url = '%s/stackedImage' % self.svc_url

        # Initialize the payload.
        data = {'id_list' : list(id_list),
                'context' : context,
                'xscale' : xscale,
                'yscale' : yscale,
                'thickness' : thickness,
                'cmap' : cmap,
                'inverse' : inverse,
                'width' : width,
                'height' : height,
                'profile' : profile,
                'debug' : debug,
                'verbose' : verbose
              }

        resp = requests.post (url, data=data, headers=headers)
        return BytesIO(resp.content)


    ###################################################
    #  STATIC UTILITY METHODS
    ###################################################

    # --------------------------------------------------------------------
    # _PLOTSPEC -- Plot a spectrum.
    #
    @staticmethod
    def _plotSpec(wavelength, flux, model=None, sky=None, ivar=None,
                  rest_frame = True, z=0.0, xlim = None, ylim = None,
                  title=None, out=None, **kw):
        """Plot a spectrum.
        
        Inputs:
            * spec = 
            * rest_frame - Whether or not to plot the spectra in the
                           rest-frame.  (def: True)
            * z - Redshift
            * xlim - Setting the xrange of the plot
            * ylim - Setting the yrange of the plot
    
        Optional kwargs:
            * bands - A comma-delimited string of which bands to plot, a
                      combination of 'flux,model,sky,ivar'
            * mark_lines - Which lines to mark.  No lines marked if None or
                           an empty string, otherwise one of 'em|abs|all|both'
            * grid - Plot grid lines (def: True)
            * dark - Dark-mode plot colors (def: True)
            * em_lines - List of emission lines to plot.  If not given, all
                         the lines in the default list will be plotted.
            * abs_lines - Lines of absorption lines to plot.  If not given,
                          all the lines in the default list will be plotted.
            * spec_args - Plotting kwargs for the spectrum.
            * model_args - Plotting kwargs for the model.
            * ivar_args - Plotting kwargs for the ivar.
            * sky_args - Plotting kwargs for the sky.
        """
        #from astropy.visualization import astropy_mpl_style
        #plt.style.use(astropy_mpl_style)
    
        def labelLines (lines, ax, color, yloc):
            '''This is for selecting only those lines that are visible in
               the x-range of the plot.
            '''
            for ii in range(len(lines)):
                # If rest_frame=False, shift lines to the observed frame.
                lam = lines[ii]['lambda']
                if (rest_frame == False):
                    lam = lam * (1+z)
                # Plot only lines within the x-range of the plot.
                if ((lam > xbounds[0]) & (lam < xbounds[1])):
                    ax.axvline(lam, color=color, lw=1.0, linestyle=':')
                    ax.annotate(lines[ii]['label'], xy=(lam, yloc),
                                xycoords=ax.get_xaxis_transform(),
                                fontsize=12, rotation=90, color=color)
    
        # Process the optional kwargs.
        dark = kw['dark'] if 'dark' in kw else True
        grid = kw['grid'] if 'grid' in kw else True
        mark_lines = kw['mark_lines'] if 'mark_lines' in kw else 'all'
        em_lines = kw['em_lines'] if 'em_lines' in kw else None
        abs_lines = kw['abs_lines'] if 'abs_lines' in kw else None
        bands = kw['bands'] if 'bands' in kw else 'flux,model'
    
        if 'spec_args' in kw:
            spec_args = kw['spec_args']
        else:
            spec_args = {'color': '#ababab', 'linewidth' : 1.0, 'alpha': 1.0}
        if 'model_args' in kw:
            model_args = kw['model_args']
        else:
            model_args = {'color': 'red', 'linewidth': 1.2}
        if 'sky_args' in kw:
            sky_args = kw['sky_args']
        else:
            sky_args = {'color': 'brown', 'linewidth': 1.0}
        if 'ivar_args' in kw:
            ivar_args = kw['ivar_args']
        else:
            ivar_args = {'color': 'blue', 'linewidth': 1.0}
    
    
        # Setting up the plot
        if dark:
            fig = plt.figure(dpi=100, figsize = (12,5), facecolor='#2F4F4F')
            plt.rcParams['axes.facecolor']='#121212'
        else:
            fig = plt.figure(dpi=100, figsize = (12,5))
    
        ax = fig.add_subplot(111)
        if 'flux' in bands:
            ax.plot(wavelength, flux*(ivar > 0), label='Flux', **spec_args)
        if 'model' in bands and model is not None:
            ax.plot(wavelength, model*(ivar > 0), label='Model', **model_args)
        if 'sky' in bands and sky is not None:
            ax.plot(wavelength, sky*(ivar > 0), label='Sky', **sky_args)
        if 'ivar' in bands and ivar is not None:
            ax.plot(wavelength, ivar*(ivar > 0), label='Ivar', **ivar_args)
    
        plt.xlim(xlim)
        plt.ylim(ylim)
        am_color = ('y' if dark else 'black')
        if rest_frame:
            plt.xlabel('Rest Wavelength ($\AA$)', color=am_color)
        else:
            plt.xlabel('Observed Wavelength ($\AA$)    z=%.3g' % z,
                       color=am_color)
        plt.ylabel('$F_{\lambda}$ ($10^{-17}~ergs~s^{-1}~cm^{-2}~\AA^{-1}$)',
                  color=am_color)
    
        if dark: ax.tick_params(color='w', labelcolor='w')
        if grid: plt.grid(color='gray', linestyle='dashdot', linewidth=0.5)
    
        if title not in [None, '']:
            ax.set_title(title + '\n', c=am_color)
        
        # Plotting Absorption/Emission lines - only works if either of the
        # lines is set to True
        if mark_lines not in [None, '']:
            if mark_lines == 'all' or mark_lines == 'both':
                opt = ['em','abs']
            else:
                opt = mark_lines.lower().split(',')
    
            # Select any lines listed by the user.
            e_lines = _em_lines
            if (em_lines != None):
                e_lines = list(filter(lambda x: x['name'] in em_lines,
                               _em_lines))
            a_lines = _abs_lines 
            if (abs_lines != None):
                a_lines = list(filter(lambda x: x['name'] in abs_lines,
                               _abs_lines))
            xbounds = ax.get_xbound()   # Getting the x-range of the plot 
    
            lcol = ['#FFFF00', '#00FFFF'] if dark else ['#FF0000', '#0000FF']
            if 'em' in opt: labelLines (e_lines, ax, lcol[0], 0.875)
            if 'abs' in opt: labelLines (a_lines, ax, lcol[1], 0.05)
        
        leg = ax.legend()
        if dark:
            for text in leg.get_texts():
                plt.setp(text, color = 'w')

        if out is not None:
            plt.savefig(out)
        else:
            plt.show()
    
    

    ###################################################
    #  PRIVATE UTILITY METHODS
    ###################################################

    def debug(self, debug_val):
        '''Toggle debug flag.
        '''
        self.debug = debug_val

    def retBoolValue(self, url):
        '''Utility method to call a boolean service at the given URL.
        '''
        response = ""
        try:
            # Add the auth token to the reauest header.
            if self.auth_token != None:
                headers = {'X-DL-AuthToken': self.auth_token}
                r = requests.get(url, headers=headers)
            else:
                r = requests.get(url)
            response = spcToString(r.content)

            if r.status_code != 200:
                raise Exception(r.content)

        except Exception:
            return spcToString(r.content)
        else:
            return response

    def getHeaders(self, token):
        '''Get default tracking headers.
        '''
        tok = def_token(token)
        user, uid, gid, hash = tok.strip().split('.', 3)
        hdrs = {'Content-Type': 'text/ascii',
                'X-DL-ClientVersion': __version__,
                'X-DL-OriginIP': self.hostip,
                'X-DL-OriginHost': self.hostname,
                'X-DL-User': user,
                'X-DL-AuthToken': tok}                  # application/x-sql
        return hdrs

    def getFromURL(self, svc_url, path, token):
        '''Get something from a URL.  Return a 'response' object.
        '''
        try:
            hdrs = self.getHeaders(token)
            resp = requests.get("%s%s" % (svc_url, path), headers=hdrs)

        except Exception as e:
            raise dlSpecError(str(e))
        return resp

    def curl_get(self, url):
        '''Utility routine to use cURL to return a URL
        '''
        b_obj = BytesIO()
        crl = pycurl.Curl()
        crl.setopt(crl.URL, url)
        crl.setopt(crl.WRITEDATA, b_obj)
        crl.perform()
        crl.close()
        return b_obj.getvalue()



# ###################################
#  Spectroscopic Data Client Handles
# ###################################

def getClient(context='default', profile='default'):
    '''Get a new instance of the specClient client.

    Parameters
    ----------
    context : str
        Dataset context

    profile : str
        Service profile

    Returns
    -------
    client : specClient
        An specClient object

    Example
    -------
    .. code-block:: python
        new_client = specClient.getClient()
    '''
    return specClient(context=context, profile=profile)




# Get the default client object.
spc_client = getClient(context='default', profile='default')


# ##########################################
#  Patch the docstrings for module functions
# ##########################################

set_svc_url.__doc__ = spc_client.set_svc_url.__doc__
get_svc_url.__doc__ = spc_client.get_svc_url.__doc__
set_profile.__doc__ = spc_client.set_profile.__doc__
get_profile.__doc__ = spc_client.get_profile.__doc__
set_context.__doc__ = spc_client.set_context.__doc__
get_context.__doc__ = spc_client.get_context.__doc__


# ####################################################################
#  Py2/Py3 Compatability Utilities
# ####################################################################

def spcToString(s):
    '''spcToString -- Force a return value to be type 'string' for all
                      Python versions.
    '''
    if is_py3:
        if isinstance(s,bytes):
            strval = str(s.decode())
        elif isinstance(s,str):
            strval = s
    else:
        if isinstance(s,bytes) or isinstance(s,unicode):
            strval = str(s)
        else:
            strval = s

    return strval

