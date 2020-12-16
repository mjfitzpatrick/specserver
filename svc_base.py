#!/usr/bin/env python

#  SVC_BASE -- Base Class for the spectral data service.

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = 'v1.0.0'



# Base service class.
class Service(object):
    '''Base service class.
    '''
    def __init__(self, release=None):
        self.release = release
        pass

    def query(self, id, fields, catalog, cond):
        '''Return a CSV string of query results on the dataset.
        '''
        pass

    def dataPath(self, id, fmt=None):
        '''Return the path to the spectrum data file.
        '''
        pass

    def previewPath(self, id):
        '''Return the path to the spectrum preview plot.
        '''
        pass

    def getData(self, fname):
        '''Return the data in the named file as a numpy array.
        '''
        pass

    def expandIDList(self, id_list):
        '''Expand/convert the input id_list we get from the service call as
           a string, to an array of valid identifiers for the dataset.
        '''
        pass

    def readFile(self, fname):
        '''Return the bytes in the named file.
        '''
        with open(fname, 'rb') as fd:
            _bytes = fd.read()
        return _bytes
