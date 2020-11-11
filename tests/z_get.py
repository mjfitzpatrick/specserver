#!/usr/bin/env python

import sys
import time
import specClient as spec
import numpy as np


def info(data):
    try:
        print(' Type: ' + str(type(data)))
        print('  Len: ' + str(len(data)))
        print('Shape: ' + str(data.shape))
        print('================================')
    except Exception as e:
        pass


PLOT = True


#--------------------------------------
# Get a single spectrum by spectrum ID
#
print('Get by ID...')
data = spec.getSpec(2210146812474530816, fmt='numpy')
info(data)
if PLOT: spec.plot(data[0])
print('--------------------------------------')

data = spec.getSpec(2210146812474530816, fmt='pandas')
info(data)
if PLOT: spec.plot(data[0])
print('--------------------------------------')

data = spec.getSpec(2210146812474530816, fmt='Spectrum1D')
info(data)
if PLOT: spec.plot(data[0])
print('--------------------------------------')

# Plot a downloaded spectrum
_s = time.time()
id_list = spec.query(30.0, 1.0, 60.0, 
                     ontext='sdss',out='', constraint="run2d='103' limit 10")
_e = time.time()
print('Query Time:  %g' % (_e - _s))
info(id_list)
print('--------------------------------------')


#--------------------------------------
# Get list by numpy array
#
print('Get list by numpy array ...')
_s = time.time()
data = spec.getSpec(id_list, fmt='numpy', align=True)
_e = time.time()
info(data)
print('Raw Time:  %g' % (_e - _s))
if PLOT:
    for i in range(3):                      # Plot the first 3 spectra
        spec.plot(data[i])
print('--------------------------------------')

_s = time.time()
data = spec.getSpec(id_list, fmt='numpy', align=False)
_e = time.time()
info(data)
print('Align Time:  %g' % (_e - _s))
if PLOT:
    for i in range(3):                      # Plot the first 3 spectra
        spec.plot(data[i])
print('--------------------------------------')


#--------------------------------------
# Get list of Pandas
#
print('Get array of Pandas ...')
_s = time.time()
data = spec.getSpec(id_list, fmt='pandas', align=False)
_e = time.time()
info(data)
print ('ty element: ' + str(type(data[0])))
if PLOT:
    for i in range(3):                      # Plot the first 3 spectra
        spec.plot(data[i])
print('--------------------------------------')


#--------------------------------------
# Get list of Spectrum1D
#
print('Get Spectrum1D array ...')
_s = time.time()
data = spec.getSpec(id_list, fmt='Spectrum1D')
_e = time.time()
info(data)
print ('ty element: ' + str(type(data[0])))
if PLOT:
    for i in range(3):                      # Plot the first 3 spectra
        spec.plot(data[i])
print('--------------------------------------')

