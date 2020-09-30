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


#--------------------------------------
# Get a single spectrum by spectrum ID
#
#print('Get by ID...')
#data = spec.getSpec(2210146812474530816, fmt='numpy', foo=True, debug=True, align=True)
#info(data)
#spec.plot(data[0])
#data = spec.getSpec(2210146812474530816, fmt='pandas')
#info(data)
#data = spec.getSpec(2210146812474530816, fmt='Spectrum1D')
#info(data)

# Plot a downloaded spectrum
_s = time.time()
id_list = spec.query(30.0, 1.0, 63.0, 
                     ontext='sdss',out='',
                     constraint="run2d='103' limit 64")
_e = time.time()
print('Query Time:  %g' % (_e - _s))
info(id_list)


#--------------------------------------
# Get list by numpy array
#
print('Get list by numpy array ...')
_s = time.time()
data = spec.getSpec(id_list, fmt='numpy', align=True)
_e = time.time()
info(data)
print('Raw Time:  %g' % (_e - _s))
for i in range(3):                      # Plot the first 4 spectra
    spec.plot(data[i])

np.save('./zcube.npy', data, allow_pickle=False)


_s = time.time()
data = spec.getSpec(id_list[:32], fmt='numpy', align=False)
_e = time.time()
info(data)
print('Align Time:  %g' % (_e - _s))
for i in range(3):                      # Plot the first 4 spectra
    spec.plot(data[i])

np.save('./zlist.npy', data, allow_pickle=False)

sys.exit(0)


print('plot Spectrum1D array ...')
data = spec.getSpec(id_list[0], fmt='Spectrum1D')


print('plot pandas array ...')
data = spec.getSpec(id_list[0], fmt='pandas')
spec.plot(data)


