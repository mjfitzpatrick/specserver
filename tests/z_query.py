#!/usr/bin/env python

import specClient as spec

import os
from astropy import units as u
from astropy.coordinates import SkyCoord
from dl import storeClient as sc

DEBUG = False
SIZE = 0.03


#   id_list = query(ra, dec, size,
#                   constraint=None, out=None,
#                   context='default', profile='default', **kw)
#   id_list = query(pos, size,
#                   constraint=None, out=None,
#                   context='default', profile='default', **kw)
#   id_list = query(region,
#                   constraint=None, out=None,
#                   context='default', profile='default', **kw)


# #################################
# Input query position tests
# #################################

print('##############  Positional tests  ##############')

# RA/Dec query
print('######  RA/Dec query')
print(spec.query(30.0, 1.0, SIZE, context='sdss',out='', debug=DEBUG))

# Astropy Coord query
posn = [ SkyCoord(ra=30.0*u.degree, dec=1.0*u.degree, frame='icrs'),
         SkyCoord(ra=30.0, dec=1.0, frame='icrs', unit='deg'),
         SkyCoord('2h0m0.0s +1d0m0s', frame='icrs')
       ]

print('######  Coord queries')
for p in posn:
    print(spec.query(p, SIZE, context='sdss',out='', debug=DEBUG))
    print('-------')


# Region query
print('######  Region query')
region = [29.95,0.95, 30.05,0.95, 30.05,1.05, 29.95,1.05]
print(spec.query(region, context='sdss',out='', debug=DEBUG))


# #################################
# Constraint tests
# #################################

print('##############  Constraint tests  ##############')
print(spec.query(30.0,1.0,0.05,
                 context='sdss',out='',
                 constraint='z > 0.2 order by z',
                 debug=DEBUG))



# #################################
# Output option tests
# #################################

print('##############  Output tests  ##############')
print('######  Output to stdout')
print(spec.query(30.0,1.0,0.05,
                 out='',
                 context='sdss', debug=DEBUG))

print('######  Output to local file')
fname = '/tmp/test.id'

if os.path.exists(fname): 
    os.remove(fname)

print(spec.query(30.0,1.0,0.05,
                 out=fname,
                 context='sdss', debug=DEBUG))

if os.path.exists(fname): 
    print('Local file:  SUCCESS')
    os.remove(fname)
else:
    print('Local file:  FAILED')


print('######  Output to VOS file')
fname = 'vos://test.id'

if sc.access(fname): 
    os.remove(fname)

print(spec.query(30.0,1.0,0.05,
                 out=fname,
                 context='sdss', debug=DEBUG))

if sc.access(fname): 
    print('VOS file:  SUCCESS')
    sc.rm(fname)
else:
    print('VOS file:  FAILED')



