#!/usr/bin/env python

import specClient as spec


# Plot by spectrum ID
print('plot by ID...')
spec.plot(2210146812474530816, bands='flux,model')
#spec.plot(4565636637219987456, bands='flux,model', grid=False)


# Plot a downloaded spectrum
id = spec.query(30.0, 1.0, 0.01, context='sdss',out='')

print('plot Spectrum1D array ...')
data = spec.getSpec(id[0], fmt='Spectrum1D')
spec.plot(data)

print('plot numpy array ...')
data = spec.getSpec(id[0])
spec.plot(data)

print('plot pandas array ...')
data = spec.getSpec(id[0], fmt='pandas')
spec.plot(data)


#_plotSpec(spec, 
#     xlim=(4500,6123),
#     #ylim=(1.0,3.0),
#     bands='flux,model',
#     dark=True,
#     grid=True,
#     title='Test Title',
#     rest_frame=False, z=0.3141,
#     mark_lines='all')
#
