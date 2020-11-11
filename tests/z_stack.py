#!/usr/bin/env python

import specClient as spec
from PIL import Image
from io import BytesIO
import time

from dl import queryClient as qc

sql = """select specobjid 
         from sdss_dr16.specobj 
         where run2d = '103' and z > 0.02 and class = 'QSO'
             order by z 
             limit 25600"""
print('start query')
_s1 = time.time()
ids = qc.query(sql=sql).split('\n')[1:-1]
_e1 = time.time()
print('end query')

id = []
for p in ids:
    id.append(int(p))


print('start stack')
_s2 = time.time()
data = spec.stackedImage(id, scale=(0.25,0.25), thickness=3, inverse=False,
                         cmap='summer')
_e2 = time.time()
print('end stack')

print ('query time: %g   stack time: %g   nspec=%d' % ((_e1-_s1),(_e2-_s2),len(ids)))
image = Image.open(data)
image.show()



