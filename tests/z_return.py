import specClient as spec
import specutils
from specutils import Spectrum1D
import numpy as np
import time
import sys

spec.set_svc_url('http://gp07.datalab.noao.edu:6998/spec')


# Utility routine to print information about a spectrum object.
def info(data):
    try:
        print(' Type: ' + str(type(data)))
        if not isinstance(data,specutils.spectra.spectrum1d.Spectrum1D):
            print('Shape: ' + str(data.shape))
            print('  Len: ' + str(len(data)))
        if isinstance(data,list) or isinstance(data,np.ndarray):
            print ('Element Type: ' + str(type(data[0])))
    except Exception as e:
        print('ERROR: ' + str(e))
        pass
import sys

            
id_int64 = 2210146812474530816                          # specobjid

test_ids =  [ id_int64,                                 # single int64
             [id_int64],                                # single int64 array
             [id_int64, id_int64],                      # int64 array
           ]

debug = False

for i, id in enumerate(test_ids):
    print('\n=============================================')
    #for align in [True,False]:
    for align in [True]:
        st_time = time.time()
        print('TEST (%d/%d) ID:  %s' % (i+1,len(test_ids),str(id)))
        print('ALIGN = %5s ==============================' % align)
        #for fmt in ['numpy','pandas','Spectrum1D']:
        for fmt in ['numpy','pandas']:
            print('\nFMT = %10s ===========================\n' % fmt)
            try:
                print('id type = ' + str(type(id)))
                data = spec.getSpec(id, fmt=fmt, align=align, debug=debug)
            except Exception as e:
                print('ERROR: %s' % str(e))
                sys.exit(0)
            else:
                en_time = time.time()
                if isinstance(data,specutils.spectra.spectrum1d.Spectrum1D):
                  print('OK\t                 Time: %.6g sec' % \
                        (en_time-st_time))
                else:
                  info(data)
                  print('OK\t    NSpec: %4d  Time: %.6g sec' % \
                      ((len(data) if align else len(data[0])),(en_time-st_time)))
        print('============================================\n\n')



