import specClient as spec
import numpy as np
import time
import sys

spec.set_svc_url('http://gp07.datalab.noao.edu:6998/spec')


# Utility routine to print information about a spectrum object.
def info(data):
    try:
        print(' Type: ' + str(type(data)))
        print('  Len: ' + str(len(data)))
        print('Shape: ' + str(data.shape))
        if isinstance(data,list) or isinstance(data,np.ndarray):
            print ('Element Type: ' + str(type(data[0])))
    except Exception as e:
        print('ERROR: ' + str(e))
        pass
import sys

            
id_int64 = 2210146812474530816                          # specobjid
id_tup1  = (1963,54331,120)                             # tuple identifier
id_tup2  = (1963,54331,121)                             # tuple identifier
id_tup3  = (1963,54331,122)                             # tuple identifier
id_tupa  = (1963,54331)                                 # plate/mjd tuple
id_tupb  = (1963,54331,'*','103')                       # fiber wildcard
id_tupc  = (1963,54331,'*')                             # fiber wildcard
id_tupd  = (1963,'*')                                   # plate/mjd tuple
id_tupe  = (1963,'*',100)                               # plate/mjd tuple
id_tupf  = ('*',54331)                                  # plate/mjd tuple
id_tupg  = ('*',54331,100)                              # plate/mjd tuple
id_tuph  = ('1962,1963','*')                            # plate list tuple
id_tupi  = ('*','53321,54331')                          # plate/mjd tuple
id_tupj  = ('*','53321,54331',100)                      # plate/mjd tuple
id_tupk  = ('1962,1963','*',100,'26,103')               # plate list tuple
id_tupl  = ([1962,1963],[54331,53321])                  # plate list tuple

test_ids =  [ id_int64,                                 # single int64
             [id_int64],                                # single int64 array
              id_tup1,                                  # single tuple
             [id_tup1],                                 # single tuple array
             [id_tup1, id_tup2],                        # array of tuples
             [id_tup1, id_int64],                       # mixed int64/tuple
             [id_int64, id_tup1],                       # mixed int64/tuple
             [id_tup1, id_int64, id_tup2],              #   "      "
             [id_tup1, id_int64, id_tup2, id_int64],    #   "      "
             [id_tupa],                                 # small tuple
             [id_tupl],                                 # small tuple
             [id_tupb,],                                # small tuple
             [id_tupc],
             [id_tupd],
             [id_tupe],
             [id_tupf],
             [id_tupg],
             [id_tuph],
             [id_tupi],
             [id_tupj],
             [id_tupk],
           ]

debug = True

for i, id in enumerate(test_ids):
    print('\n=============================================')
    for align in [True,False]:
        st_time = time.time()
        print('TEST (%d/%d) ID:  %s' % (i+1,len(test_ids),str(id)))
        print('ALIGN = %5s ==============================\n' % align)
        try:
            print('id type = ' + str(type(id)))
            data = spec.getSpec(id, fmt='numpy', align=align, debug=debug)
        except Exception as e:
            print('ERROR: %s' % str(e))
            sys.exit(0)
        else:
            en_time = time.time()
            info(data)
            print('OK\t    NSpec: %4d  Time: %.6g sec' % \
                  ((len(data) if align else len(data[0])),(en_time-st_time)))
        print('============================================\n\n')



