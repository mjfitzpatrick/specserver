import specClient as spec
import time

spec.set_svc_url('http://gp07.datalab.noao.edu:6998/spec')


# Utility routine to print information about a spectrum object.
def info(data):
    try:
        print(' Type: ' + str(type(data)))
        print('  Len: ' + str(len(data)))
        if isinstance(data,list) or isinstance(data,np.ndarray):
            print ('Element Type: ' + str(type(data[0])))
        print('Shape: ' + str(data.shape))
    except Exception as e:
        pass
import sys

            
id_int64 = 2210146812474530816                          # specobjid
id_tup1  = (1963,54331,120)                             # tuple identifier
id_tup2  = (1963,54331,121)                             # tuple identifier
id_tup3  = (1963,54331,122)                             # tuple identifier
id_tup2a = (1963,54331)                                 # plate/mjd tuple
id_tup2b = (1963,54331,'*','103')                       # fiber wildcard
id_tup2c = (1963,54331,'*')                             # fiber wildcard
id_tup2d = (1963,'*')                                   # plate/mjd tuple
id_tup2e = (1963,'*',100)                               # plate/mjd tuple
id_tup2f = ('*',54331)                                  # plate/mjd tuple
id_tup2g = ('*',54331,100)                              # plate/mjd tuple
id_tup2h = ('1962,1963','*')                            # plate list tuple
id_tup2i = ('*','53321,54331')                          # plate/mjd tuple
id_tup2j = ('*','53321,54331',100)                      # plate/mjd tuple
id_tup2k = ('1962,1963','*',100,'26,103')               # plate list tuple

test_ids =  [ id_int64,                                 # single int64
             [id_int64],                                # single int64 array
              id_tup1,                                  # single tuple
             [id_tup1],                                 # single tuple array
             [id_tup1, id_tup2],                        # array of tuples
             [id_tup1, id_int64],                       # mixed int64/tuple
             [id_int64, id_tup1],                       # mixed int64/tuple
             [id_tup1, id_int64, id_tup2],              #   "      "
             [id_tup1, id_int64, id_tup2, id_int64],    #   "      "
             [id_tup2a],                                # small tuple
             [id_tup2b,],                               # small tuple
             [id_tup2c],
             [id_tup2d],
             [id_tup2e],
             [id_tup2f],
             [id_tup2g],
             [id_tup2h],
             [id_tup2i],
             [id_tup2j],
             [id_tup2k],
           ]
test_ids =  [ id_tup1,                                  # single tuple
           ]

debug = True

spec.set_context('sdss_dr14')

for i, id in enumerate(test_ids):
  print('\n=============================================')
  for context in ['sdss_dr16','sdss_dr13']:
    print('\n=============================================')
    print('CONTEXT = %9s =========================\n' % context)
    for align in [False,True]:
        st_time = time.time()
        print('TEST (%d/%d) ID:  %s' % (i+1,len(test_ids),str(id)))
        print('ALIGN = %5s ==============================\n' % align)
        try:
            print('id type = ' + str(type(id)))
            data = spec.getSpec(id, fmt='numpy', align=align, 
                                context=context, debug=debug)
        except Exception as e:
            print('ERROR: %s' % str(e))
            sys.exit(0)
        else:
            en_time = time.time()
            info(data)
            print('OK\t    NSpec: %4d  Time: %.6g sec' % \
                  ((len(data) if align else len(data[0])),(en_time-st_time)))
        print('============================================\n\n')



