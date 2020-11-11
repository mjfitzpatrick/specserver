
import numpy as np
from svc_sdss import pack_specobjid
from svc_sdss import unpack_specobjid



DEBUG = False


def test_sdss ():
    '''
    '''
    id1 = np.array([4565636362342079488, 
                   4565636637219987456, 
                   4566762812004657152, 
                   6439153884405714944], dtype=np.uint64)

    if DEBUG: print('INPUT ID ==========')
    if DEBUG: print(id1)

    u = unpack_specobjid(id1)
    p1,m1,f1,r1 = u.plate, u.mjd, u.fiber, u.run2d
    if DEBUG: print(p1,m1,f1,r1)


    # Repack the values to see if we recover the IDs
    id2 = pack_specobjid(p1, m1, f1, r1)
    if DEBUG: print('REPACKED ID VALUES ==========')
    if DEBUG: print(id2)

    u = unpack_specobjid(id2)
    p2,m2,f2,r2 = u.plate, u.mjd, u.fiber, u.run2d
    if DEBUG: print(p2,m2,f2,r2)

    assert np.array_equal(p1, p2),"Plate values don't match"
    assert np.array_equal(m1, m2),"MJD values don't match"
    assert np.array_equal(f1, f2),"Fiber values don't match"
    assert np.array_equal(r1, r2),"Run2d values don't match"
    assert np.array_equal(id1, id2),"ID values don't match"


def test_specobjid ():

    specobjid = 2210146812474530816
    plate = 1963
    mjd = 54331
    fiber = 19
    run2d = 103

    s = pack_specobjid (plate, mjd, fiber, run2d)
    assert s[0] == specobjid,'Bad specobjid'

    u = unpack_specobjid (specobjid)
    assert u.plate[0] == plate,'Bad plate value'
    assert u.mjd[0] == mjd,'Bad mjd value'
    assert u.fiber[0] == fiber,'Bad fiber value'
    assert u.run2d[0] == str(run2d),'Bad run2d value'


test_sdss()
test_specobjid ()
