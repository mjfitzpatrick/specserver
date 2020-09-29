#!/usr/bin/env python

#  SVC_SDSS -- Class definition file for the SDSS spectral data.

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = 'v1.0.0'


from svc_base import Service
import numpy as np
import logging

# Base service class.
class sdssService(Service):
    '''Base service class.
    '''

    def __init__(self, release='dr16'):
        self.release = release                  # SDSS data release
        self.fits_root = '/ssd0/'               # Root path of FITS data
        self.cache_root = '/ssd0/'              # Root path of cached data
        self.data_root = 'sdss/%s/sdss/spectro/redux/' % release

    def dataPath (self, id, fmt='npy'):
        '''Get the path to the SDSS spectrum data file.
        '''
        if fmt.lower() == 'fits':       # 'fmt' can be a client format
            return self.idToPath(id, 'fits')
        else:
            return self.idToPath(id, 'npy')

    def previewPath (self, id):
        '''Get the path to the SDSS spectrum preview file.
        '''
        return self.idToPath(id, 'png')


    def idToPath (self, specobjid, extn):
        '''Get the path to a SDSS spectrum data file with the named extension.
        '''
        u = unpack_specobjid(np.array([specobjid],dtype=np.uint64))
        plate = u.plate[0]
        mjd = u.mjd[0]
        fiber = u.fiber[0]
        run2d = u.run2d[0]

        base_path = self.fits_root if extn == 'fits' else self.cache_root
        path = base_path + self.data_root + ('%s/%04i/' % (run2d, plate))
        fname = path + 'spec-%04i-%05i-%04i.%s' %  (plate,mjd,fiber,extn)

        return fname



#############################################
# Utility Methods
#############################################

def pack_specobjid(plate, mjd, fiber, run2d):
    """Convert SDSS spectrum identifiers into CAS-style specObjID.

    Bits are assigned in specObjID thus:

    Bits  Name       Comment
    ===== ========== =========================================================
    50-63 Plate ID   14 bits
    38-49 Fiber ID   12 bits
    24-37 MJD        Date plate was observed minus 50000 (14 bits)
    10-23 run2d      Spectroscopic reduction version
    0-9   line/index 0 for use in SpecObj files (10 bits, not used)

    Parameters
    ----------
    plate, fiber, mjd : :class:`int` or array of int
        Plate, fiber ID, and MJD for a spectrum.  If arrays are
        passed, all must have the same length.  The MJD value must be
        greater than 50000.
    run2d : :class:`int`, :class:`str` or array of int or str
        The run2d value must be an integer or a string of the form 'vN_M_P'.
        If an array is passed, it must have the same length as the other
        inputs listed above.  If the string form is used, the values are
        restricted to :math:`5 \le N \le 6`, :math:`0 \le M \le 99`,
        :math:`0 \le P \le 99`.

    Returns
    -------
    :class:`numpy.ndarray` of :class:`numpy.uint64`
        The specObjIDs of the objects.

    Raises
    ------
    :exc:`ValueError`
        If the sizes of the arrays don't match or if the array values are
        out of bounds.

    Notes
    -----
    * On 32-bit systems, makes sure to explicitly declare all inputs as
      64-bit integers.
    * This function defines the SDSS-III/IV version of specObjID, used for
      SDSS DR8 and subsequent data releases.  It is not compatible with
      SDSS DR7 or earlier.

    Credit
    ------
    * Based on routine from `pydl.pydlutils.sdss`: 
        https://github.com/weaverba137/pydl

    Examples
    --------
    >>> print(pack_specobjid(4055,408,55359,'v5_7_0'))
    [4565636362342690816]
    """

    if isinstance(plate, int):
        plate = np.array([plate], dtype=np.uint64)
    if isinstance(fiber, int):
        fiber = np.array([fiber], dtype=np.uint64)
    if isinstance(mjd, int):
        mjd = np.array([mjd], dtype=np.uint64) - 50000
    else:
        mjd = mjd - 50000
    if isinstance(run2d, str):
        try:
            run2d = np.array([int(run2d)], dtype=np.uint64)
        except ValueError:
            # Try a "vN_M_P" string.
            m = re.match(r'v(\d+)_(\d+)_(\d+)', run2d)
            if m is None:
                raise ValueError("Could not extract integer run2d value!")
            else:
                N, M, P = m.groups()
            run2d = np.array([(int(N) - 5)*10000 + int(M) * 100 + int(P)],
                             dtype=np.uint64)
    elif isinstance(run2d, int):
        run2d = np.array([run2d], dtype=np.uint64)
    else:
        cnv = {'103' : 103, '104' : 104, '26' : 26,
               'v5_10_0' : 1000, 'v5_13_0' : 1300
              }
        R = np.array(run2d)
        for t in list(cnv):
            R = np.where(R == t, cnv[t], R)
        run2d = R.astype(np.uint32)

    # Check that all inputs have the same shape.
    #
    if plate.shape != fiber.shape:
        raise ValueError("fiber.shape does not match plate.shape!")
    if plate.shape != mjd.shape:
        raise ValueError("mjd.shape does not match plate.shape!")
    if plate.shape != run2d.shape:
        raise ValueError("run2d.shape does not match plate.shape!")

    # Compute the specObjID
    #
    plate = np.array(plate, dtype=np.uint64)
    fiber = np.array(fiber, dtype=np.uint64)
    mjd = np.array(mjd, dtype=np.uint64)
    run2d = np.array(run2d, dtype=np.uint64)

    specObjID = ((plate << 50) |
                 (fiber << 38) |
                 (mjd   << 24) |
                 (run2d << 10)).astype(np.uint64)
    return specObjID


def unpack_specobjid(specObjID):
    """Unpack SDSS specObjID into plate, fiber, mjd, run2d.

    Parameters
    ----------
    specObjID : :class:`numpy.ndarray`
        An array containing 64-bit integers or strings.  If strings are passed,
        they will be converted to integers internally.

    Returns
    -------
    :class:`numpy.recarray`
        A record array with the same length as `specObjID`, with the columns
        'plate', 'fiber', 'mjd', 'run2d'.

    Raises
    ------
    :exc:`ValueError`
        If the specObjID data type is not recognized.

    Credit
    ------
    * Based on similar routine from `pydl.pydlutils.sdss`:
        https://github.com/weaverba137/pydl

    Example:
    --------
        >>> unpack_specobjid(array([4565636362342690816], dtype=numpy.uint64))
        rec.array([(4055, 408, 55359, 'v5_7_0', 0)],
              dtype=[('plate', '<i4'), ('fiber', '<i4'),
                     ('mjd', '<i4'), ('run2d', '<U8')])
    """

    if isinstance (specObjID, np.uint64) or isinstance (specObjID, int):
        tempobjid = np.array([ specObjID ], dtype=np.uint64)
    elif specObjID.dtype.type in [ np.string_, np.unicode_ ]:
        tempobjid = specObjID.astype(np.uint64)
    elif specObjID.dtype.type is np.uint64:
        tempobjid = specObjID.copy()
    else:
        raise ValueError('Unrecognized dtype for specObjID!')

    unpack = np.recarray(tempobjid.shape,
                         dtype=[('plate', 'i4'), ('fiber', 'i4'),
                                ('mjd', 'i4'), ('run2d', 'U8')])
    unpack.plate = np.bitwise_and(tempobjid >> 50, 2**14 - 1)
    unpack.fiber = np.bitwise_and(tempobjid >> 38, 2**12 - 1)
    unpack.mjd = np.bitwise_and(tempobjid >> 24, 2**14 - 1) + 50000

    run2d = np.bitwise_and(tempobjid >> 10, 2**14 - 1)
    N = ((run2d // 10000) + 5).tolist()
    M = ((run2d % 10000) // 100).tolist()
    P = (run2d % 100).tolist()
    R = np.array(
            ['v{0:d}_{1:d}_{2:d}'.format(n, m, p) for n, m, p in zip(N, M, P)])

    # Fix the integer version conversion.
    R = np.where (R == 'v5_1_3', '103', R)
    R = np.where (R == 'v5_1_4', '104', R)
    R = np.where (R == 'v5_0_26', '26', R)
    unpack.run2d = R

    return unpack

