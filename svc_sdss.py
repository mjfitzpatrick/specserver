#!/usr/bin/env python

#  SVC_SDSS -- Class definition file for the SDSS spectral data.

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = 'v1.0.0'


import os
import glob
import re
import time
#import logging
import numpy as np
from svc_base import Service
from astropy.table import Table
from astropy.io import ascii
from io import BytesIO
from io import StringIO

from dl import queryClient as qc


# Primary object identifier
sdss_id_main = 'specobjid'

# Default RUN2D values for various SDSS data releases.
sdss_run2d = {'dr16': [26, 103, 104, 'v5_13_0'],
              'dr15': [26, 103, 104, 'v5_10_0'],
              'dr14': [26, 103, 104, 'v5_10_0'],
              'dr13': [26, 103, 104, 'v5_9_0'],
              'dr12': [26, 103, 104, 'v5_7_0', 'v5_7_2'],
              'dr11': [26, 103, 104, 'v5_6_5'],
              'dr10': [26, 103, 104, 'v5_5_12'],
              'dr9': [26, 103, 104, 'v5_4_45'],
              'dr8': [26, 103, 104]
             }

DEF_QUERY_PROFILE = 'db01'


# SDSS data service sub-class.
class sdssService(Service):
    '''SDSS data service sub-class.
    '''

    def __init__(self, release='dr16'):
        self.release = release                            # SDSS data release
        self.fits_root = '/net/mss1/archive/hlsp/sdss/'   # Root to FITS data
        self.cache_root = '/ssd0/sdss/'                   # Root to cached data
        self.data_root = '%s/sdss/spectro/redux/' % release

        self.run2d = sdss_run2d[release]
        self.query_profile = DEF_QUERY_PROFILE

        self.debug = False
        self.verbose = False

    # ----------------
    # SubClass Methods
    # ----------------
    def query(self, id, fields, catalog, cond):
        '''Return a CSV string of query results on the dataset.  If an 'id'
           is supplied we query directly against the value, otherwise we can
           use an arbitrary 'cond' in the WHERE clause.
        '''

        if fields in [None, 'None', '']:
            fields = sdss_id_main

        if id not in [None, 'None', '']:
            _where = id
            qstring = 'SELECT %s FROM %s WHERE %s = %s' % \
                      (fields, catalog, sdss_id_main, \
                      toSigned(np.uint64(id), 64))
            print(qstring)
            res = qc.query(sql=qstring, fmt='table')
        else:
            _where = '' if cond.strip()[:5].lower() in ['order', 'limit'] else 'WHERE'
            if sdss_id_main in fields:
                qstring = 'SELECT %s FROM %s %s %s' % \
                          (fields, catalog, _where, cond)
            else:
                qstring = 'SELECT %s,%s FROM %s %s %s' % \
                          (sdss_id_main, fields, catalog, _where, cond)

            # Query the table and force the object ID to be an unsigned int.
            res = qc.query(sql=qstring, fmt='table')
            res[sdss_id_main].dtype = np.uint64

        # Return result as CSV
        ret = StringIO()
        ascii.write(res, ret, format='csv')
        retval = ret.getvalue()

        return retval


    def dataPath(self, id, fmt='npy'):
        '''Get the path to the SDSS spectrum data file.
        '''
        if fmt.lower() == 'fits':       # 'fmt' can be a client format
            return self._idToPath(id, 'fits')
        else:
            return self._idToPath(id, 'npy')


    def previewPath(self, id):
        '''Get the path to the SDSS spectrum preview file.
        '''
        return self._idToPath(id, 'png')


    def getData(self, fname):
        '''Return the data in the named file as a numpy array.
        '''
        if fname[-3:] == 'npy':
            return np.load(str(fname))
        elif fname[-4:] == 'fits':
            data = Table.read(fname, hdu=1).as_array()
            retval = BytesIO()
            np.save(retval, data, allow_pickle=False)
            return np.load(BytesIO(retval.getvalue()), allow_pickle=False)
        else:
            raise Exception('getdata(): Unknown file extension')


    def expandIDList(self, id_list):
        '''Expand the input (string) identifier list to an array of valid
           SDSS identifiers.  For plate/mjd/fiber/run2d tuples we also expand
           wildcards.
        '''
        st_time = time.time()

        # Remove the array brackets from the string and strip any internal
        # (non-quoted) whitespace.  The result is an array of strings we
        # can map to identifiers.
        if self.debug:
            print('ID_LIST in: :' + str(id_list) + ':')
        id_str = id_list[1:-1].strip()  if id_list[0] == '[' else id_list
        if id_str.find('(') >= 0:
            id_str = id_str.replace(' ', '')
            id_str = id_str.replace(',(', ' (')
            id_str = id_str.replace("),", ") ")
            id_str = id_str.replace(",(", " (")
        else:
            id_str = id_str.replace('\n', ' ')
            id_str = id_str.replace('  ', ' ')
            id_str = id_str.replace(' ', ',')
            id_str = id_str.replace(' ', '')
            id_str = id_str.replace(',,', ',')

        if "(" in id_str:
            split_char = ' '
        elif "'" in id_str:
            split_char = ','
            id_str = id_str.replace("'", '')
        else:
            split_char = ','
        id_str = id_str.split(split_char)
        if self.debug:
            print('ID_STR: ' + str(id_str))

        if all(x.isdigit() for x in id_str):
            # All identifiers are integers.
            ids = np.uint64(id_str)
        elif any(x.startswith('(') for x in id_str):
            # At least some identifiers are tuple strings.
            ids = []
            for s in id_str:
                if s[0] == '(':
                    v = eval(s)
                    if len(v) == 1:
                        _ids = self._expandID(v[0], '*', '*', '*')
                    elif len(v) == 2:
                        _ids = self._expandID(v[0], v[1], '*', '*')
                    elif len(v) == 3:
                        _ids = self._expandID(v[0], v[1], v[2], '*')
                    else:
                        _ids = self._expandID(v[0], v[1], v[2], v[3])

                    ids = ids + _ids
                elif s.isdigit():
                    ids.append(np.uint64(s))
        else:
            raise Exception('Unknown identifier values')

        if self.debug:
            print('EXPAND ids = ' + str(ids)[:128])
            print('EXPAND len(ids) = ' + str(len(ids)))
            print('EXPAND time: ' + str(time.time() - st_time))
        return ids


    # ----------------
    # Utility Methods
    # ----------------

    def _findFile(self, plate, mjd, fiber, extn):
        '''Find a file given a plate/mjd/fiber tuple.
        '''
        st_time = time.time()
        base_path = self.fits_root if extn == 'fits' else self.cache_root
        for r in self.run2d:
            spath = base_path + \
                  '%s/sdss/spectro/redux/%s/spectra/%04i/spec-%04i-%05i-%04i.%s' % \
                  (self.release, str(r), plate, plate, mjd, fiber, extn)
            if os.path.exists(spath):
                if self.debug and self.verbose:
                    print('_findFile() time0: ' + str(time.time()-st_time))
                return(spath)

        # FALLTHRU
        spath = base_path + \
                  '%s/*/spectro/redux/*/spectra/full/%04i/spec-%04i-%05i-%04i.%s' % \
                  (self.release, plate, plate, mjd, fiber, extn)
        files = glob.glob(spath)
        for f in files:
            if os.path.exists(f):
                if self.debug and self.verbose:
                    print('_findFile() time1: ' + str(time.time()-st_time))
                return(f)

        if self.debug and self.verbose:
            print('_findFile() time2: ' + str(time.time()-st_time))
        return None


    def _buildPath(self, plate, mjd, fiber, run2d, survey, extn):
        '''Build a pathname for the ID.
        '''
        base_path = self.fits_root if extn == 'fits' else self.cache_root
        base_path = base_path + \
                        '%s/%s/spectro/redux/' % (self.release, survey)
        path = base_path + ('%s/spectra/%04i/' % (run2d, plate))
        fname = path + 'spec-%04i-%05i-%04i.%s' %  (plate, mjd, fiber, extn)
        return fname


    def _expandID(self, plate, mjd, fiber, run2d):
        '''Expand wildcards in a tuple identifier.
        '''
        # PLATE may be an int or list
        if isinstance(plate, int):
            wp = 'plate = %d' % plate
        elif plate == '*':
            wp = None
        elif isinstance(plate,list):
            wp = 'plate in %s' % str(plate).replace('[','(').replace(']',')')
        elif isinstance(plate,str) and ',' in plate:
            plist = list(map(int, plate.split(',')))
            wp = 'plate in %s' % str(plist).replace('[','(').replace(']',')')
        else:
            wp = 'plate = %d' % int(plate)

        # MJD may be an int or list
        if isinstance(mjd, int):
            wm = 'mjd = %d' % mjd
        elif mjd == '*':
            wm = None
        elif isinstance(mjd,list):
            wm = 'mjd in %s' % str(mjd).replace('[','(').replace(']',')')
        elif isinstance(mjd,str) and ',' in mjd:
            mlist = list(map(int, mjd.split(',')))
            wm = 'mjd in %s' % str(mlist).replace('[','(').replace(']',')')
        else:
            wm = 'mjd = %d' % int(mjd)

        # Fiber may be an int, range or list
        if isinstance(fiber, int):
            wf = 'fiberid = %d' % fiber
        elif isinstance(fiber,list):
            wf = 'fiberid in %s' % str(fiber).replace('[','(').replace(']',')')
        elif fiber.find('-') > 0 or fiber.find(':') > 0:
            split_char = '-' if fiber.find('-') > 0 else ':'
            st = int(fiber.split(split_char)[0])
            en = int(fiber.split(split_char)[1])
            wf = 'fiberid between %d and %d' % (st, en)
        elif fiber.find(',') > 0:
            flist = list(map(int, fiber.split(',')))
            wf = 'fiberid in %s' % str(flist).replace('[','(').replace(']',')')
        elif fiber.isdigit():
            wf = 'fiberid = %s' % fiber
        else:
            wf = None

        # RUN2D may be a str or list
        if run2d is None or run2d == '*':
            wr = None
        elif isinstance(run2d, list):
            wr = 'run2d in %s' % str(run2d).replace('[', '(').replace(']', ')')
        elif run2d.find(',') > 0:
            rlist = list(map(str, run2d.split(',')))
            wa = str(rlist).replace('[', '(').replace(']', ')')
            wr = "run2d in %s" % wa
        else:
            wr = "run2d = '%s'" % run2d

        if all(w is None for w in [wp, wm, wf, wr]):
            raise Exception('At least one of plate or mjd must be specified')
        else:
            w = '' if wp is None else wp
            if wm is not None:
                w = w  + ('' if w == '' else ' AND ') + wm
            if wf is not None:
                w = w  + ('' if w == '' else ' AND ') + wf
            if wr is not None:
                w = w  + ('' if w == '' else ' AND ') + wr

        # DR16 will have the needed information, we cannot count on earlier
        # releases having an available 'specobj' table.
        query = '''SELECT plate,mjd,fiberid,run2d,survey
                   FROM sdss_dr16.specobj WHERE %s''' % w

        res = qc.query(sql=query, profile=self.query_profile)
        rows = res.split('\n')[1:-1]
        ids = []
        for row in rows:
            p, m, f, r, s = row.split(',')
            s = 'sdss' if s.startswith('segue') else s.lower()
            ids.append(tuple([int(p), int(m), int(f), r, s]))

        if self.debug:
            print('return ids = :%s:' % ids)
        return(ids)


    def _idToPath(self, id, extn):
        '''Get the path to a SDSS spectrum data file with the named extension.
        '''
        st_time = time.time()
        survey = 'sdss'			# default survey name
        if isinstance(id, str) or isinstance(id, np.unicode):
            if id[0] == '(':
                id = id.astype(np.uint64)
            else:
                id = int(id)
        if isinstance(id, int) or isinstance(id, np.uint64):
            # The ID is a 'specobjid' object
            u = unpack_specobjid(np.array([id], dtype=np.uint64))
            plate = u.plate[0]
            mjd = u.mjd[0]
            fiber = u.fiber[0]
            run2d = u.run2d[0]
        elif isinstance(id, tuple):
            # The ID is a '(plate,mjd,fiber[,run2d])' tuple object
            if len(id) >= 3:
                 plate, mjd, fiber = id[0], id[1], id[2]
                 run2d = id[3] if len(id) == 4 else ''
                 survey = id[4] if len(id) == 5 else 'sdss'
                 if len(id) == 5:
                     survey = id[4].lower()
                     if survey.startswith('segue'):
                         survey = 'sdss'
        else:
            raise Exception('Unknown identifier: ' + str(id))

        # Strip a leading '.' from the extension name if present.
        if extn.startswith('.'):
            extn = extn[1:]

        if run2d == '':
            # If we don't have an explicit RUN2D value, search for a
            # plate/mjd/fiber combo in the given context.
            fname = self._findFile(plate, mjd, fiber, extn)
        else:
            # Otherwise construct the path to the file.
            fname = self._buildPath(plate, mjd, fiber, run2d, survey, extn)
            if not os.path.exists(fname):
                # Not found in the current context, look around....
                fname = self._findFile(plate, mjd, fiber, extn)

        # Lastly, try to find a FITS file before giving up.
        if fname is None:
            fname = self._findFile(plate, mjd, fiber, 'fits')
            if fname is None:
                raise Exception('File not found: ')

        if self.debug and self.verbose:
            print('_idToPath() time: ' + str(time.time()-st_time))
        return fname


#############################################
# Utility Methods
#############################################

def toSigned(number, bitLength):
    '''Convert an unsigned number of a given bitlength to the signed value.
    '''
    mask = (2 ** bitLength) - 1
    if number & (1 << (bitLength - 1)):
        return number | ~mask
    else:
        return number & mask


def pack_specobjid(plate, mjd, fiber, run2d):
    '''Convert SDSS spectrum identifiers into CAS-style specObjID.

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
    '''

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
        cnv = {'103': 103, '104': 104, '26': 26,
               'v5_10_0': 1000, 'v5_13_0': 1300
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

    if isinstance(specObjID, np.uint64) or isinstance(specObjID, int):
        tempobjid = np.array([specObjID], dtype=np.uint64)
    elif specObjID.dtype.type in [np.string_, np.unicode_]:
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
    if run2d == 0:
        R = np.array('')
    else:
        N = ((run2d // 10000) + 5).tolist()
        M = ((run2d % 10000) // 100).tolist()
        P = (run2d % 100).tolist()
        R = np.array(
            ['v{0:d}_{1:d}_{2:d}'.format(n, m, p) for n, m, p in zip(N, M, P)])

        # Fix the integer version conversion.
        R = np.where(R == 'v5_1_3', '103', R)
        R = np.where(R == 'v5_1_4', '104', R)
        R = np.where(R == 'v5_0_26', '26', R)
    unpack.run2d = R

    return unpack
