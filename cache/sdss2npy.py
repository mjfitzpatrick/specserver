#!/usr/bin/env python
#
#  SDSS2DB.PY -- Convert an SDSS spectrum FITS file in order to extract the
#  coadd spectra to a binary-format file, and insert the tabular specobj
#  and spzline data to postgres tables.
#
#  Usage:
#      % sdss2db [<options>] [<file> | <dir>]
#
#  where <options> include:
#      -h|--help               # This message
#
#      -i <name>               # Input file/dir to process
#      -o <name>               # Output file/dir to create
#      -r|--recursive          # Recurse down directory trees
#      -f|--follow             # Follow symbolic links
#      -m|--mirror             # Mirror directory structure
#
#      -B|--bin_extn <e,e,..>  # Extension names to convert to bin files
#      -D|--db_extns <e,e,..>  # Extension names to load into database
#      -S|--ssa <tbl>          # Load PHU in DB SSA table
#      -T|--tbl_suffix <name>  # Suffix to be added to DB table names
#
#      Database Options
#      -u|--user <user>        # Database user name
#      -p|--passwd <password>  # Database password
#      -h|--host <hostname>    # Database host
#
#  Examples:
#
#    1) Convert a single SDSS spectrum file:
#
#       % sdss2db -i spec.fits -B coadd -D "specobj,spzline"
#       % sdss2db -i spec.fits --bin_extn=coadd --db_extns="specobj,spzline"
#
#       Creates a 'spec.npy' file and DB tables with same name as extension
#
#    2) Recursively convert all FITS files in a directory tree, mirroring that
#       tree structure in a named output directory.  Filename ".fits" extensions
#       are converted to ".npy" automatically, DB tables are appended
#
#       % sdss2db -i /<path>/spectra -o ./spectra --follow --mirror \
#               -B coadd -D "specobj,spzline"
#
#    3) Create an SSA metadata table of spectra from the PHU of an SDSS
#       spectrum file.  The argument to the '-ssa' flag names the table to
#       be created or appended.
#
#       % sdss2db -i /<path>/spectra --folow --mirror --ssa sdss_dr16_ssa
#


from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = '20200401'  # yyyymmdd version


import os
import sys
import time
import re
import numpy as np
import optparse
import psycopg2
import psycopg2.pool
import matplotlib as mpl
            
# Con't user interactive graphics
mpl.use('agg')

from astropy.table import Table
from astropy.io import fits
from contextlib import contextmanager
from matplotlib import pyplot as plt


# Global parameters.
opts = None                     # task options

iname = None                    # input file/dir name
oname = None                    # output file/dir name
ssa_table = None                # SSA metadata table name
mirror = False                  # mirror directory structure
recurse = False                 # descend directories
follow = False                  # follow symbolic links
npy = True                  	# make numpy files
png = False                  	# make a png preview file
bin_extn = 'coadd'              # binary format extension (SDSS default)
db_extns = 'specobj,spzline'    # DB table extensions (SDSS default)

db_name = 'sdss'                # database name
db_user = 'sdss'                # database username
db_password = 'sdss'            # database password
db_host = '0.0.0.0'  		# database host name

cursor = None                   # database cursor


#######################
##  TESTING BEGIN
#######################
#filename = '1963/spec-1963-54331-0614.fits'             # for testing only
#fits.info (filename)


#######################
##  TESTING END
#######################




@contextmanager
def getcursor():
    '''Get a cursor for the specified database from the pool.
    '''

    def getDBConf():
        ''' Get a db configuration.
        '''
        return {'database': opts.db_name,
                'host': opts.db_host,
                'user': opts.db_user,
                'password': opts.db_password,
                'port': 5432}

    dbConf = getDBConf()
    pool = psycopg2.pool.SimpleConnectionPool(1, 100, **dbConf)
    conn = pool.getconn()
    conn.autocommit = True
    try:
        yield conn.cursor()
    finally:
        pool.putconn(conn)


def boolQuery(cmd):
    '''Return a boolean result of a DB query.  '''
    try:
        with getcursor() as cur:
            cur.execute(cmd)
        res = str(cur.fetchone())
        cur.close()
        return ('True' in res)
    except Exception:
        import traceback
        traceback.print_exc()
        return False


def tableExists(schema, table):
    '''Check whether the <schema>.<table> exists.  '''
    cmd  = "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_foreign_table WHERE "
    if schema is not None:
        cmd += "'schema_name=%s'=ANY(ftoptions) AND " % schema
    cmd += "'table_name=%s'=ANY(ftoptions))" % table

    return boolQuery(cmd)



def psqlType (type):
    '''Convert a FITS BINTABLE column type (i.e. TFORM) to a compatible
       Postgres datatype.
    '''
    fitsTypes = {
        'A' : "varchar",            # 8-bit char
        'L' : "boolean",            # 8-bit logical (boolean)
        'B' : "smallint",           # 1-bit byte (unsigned)
        'S' : "character",          # 8-bit byte
        'I' : "smallint",           # 16-bit char
        'U' : "smallint",           # 16-bit char (unsigned)
        'J' : "integer",            # 32-bit int
        'V' : "integer",            # 32-bit int
        'K' : "bigint",             # 64-bit int (unsigned)
        'E' : "real",               # 32-bit float
        'D' : "double precision",   # 64-bit float
    }

    # Break a format indicator into size so we can handle arrays.
    match = re.match(r"([0-9]+)([a-zA-Z]+)", type, re.I)
    if match:
        _t = fitsTypes[match.groups()[1]]
        if _t != 'varchar':
            # int/real etc use square brackets for array size.
            _typ = _t + '[' + str(match.groups()[0]) + ']'
        else:
            _typ = _t + '(' + str(match.groups()[0]) + ')'
    else:
        _typ = "" if (type is None or type == "") else fitsTypes[type]

    return _typ


def dbTableName (base):
    if opts.tbl_suffix is None:
        return base
    else:
        return (base + '_' + opts.tbl_suffix)


def createSchema (filename, table, hdu=1):
    '''Create a Postgres table schema definition from a FITS BINTABLE header.
    '''
    hdr = fits.getheader(filename, hdu)
    nfields = int(hdr['TFIELDS'])

    schema = 'CREATE TABLE IF NOT EXISTS %s (\n' % dbTableName(table)
    for i in range (1,nfields+1):
        idx = str(i)
        _name = hdr['TTYPE'+idx]
        _type = psqlType(hdr['TFORM'+idx])
        _eol = ',' if (i < nfields) else ' '
        schema += '    %s  %s%c\n' % (_name, _type, _eol)
    schema += ');\n'

    return schema


def createInsert (name, table):
    '''Create an INSERT statement for a row in a table.
    '''
    stmt = 'INSERT INTO %s (' % dbTableName(name)
    for col in table.colnames:
        stmt += col + (',' if col != table.colnames[-1] else '')

    stmt += ') VALUES ('
    for col in table.colnames:
        c = table[col]
        sz = (1 if len(c.shape) == 1 else c.shape[1])
        if c.dtype.name == 'uint64':
            a = table[col] + 9223372036854775808
            value = "'" + str(a[0]).strip() + "'"       # quote strings
        elif c.dtype.name[:5] == 'bytes':
            a = table[col]
            value = "'" + str(a[0]).strip() + "'"       # quote strings
        elif sz > 1:
            a = table[col][0]                           # column value
            # Convert the Numpy array to a csv array and replace the square
            # brackets with quoted braces for postgres.
            value = np.array2string(a,separator=',')
            if a.ndim == 1:
                value = value.replace("[","'{").replace("]","}'")
            else:
                value = value.replace("[[","'{{").replace("]]","}}'")
                value = value.replace("[","{").replace("]","}")
        else:
            # Strip leading/trailing whitespace from value.
            #value = "'" + str(table[col][0]).strip() + "'"
            value = str(table[col][0]).strip()
        stmt += value + (',' if col != table.colnames[-1] else '')
    stmt += ');'

    return stmt


def getInputs (opts, args):
    '''Process options and args to get the input files/dirs.
       Use cases:
           1) sdss2db inspec1.fits outspec1.npy 
           2) sdss2db spec1.fits spec2.fits ......
           3) sdss2db -o outdir spec1.fits spec2.fits ...... 
           4) sdss2db *.fits 
           5) sdss2db -o outdir -r *.fits dir1 dir2
    '''
    ipath, ifile = './', None
    if opts.iname is not None:
        if os.path.isdir(opts.iname):
            ipath = (opts.iname + '/').replace('//','/')
        else:
            ifile = opts.iname
    elif len(args) > 0:
        ifile = args
    return ipath, ifile


def getOutputs (opts, args):
    '''Process options and args to get the output file or directory name.
    '''
    opath, ofile = './', None
    if opts.oname is not None:
        if not os.path.exists(opts.oname):           # create named directory
            if len(args) == 1 and args[0].find('.fits') > 0:
                ofile = opts.oname                   # use output name
            else:
                os.makedirs(opts.oname,mode=0o755)
        if os.path.isdir(opts.oname):
            opath = (opts.oname + '/').replace('//','/')
        else:
            ofile = opts.oname                       # use output name
    return opath, ofile


def processFile (infile, outfile):
    '''Process a single file given an input and output filename.
    '''

    def name2extn (hdulist, name):
        '''Given an extension name, return the HDU number.
        '''
        defaults = {'coadd' : 1, 
                    'specobj' : 2, 
                    'spzline' : 3
                   }
        for i in range(1,len(hdulist)):
            if hdulist[i].name.lower() == name:
                return i
        return defaults[name]
        

    if opts.verbose and not opts.debug:
        print('Processing: ' + infile)
                    
    hdulist = fits.open(infile)                 # open the file

    if bin_extn is not None:
        # Save the binary table as a binary numpy file.  This makes it easy
        # to send as a binary stream and load back into an array we can
        # manipulate before sending to a client.
        try:
            if opts.debug: print('Loading coadd table ...')
            data = Table.read(infile, hdu=name2extn(hdulist, bin_extn))
        except Exception as e:
            print ('Error: ' + str(e))
            return
        data = data.as_array()
        if opts.npy:
            np.save (outfile, data, allow_pickle=False)

        if opts.png:
            plt.ioff()
            plt.figure(figsize=(4, 1))
            #plt.axis('off')
            n = outfile.split('/')[-1][:-4].replace('spec-','')
            try:
                plt.plot(np.power(10.0, data['loglam']), data['flux'], label=n)
            except ValueError:
                plt.plot(np.power(10.0, data['LOGLAM']), data['FLUX'], label=n)
            plt.legend(loc='best', frameon=False, markerscale=None)
            plt.savefig(outfile.replace('.npy','.png'))
            plt.close()

    if db_extns is not None and not opts.png_only:
        # Load the named extensions to the database.
        with getcursor() as cur:
            for extn in db_extns.split(','):
                if not tableExists(None, dbTableName(extn)):
                    # Create the table schema if it doesn't exist.
                    if opts.drop:
                        cur.execute('drop table if exists %s' % dbTableName(extn))
                    if opts.debug: print('Creating schema for "%s" ...' % dbTableName(extn))
                    cmd = createSchema(infile,extn,hdu=name2extn(hdulist,extn))
                    cur.execute(cmd)

                if opts.truncate:
                    cur.execute('truncate table if exists %s' % dbTableName(extn))
                # Load the data from the table.
                try:
                    if opts.debug: print('Loading table "%s" ...' % dbTableName(extn))
                    data = Table.read(infile, hdu=name2extn(hdulist, extn))
                    if len(data) > 0:
                        cmd = createInsert(extn, data)
                        cur.execute(cmd)
                except Exception as e:
                    print ('Error: ' + str(e))
                    continue
        cur.close()
    hdulist.close()



# ########################################################################
#  Application MAIN
#

if __name__ == '__main__':
    #  Parse the arguments
    parser = optparse.OptionParser()

    parser.add_option('--mirror', '-m', action="store_true", dest="mirror",
                       help="Mirror directory structure", default=False)
    parser.add_option('--follow', '-f', action="store_true", dest="follow",
                       help="Follow symbolic links", default=False)
    parser.add_option('--recursive', '-r', action="store_true",dest="recursive",
                       help="Descend directories", default=False)
    parser.add_option('--npy', '', action="store_true",dest="npy",
                       help="Make npy files (default)", default=True)
    parser.add_option('--png', '', action="store_true",dest="png",
                       help="Make only png files", default=True)
    parser.add_option('--png_only', '', action="store_true",dest="png_only",
                       help="Make only png files", default=False)

    parser.add_option('--input', '-i', action="store", dest="iname",
                       help="Input directory name", default=None)
    parser.add_option('--output', '-o', action="store", dest="oname",
                       help="Output directory name", default=None)

    parser.add_option('--bin_extn', '-B', action="store", dest="bin_extn",
                       help="Extensions to convert to bin", default=None)
    parser.add_option('--db_extns', '-D', action="store", dest="db_extns",
                       help="Extensions to convert to load in DB", default=None)
    parser.add_option('--tbl_suffix', '-T', action="store", dest="tbl_suffix",
                       help="Suffix to add to DB table names", default=None)
    parser.add_option('--ssa', '-S', action="store", dest="ssa_table",
                       help="Load PHU in DB SSA table", default=None)

    parser.add_option('--db', '', action="store", dest="db_name",
                       help="Database name", default=db_name)
    parser.add_option('--user', '', action="store", dest="db_user",
                       help="DB connection user name", default=db_user)
    parser.add_option('--password', '', action="store", dest="db_password",
                       help="DB connection password", default=db_password)
    parser.add_option('--host', '', action="store", dest="db_host",
                       help="DB connection host name", default=db_host)

    parser.add_option('--drop', '', action="store_true", dest="drop",
                       help="Drop table before create", default=False)
    parser.add_option('--truncate', '', action="store_true", dest="truncate",
                       help="Truncate table before load", default=False)
    parser.add_option('--verbose', '', action="store_true", dest="verbose",
                       help="Print verbose output", default=False)
    parser.add_option('--debug', '', action="store_true", dest="debug",
                       help="Print debug output", default=False)

    opts, args = parser.parse_args()


    # If we're not mirroring then don't create subdirectories.
    if not opts.mirror:
        opts.recursive = False

    # Get the database cursor if we need one.
    cursor = None if opts.db_extns is None else getcursor()

    # Setup the input and output paths.
    ipath, ifile = getInputs (opts, args)
    opath, ofile = getOutputs (opts, args)

    if ifile is None:
        print ('Error: No input files to process')
        sys.exit (1)
    if opts.png_only:
        opts.png = True
        opts.npy = False
    db_extns = opts.db_extns


    # Loop over each of the input items. These may be individual files or
    # directories.  If directories we'll descend the the tree if using the
    # recursive option, otherwise these are ignored.
    for f in ifile:
        if not os.path.exists(f):               # skip non-existent filenames
            print ('Warning: input file "%s" does not exist' % f)
            continue

        if os.path.isdir(f):
            # Loop over directory names.
            sys_path = ipath + f
            for root,subdirs,files in os.walk(sys_path,
                                              followlinks=opts.follow):

                # Copy the directory structure if mirroring.
                if opts.mirror:
                    structure = os.path.join(opath, root[len(ipath+f):])
                    if not os.path.isdir(structure):
                        os.mkdir(structure)

                for _f in files:
                    _root = os.path.splitext(root)[0] # root filename inc. path
                    _base = root.split('/')[-1]       # base filename exc. path
                    _extn = os.path.splitext(root)[1] # file extension

                    if os.path.splitext(_f)[1] not in ['.fits','.fit','.fz']:
                        continue

                    _in = _root + '/' + _f
                    _out = os.path.splitext(_f)[0] + '.npy'
                    if opts.mirror:
                        _out = opath + _root[len(ipath+f):] + '/' + _out
                    else:
                        _out = opath + _out

                    _out = _out.replace('//','/')
                    if opts.debug: print ('Processing: '+_in+' --> '+_out)
                    if not os.path.exists(_out):
                        processFile (_in, _out)

                # Only process toplevel dir if not recursive.
                if not opts.recursive:          
                    break
        else:
            root = os.path.splitext(f)[0]           # root filename inc. path
            base = root.split('/')[-1]              # base filename exc. path
            extn = os.path.splitext(f)[1]           # file extension

            # Skip non-FITS files.  Rather than check the file contents we
            # simply require a commonly-used extension, this allows us to
            # specify a directory name or "*" filename template and process
            # only the FITS files therein.
            if extn not in ['.fits','.fit','.fz']:
                continue

            _in = f
            if root[0] not in ['.','/']:        # no relative/absolute path
                _in = ipath + _in
            _out = base + '.npy'
            if base[0] not in ['.','/']:        # no relative/absolute path
                _out = opath + _out

            if opts.debug:
                print ('Processing: '+_in+' --> '+_out)
            processFile (_in, _out)


