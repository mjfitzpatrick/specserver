#!/usr/bin/env python

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = 'v1.0.0'


import optparse
import redis

import specClient as spec



SERVICE_URL     = "http://127.0.0.1:6999"
DEBUG		= True


#####################################
#  Local User Utility procedures
#####################################



#  Application MAIN
#
if __name__ == '__main__':

    #  Parse the arguments
    parser = optparse.OptionParser()
    parser.add_option ('--cmd', '-c', action="store", dest="cmd",
                        help="Test command", default="init")
    parser.add_option ('--user', '-u', action="store", dest="user",
                        help="User name", default="demo")
    parser.add_option ('--password', '-p', action="store", dest="passwd",
                        help="User password", default="dldemo")

    options, args = parser.parse_args()


    if DEBUG:
        print ("test opts:  cmd = " + options.cmd)
        for a in args:
             print ("arg: " + a)

    if options.cmd == 'init':				# INIT
        pass

    elif options.cmd == 'ping':				# PING
        pass

    else:
        print ("Error:  Unknown command '" + options.cmd + "'")


