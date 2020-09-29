
from svc_sdss import sdssService

# Plug-in service registry
services = {
    'default' : sdssService,
    'sdss_dr16' : sdssService,
    'sdss_dr15' : sdssService('dr15')
    }

