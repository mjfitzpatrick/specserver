
from svc_sdss import sdssService

# Plug-in service registry
services = {
    'default' : sdssService,
    'sdss_dr16' : sdssService('dr16'),
    'sdss_dr15' : sdssService('dr15'),
    'sdss_dr14' : sdssService('dr14'),
    'sdss_dr13' : sdssService('dr13'),
    'sdss_dr12' : sdssService('dr12'),
    'sdss_dr11' : sdssService('dr11'),
    'sdss_dr10' : sdssService('dr10'),
    'sdss_dr9' : sdssService('dr9'),
    'sdss_dr8' : sdssService('dr8')
    }

