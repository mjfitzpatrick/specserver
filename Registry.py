
from svc_sdss import sdssService

# Plug-in service registry
services = {
    'default' : sdssService(),
    'sdss_dr16' : sdssService(release='dr16'),
    'sdss_dr15' : sdssService(release='dr15'),
    'sdss_dr14' : sdssService(release='dr14'),
    'sdss_dr13' : sdssService(release='dr13'),
    'sdss_dr12' : sdssService(release='dr12'),
    'sdss_dr11' : sdssService(release='dr11'),
    'sdss_dr10' : sdssService(release='dr10'),
    'sdss_dr9' : sdssService(release='dr9'),
    'sdss_dr8' : sdssService(release='dr8'),
    }

