__doc__= """ Configuration setup """

import os
import ConfigParser
from .log import log

# TODO: documentation?

PKGNAME = 'medgen'

# env variable MEDGEN_CONFIG_DIR should be an absolute path to a directory.
# if this env doesn't exist, default to looking in package's config files.

default_config_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config')

CFGDIR = os.getenv('MEDGEN_CONFIG_DIR', default_config_dir)
ENV = os.getenv('MEDGEN_ENV', 'default')


log.info("MedGen Configuration Settings: ")
log.info("CFGDIR: %s" % CFGDIR)
log.info("ENV: %s" % ENV)


def _get_config(dirname=CFGDIR, env=ENV):
    config = ConfigParser.ConfigParser()
    configs = [os.path.join(dirname, x) for x in os.listdir(dirname) if x.find(env) > -1]
    config.read(configs)
    return config

config = _get_config()

