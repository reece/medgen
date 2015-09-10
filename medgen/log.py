import sys
import logging
import datetime
from  collections import OrderedDict

log = logging.getLogger("medgen")

##########################################################################
# LOGGING
#
# TODO: configure externally
#
#
# Could we please *not* have a StreamHandler defined here? 
# This kind of thing belongs in a command line script, not buried deep 
# in a hard-coded config file.
#   
# Consider this a requirement for making this library open source.  
#   
#   --NM 8/6/2015
#
##########################################################################

HGVS_LOG_LEVEL = logging.DEBUG

console = logging.StreamHandler(sys.stdout)
console.setFormatter(logging.Formatter('%(module)s  %(funcName)s(%(lineno)d): %(levelname)s %(message)s'))

log.setLevel(HGVS_LOG_LEVEL)
log.addHandler(console)

IS_DEBUG_ENABLED = False #log.isEnabledFor(logging.DEBUG)
IS_INFO_ENABLED  = log.isEnabledFor(logging.INFO)

biomed_timer_start = datetime.datetime.now()
biomed_timer_stop  = datetime.datetime.now()

class LogTimer:

    def __init__(self):
        self._start_time = None
        self._stop_time  = None
        self.passed = OrderedDict()
        self.failed = OrderedDict()
        self.restart()

    def restart(self):
        "Start or restart"
        self._start_time= datetime.datetime.now()
        self._stop_time = None
        log.debug(str(self))

    def log_passed(self, item, result=None):
        log.debug(str(item)+':'+str(result))
        self.passed[item] = result

    def log_failed(self, item, error=None):
        log.warn(str(item)+':'+str(error))
        self.failed[item] = error

    def elapsed(self):
        self._stop_time = datetime.datetime.now()

        diff = (self._stop_time - self._start_time)

        log.info('elapsed:'+ str(diff))
        log.info('passed:'+ str(len(self.passed.keys())))
        log.info('failed:'+ str(len(self.failed.keys())))

        return diff

    def __str__(self):
        return '@LogTimer '+ str(self.__dict__)


def LogContainer(_func, _collection):
    t = LogTimer()
    for item in _collection:
        try:
            t.log_passed(item, _func(item))

        except Exception as e:
            t.log_failed(item, e)
            t.elapsed()

    t.elapsed()
    return t

