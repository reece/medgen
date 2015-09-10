from __future__ import absolute_import
from .log import log
####################################################################################
class MedGenError(Exception):

    def __init__(self, error=None, cause=None):
        self.error = error
        self.cause = cause

    def __str__(self):
        return str(__dict__)

class ConnectError(MedGenError):
    """
    caused by connection errors in
    HGVS Parser, Mapper, UTA, or PubTator.
    Thrown when the annotator cannot load a resource
    such as the HGVS grammar file or sql databases.
    """
    pass

class ParseError(MedGenError):
    def __init__(self, issue=None, cause=None):
        self.issue = issue
        self.cause = cause

    def __str__(self):
        return str(self.__dict__)

####################################################################################

class DBError(MedGenError): pass

class AnnotateError(MedGenError):pass

class TODO(MedGenError): pass

####################################################################################

class JIRA_TODO(MedGenError):
    def __init__(self, issue=None, cause=None):
        if  issue==None:
            issue='Not Yet Implemented'

        self.issue = str('https://invitae.jira.com/browse/'+ str(issue))
        self.cause = cause

    def __str__(self):
        return str(self.__dict__)

####################################################################################
#
# GUARDS (usage optional) 
# 
####################################################################################

def GuardNotNone(echo, strict=True):
    """
    This checks whether an object is None.
    If echo is None and strict is set to True (default), throws an exception
    (or whatever GuardError has now been set to do.
    If echo is None and strict is set to False, will return None.

    :param echo: the variable to be checked
    :param strict: whether an exception should be thrown
    :return: the original object (echo)
    """
    log.debug(echo)

    return \
        echo if (echo is not None) else \
        None if (strict is not True) else GuardError(echo, strict)

def GuardAssert(echo, assertion=False, strict=True):
    return \
        echo if assertion is True else \
        GuardNotNone(echo,strict) if not strict else \
        GuardError(echo, strict, assertion)


def GuardError(echo, assertion=False, strict=True):
    error = ' echo ='+str(echo) + " strict ="+str(strict)+ " assertion="+str(assertion)
    if strict:
        raise Exception('Guard error: found value is None' )#error)
    else:
        print 'GuardError(?)' + error
