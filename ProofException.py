"""
All exception classes in PROOF.
"""

__version__='$Revision: 117 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"



##############################################
#                EXCEPTIONS
##############################################

class ProofException(Exception):
    """ PROOF exception.
    """
    
    def __init__(self, error):
        """ Constructor.
            @hidden
        """
        Exception.__init__(self, "%s" % error)


class ProofNotFoundException(ProofException):
    """ Fatal exception.
    """
  
    def __init__(self, error):
        """ Constructor.
            @hidden
        """
        ProofException.__init__(self, "%s" % error)
  
 
class ProofImproperUseException(ProofException):
    """ Fatal exception.
    """
  
    def __init__(self, error):
        """ Constructor.
            @hidden
        """
        ProofException.__init__(self, "%s" % error)

class ProofTransactionException(ProofException):
    """ Fatal exception.
    """
  
    def __init__(self, error):
        """ Constructor.
            @hidden
        """
        ProofException.__init__(self, "%s" % error)

class ProofNotImplementedException(ProofException):
    """ Fatal exception.
    """
  
    def __init__(self, error):
        """ Constructor.
            @hidden
        """
        ProofException.__init__(self, "%s" % error)
        
class ProofConnectionException(ProofException):
    """ Fatal exception.
    """
  
    def __init__(self, error):
        """ Constructor.
            @hidden
        """
        ProofException.__init__(self, "%s" % error)

class ProofSQLException(ProofException):
    """ Fatal exception.
    """
  
    def __init__(self, error):
        """ Constructor.
            @hidden
        """
        ProofException.__init__(self, "%s" % error)

class ProofResourceFailure(ProofException):
    """ Fatal exception.
    """
  
    def __init__(self, error):
        """ Constructor.
            @hidden
        """
        ProofException.__init__(self, "%s" % error)

