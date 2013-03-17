"""
The abstract base Connection class for all DB specific connections. 
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import proof.ProofException as ProofException


class Connection:

    def __init__(self, **kwargs):
        pass

    def close(self):
        raise ProofException.ProofNotImplementedException( \
            "Connection.close: need to be overrided." )

    def commit(self):
        raise ProofException.ProofNotImplementedException( \
            "Connection.close: need to be overrided." )

    def rollback(self):
        raise ProofException.ProofNotImplementedException( \
            "Connection.close: need to be overrided." )

    def cursor(self):
        raise ProofException.ProofNotImplementedException( \
            "Connection.close: need to be overrided." )

    getCursor = cursor

    def setAutoCommit(self, b):
        pass

    def getAutoCommit(self):
        return True
    
    def supportsTransactions(self):
        return False
