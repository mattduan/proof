"""
The abstract base Cursor class for all DB specific drivers. 
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import proof.ProofException as ProofException


class Cursor:

    def __init__(self, connection):
        self.connection = connection

    def close(self):
        self.connection = None

    def execute(self, q, args=None):
        """ Return query rowcount.
        """
        raise ProofException.ProofNotImplementedException( \
            "Cursor.execute: need to be overrided." )

    def query(self, q):
        """ Return query rowcount. 
        """
        raise ProofException.ProofNotImplementedException( \
            "Cursor.query: need to be overrided." )
        
    def update(self, q, args=None):
        """ Return an integer idicating how many rows updated.
        """
        raise ProofException.ProofNotImplementedException( \
            "Cursor.update: need to be overrided." )

    def fetchone(self):
        raise ProofException.ProofNotImplementedException( \
            "Cursor.fetchone: need to be overrided." )

    def fetchmany(self, size=None):
        raise ProofException.ProofNotImplementedException( \
            "Cursor.fetchmany: need to be overrided." )

    def fetchall(self):
        raise ProofException.ProofNotImplementedException( \
            "Cursor.fetchall: need to be overrided." )

    def info(self):
        """ Return the message from db when doing query.
        """
        return ""

    def insert_id(self):
        """ Return last insert id.
        """
        raise ProofException.ProofNotImplementedException( \
            "Cursor.insert_id: need to be overrided." )

    def nextset(self):
        """ Advance to the next result set.
            Returns None if there are no more result sets.
        """
        raise ProofException.ProofNotImplementedException( \
            "Cursor.nextset: need to be overrided." )

    def seek(self, row, mode='relative'):
        """ Looking for a row.
        """
        raise ProofException.ProofNotImplementedException( \
            "Cursor.seek: need to be overrided." )

    def tell(self):
        """ Return the current position.
        """
        raise ProofException.ProofNotImplementedException( \
            "Cursor.tell: need to be overrided." )
    
    def getConnection(self):
        return self.connection
