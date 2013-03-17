"""
A PooledDataSource object is a factory for PooledConnection objects.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import logging

import util.logger.Logger as Logger

import proof.ProofException as ProofException

class PooledDataSource:

    __is__ = 'interface'

    def __init__( self,
                  host,
                  username,
                  password,
                  dbname,
                  pool,
                  logger = None ):
        """ Constructor.
        """
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write

    #==================== Interfaces ==========================
    
    def getPooledConnection(self, **kwargs):
        """ Establish a database connection and return it.
        """
        raise ProofException.ProofNotImplementedException( \
            "PooledDataSource.getPooledConnection: need to be overrided by db specific PooledDataSource." )

    def getLogger(self):
        return self.__logger

    def setLogger(self, logger):
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write

