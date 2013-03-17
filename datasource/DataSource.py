"""
A DataSource object is a factory for Connection objects.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"

import logging

import util.logger.Logger as Logger

import proof.ProofException as ProofException

class DataSource:

    __is__ = 'interface'

    def __init__( self,
                  host,
                  username,
                  password,
                  dbname,
                  logger = None ):
        """ Constructor.
        """
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write        

    #==================== Interfaces ==========================
    
    def getConnection(self, **kwargs):
        """ Establish a database connection and return it.
        """
        raise ProofException.ProofNotImplementedException( \
            "DataSource.getConnection: need to be overridden by db specific DataSource." )

    def getLogger(self):
        return self.__logger

    def setLogger(self, logger):
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write

