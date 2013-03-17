"""
Interface to be implemented by id generators.  It is possible
that some implementations might not require all the arguments,
for example MySQL will not require a keyInfo Object, while the
IDBroker implementation does not require a Connection as
it only rarely needs one and retrieves a connection from the
Connection pool service only when needed.
"""

__version__='$Revision: 3194 $'[11:-2]


import logging

import util.logger.Logger as Logger

import proof.ProofException as ProofException


class IDGenerator:

    __is__ = 'interface'

    def __init__(self, logger=None):
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write
        pass

    def getId(self, connection=None, key_info=None):
        """ Returns an id.
        
            @param connection A Connection.
            @param key_info an Object that contains additional info.
        """
        raise ProofException.ProofNotImplementedException( \
            "IdGenerator.getId: need to be overrided." )

    def isPriorToInsert(self):
        """ A flag to determine the timing of the id generation.

            @return a <code>boolean</code> value
        """
        raise ProofException.ProofNotImplementedException( \
            "IdGenerator.isPriorToInsert: need to be overrided." )

    def isPostInsert(self):
        """ A flag to determine the timing of the id generation
        
            @return Whether id is availble post-<code>insert</code>.
        """
        raise ProofException.ProofNotImplementedException( \
            "IdGenerator.isPostInsert: need to be overrided." )

    def isConnectionRequired(self):
        """ A flag to determine whether a Connection is required to
            generate an id.
            
            @return a <code>boolean</code> value
        """
        raise ProofException.ProofNotImplementedException( \
            "IdGenerator.isConnectionRequired: need to be overrided." )
    
