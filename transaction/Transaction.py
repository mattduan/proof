"""
 begin/commit/rollback transaction methods.
 
 This can be used to handle cases where transaction support is optional.
 The second parameter of beginOptionalTransaction will determine with a 
 transaction is used or not. 
 If a transaction is not used, the commit and rollback methods
 do not have any effect. Instead it simply makes the logic easier to follow
 by cutting down on the if statements based solely on whether a transaction
 is needed or not.
"""

__version__= '$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import logging

import util.logger.Logger as Logger
import util.Trace as Trace

import proof.driver.Connection as Connection
import proof.ProofException as ProofException


class Transaction:
    
    def __init__( self,
                  proof_instance,
                  logger=None ):
        self.__proof_instance = proof_instance
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write
        self.__con = None

    def begin(self, dbName, useTransaction=True):
        """ Begin a transaction.  This method will fallback gracefully to
            return a normal connection, if the database being accessed does
            not support transactions.
            
            @param dbName Name of database.
            @return The Connection for the transaction.
        """
        self.__con = self.__proof_instance.getConnection(dbName)
        if useTransaction:
            try:
                if self.__con.supportsTransactions():
                    self.__con.setAutoCommit(False)
            except:
                raise ProofTransactionException( "Transaction.begin(): %s" % \
                                                 (Trace.traceBack()) )

        return self.__con

    def commit(self):
        """ Commit a transaction.  This method takes care of releasing the
            connection after the commit.  In databases that do not support
            transactions, it only returns the connection.
            
            @param con The Connection for the transaction.
        """
        assert isinstance(self.__con, Connection.Connection)

        try:
            if self.__con.supportsTransactions() and \
                   not self.__con.getAutoCommit():
                self.__con.commit()
                self.__con.setAutoCommit(True)
        except:
            raise ProofTransactionException( "Transaction.commit(): %s" % \
                                             (Trace.traceBack()) )
        # close connection
        self.__con = self.__proof_instance.closeConnection(self.__con)
        
    def rollback(self):
        """ Roll back a transaction in databases that support transactions.
            It also releases the connection. In databases that do not support
            transactions, this method will log the attempt and release the
            connection.
        """
        assert isinstance(self.__con, Connection.Connection)

        try:
            try:
                if self.__con.supportsTransactions() and \
                       not self.__con.getAutoCommit():
                    self.__con.rollback()
                    self.__con.setAutoCommit(True)
            except:
                raise ProofTransactionException( "Transaction.rollback(): %s" % \
                                                 (Trace.traceBack()) )
        finally:
            # close connection
            self.__con = self.__proof_instance.closeConnection(self.__con)

    def safeRollback(self):
        """ Roll back a transaction without throwing errors if they occur.
            A null Connection argument is logged at the debug level and other
            errors are logged at warn level.
        """
        if not isinstance(self.__con, Connection.Connection):
            self.log( "Transaction.safeRollback(): self.__con = %s" % (`self.__con`) )
        
        else:
            try:
                self.rollback()
            except:
                self.log( "An error occured during rollback: %s" % (Trace.traceBack()),
                            logging.WARNING )

