"""
The Connection class for MySQL database. 
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


#import mysql
import MySQLdb

import proof.ProofException as ProofException
import proof.driver.Connection as Connection
import proof.driver.MySQLCursor as MySQLCursor
import proof.driver.MySQLDictCursor as MySQLDictCursor

class MySQLConnection(Connection.Connection):

    """ A wrapper to MySQLdb Connection class.
    """

    def __init__(self, **kwargs):
        self.__connection = MySQLdb.Connection( **kwargs )
        self.__autocommit = True

    def close(self):
        self.__connection.close()

    def commit(self):
        self.__connection.commit()

    def rollback(self):
        try:
            self.__connection.rollback()
        except:
            pass

    def cursor(self, ret_dict=0):
        if ret_dict:
            return self.__connection.cursor(cursorclass=MySQLDictCursor.MySQLDictCursor)
        else:
            return self.__connection.cursor(cursorclass=MySQLCursor.MySQLCursor)

    getCursor = cursor

    def setAutoCommit(self, b):
        self.__autocommit = b

    def getAutoCommit(self):
        return self.__autocommit
    
    def supportsTransactions(self):
        return False


