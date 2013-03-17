"""
Cursor implementation for MySQL database server.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"

#import mysql.cursors as cursors
import MySQLdb.cursors as cursors
import proof.driver.Cursor as Cursor

class MySQLDictCursor(Cursor.Cursor):
    """ A wrapper to MySQLdb.DictCursor, but implements Cursor.Cursor
        interfaces.
    """

    def __init__(self, connection):
        self.connection = connection
        self.__cursor = cursors.DictCursor(connection)

    def close(self):
        self.__cursor.close()
        self.connection = None

    def execute(self, q, args=None):
        """ Return query rowcount.
        """
        return self.__cursor.execute(q, args)
        
    def query(self, q):
        """ Return query rowcount. 
        """
        return self.__cursor.execute(q)
        
    def update(self, q, args=None):
        """ Return an integer idicating how many rows updated.
        """
        return self.__cursor.execute(q)

    def fetchone(self):
        return self.__cursor.fetchone()

    def fetchmany(self, size=None):
        return self.__cursor.fetchmany(size=size)

    def fetchall(self):
        return self.__cursor.fetchall()

    def info(self):
        """ Return the message from db when doing query.
        """
        return self.__cursor._info

    def insert_id(self):
        """ Return last insert id.
        """
        #return self.__cursor.insert_id()
        return self.__cursor.lastrowid

    def nextset(self):
        return self.__cursor.nextset()

    def seek(self, row, mode='relative'):
        """ Looking for a row.
        """
        self.__cursor.scroll(row, mode=mode)

    def tell(self):
        """ Return the current position.
        """
        return self.__cursor.rownumber
    
    def getConnection(self):
        return self.connection

    def __del__(self):
        try:
            self.close()
            del self.__cursor
        finally:
            self = None

