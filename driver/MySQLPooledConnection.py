"""
A connection object provides hooks for ConnectionPool. The connection can
be recycled rather than being closed when it is not in a connection pool. 
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import proof.driver.MySQLConnection as MySQLConnection


class MySQLPooledConnection(MySQLConnection.MySQLConnection):

    def __init__(self, **kwargs):
        host              = kwargs['host']
        user              = kwargs['user']
        passwd            = kwargs['passwd']
        db                = kwargs['db']
        unix_socket       = kwargs.get('unix_socket', '/tmp/mysql.sock')
        read_default_file = kwargs.get('read_default_file', '/etc/my.cnf')
        MySQLConnection.MySQLConnection.__init__( self, 
                                                  host              = host, 
                                                  user              = user, 
                                                  passwd            = passwd, 
                                                  db                = db,
                                                  unix_socket       = unix_socket,
                                                  read_default_file = read_default_file )
        
        self.__pool = kwargs.get('pool', None)
        # make sure self is not in the pool
        if self.__pool and self.__pool.hasConnection(self):
            self.__pool.popConnection(self)

    def close(self):
        if self.__pool and not self.__pool.hasConnection(self):
            self.__pool.releaseConnection(self)
        else:
            MySQLConnection.MySQLConnection.close(self)

    def getConnection(self):
        return self

    def getConnectionPool(self):
        return self.__pool

    def releaseConnectionPool(self):
        """ This method will remove the reference to the related
            connection pool. It will be like a regular connection
            object. It should only be called in ConnectionPool
            before the real connection is closed.
        """
        self.__pool = None
    
