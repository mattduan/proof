"""
The implementation of MySQL PooledDataSource object.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import proof.driver.MySQLPooledConnection as MySQLPooledConnection
import proof.datasource.PooledDataSource as PooledDataSource


class MySQLPooledDataSource(PooledDataSource.PooledDataSource):

    __implements__ = 'PooledDataSource'

    def __init__( self,
                  host,
                  username,
                  password,
                  dbname,
                  pool,
                  logger = None ):
        # init logger object
        PooledDataSource.PooledDataSource.__init__( self,
                                                    host,
                                                    username,
                                                    password,
                                                    dbname,
                                                    pool,
                                                    logger=logger )

        self.__host       = host
        self.__username   = username
        self.__password   = password
        self.__dbname     = dbname
        self.__pool       = pool

    def getPooledConnection( self,
                             host = None,
                             username = None,
                             password = None,
                             dbname = None,
                             unix_socket = '/tmp/mysql.sock',
                             read_default_file = '/etc/my.cnf' ):
        if host and username and password and dbname:
            self.__host       = host
            self.__username   = username
            self.__password   = password
            self.__dbname     = dbname

        return MySQLPooledConnection.MySQLPooledConnection( host   = self.__host,
                                                            user   = self.__username,
                                                            passwd = self.__password,
                                                            db     = self.__dbname,
                                                            pool   = self.__pool,
                                                            unix_socket = unix_socket,
                                                            read_default_file = read_default_file )
    
