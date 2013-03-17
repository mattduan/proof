"""
The implementation of MySQL DataSource object.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import proof.driver.MySQLConnection as MySQLConnection
import proof.datasource.DataSource as DataSource


class MySQLDataSource(DataSource.DataSource):

    __implements__ = 'DataSource'

    def __init__( self,
                  host,
                  username,
                  password,
                  dbname,
                  logger = None ):
        # init logger object
        DataSource.DataSource.__init__( self,
                                        host,
                                        username,
                                        password,
                                        dbname,
                                        logger=logger )

        self.__host        = host
        self.__username    = username
        self.__password    = password
        self.__dbname      = dbname
    
    def getConnection( self,
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

        return MySQLConnection.MySQLConnection( host        = self.__host,
                                                user        = self.__username,
                                                passwd      = self.__password,
                                                db          = self.__dbname,
                                                unix_socket = unix_socket,
                                                read_default_file = read_default_file )
    
