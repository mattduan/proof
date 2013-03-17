"""
A factory which instantiates DataSource.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"
 

import proof.ProofConstants as ProofConstants

import proof.datasource.MySQLDataSource as MySQLDataSource


class DataSourceFactory:

    def __init__(self):
        pass

    def create(self, adapter, **kargs):
        """ Factory method which instantiates DataSource based on the
            return value of the provided adapter's getResourceType method.
            Returns <code>None</code> for unknown types.
 
            @param adapter The type of adapter to create a DataSource for.
            @return The appropriate DataSource (possibly <code>None</code>).
        """
        res_type = adapter.getResourceType()

        if res_type == ProofConstants.MYSQL:
            host     = kargs.get('host', '')
            username = kargs.get('username', '')
            password = kargs.get('password', '')
            dbname   = kargs.get('dbname', '')
            logger   = kargs.get('logger', None)
            return MySQLDataSource.MySQLDataSource( host,
                                                    username,
                                                    password,
                                                    dbname,
                                                    logger )
        else:
            return None
