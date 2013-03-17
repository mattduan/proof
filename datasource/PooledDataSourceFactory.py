"""
A factory which instantiates PooledDataSource.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"
 

import proof.ProofConstants as ProofConstants
import proof.datasource.MySQLPooledDataSource as MySQLPooledDataSource


class PooledDataSourceFactory:

    def __init__(self):
        pass

    def create(self, pool, **kargs):
        """ Factory method which instantiates PooledDataSource based on the
            return value of the provided adapter's getResourceType method.
            Returns <code>None</code> for unknown types.
 
            @param pool The pool containing the PooledDataSource.
            @return The appropriate DataSource (possibly <code>None</code>).
        """
        adapter  = pool.getAdapter()
        res_type = adapter.getResourceType()

        if res_type == ProofConstants.MYSQL:
            host     = kargs.get('host', '')
            username = kargs.get('username', '')
            password = kargs.get('password', '')
            dbname   = kargs.get('dbname', '')
            logger   = kargs.get('logger', None)
            return MySQLPooledDataSource.MySQLPooledDataSource( host,
                                                                username,
                                                                password,
                                                                dbname,
                                                                pool,
                                                                logger )
        else:
            return None
