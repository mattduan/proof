"""
Resource Registry defines the constants mapping among database types,
drivers, datasources and adapters.

All resources should be registered here in order to locate them by
the resource type.

The resource types are defined first. The registry should have this
format:

REGISTRY = {
    RESOURCE_TYPE1 => { ENTRY1 => ( MODULE, CLASS ),
                        ENTRY2 => ( MODULE, CLASS ),
                        ...
                       },
    RESOURCE_TYPE2 => { ENTRY1 => ( MODULE, CLASS ),
                        ENTRY2 => ( MODULE, CLASS ),
                        ...
                       },
           }

"""

__version__='$Revision: 117 $'[11:-2]


MYSQL = 'mysql'

RESOURCE_TYPES = [ MYSQL ]


RESOURCE_REGISTRY = {
    MYSQL : { '__driver__'           : ( 'proof.driver.MySQLConnection', 'MySQLConnection' ),
              '__connection__'       : ( 'proof.driver.MySQLConnection', 'MySQLConnection' ),
              '__pooledconnection__' : ( 'proof.driver.MySQLPooledConnection', 'MySQLPooledConnection' ),
              '__datasource__'       : ( 'proof.datasource.MySQLDataSource', 'MySQLDataSource' ),
              '__pooleddatasource__' : ( 'proof.datasource.MySQLPooledDataSource', 'MySQLPooledDataSource' ),
              '__adapter__'          : ( 'proof.adapter.MySQLAdapter', 'MySQLAdapter' ) },
    }
