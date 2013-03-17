"""
ProofResourceMockup.py
======================

A ProofResource mockup implementation.

"""

import logging

import proof.ProofResource as ProofResource


class ProofResourceMockup(ProofResource.ProofResource):

    def __init__( self,
                  config_filename,
                  schema_name = '',
                  database_name = '',
                  db_conf = {'host'     : 'localhost',
                             'database' : 'test_proof',
                             'username' : 'xxx',
                             'password' : ''},
                  logger = None ):
        ProofResource.ProofResource.__init__( self,
                                              config_filename,
                                              logger = logger )
        self._schema_name = schema_name
        self._database_name = database_name
        self._db_conf = db_conf
        self._db_conf['database'] = database_name
    
    def getSchemaKey(self, database=None):
        return self._schema_name
    
    getSchemaName = getSchemaKey
    
    def getDatabaseName(self, schema=None, namespace=None):
        return self._database_name
    
    def getDBConf(self, schema=None, namespace=None):
        return self._db_conf
    
    def getNameSpaceName(self, database=None):
        return 'test'
    
    def hasNameSpace(self, namespace):
        return True
    

