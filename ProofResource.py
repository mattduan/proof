"""
The PROOF resource factory. This class provides all the resource data needed by
a proof instance. These data include database map, adapter type, table/module
mapping, table/class mapping, schema/namespace/database mapping and database
connection configuration. It defines interfaces needed by the ProofInstance.

Each PROOF implementation should have its own resource class by overriding the
necessary functions. The resource can come from a configuration file, xml file
or a database.

ProofResource provides resources in schema wide scope. It is not any database
specific. But schema and database names should be transparent and exchangible.

By separating the resource from the proof instance, we make the ProofInstance
possible to work both as a stand-alone and shared persistent data layer.
It also make testing easier by creating some dummy resources.

The resource data are parsed from configuration files, which can be both in
XML or python dict format. For details, refer to
proof/template/config/resource.xml
proof/template/config/resource.py

There are six dictionaries in this class to maintain these data:

# database maps
database_maps = {
  'schema_nameX' : database_mapX,
  'schema_nameY' : database_mapY,
  ... ...
}

# adapter maps
adapter_maps = {
  'schema_nameX' : adapterX,
  'schema_nameY' : adapterY,
  ... ...
}

# object maps
object_maps = {
  'schema_nameX' : { 'table_nameX' : { 'module'        : 'xxxxxx',
                                       'class'         : 'ssssss',
                                       'factorymodule' : 'dddddd',
                                       'factoryclass'  : 'wwwwww' },
                     'table_nameY' : { 'module'        : 'xxxxxx',
                                       'class'         : 'ssssss',
                                       'factorymodule' : 'dddddd',
                                       'factoryclass'  : 'wwwwww' },
                     ... ...
                     }
  'schema_nameY' : { 'table_nameX' : { 'module'        : 'xxxxxx',
                                       'class'         : 'ssssss',
                                       'factorymodule' : 'dddddd',
                                       'factoryclass'  : 'wwwwww' },
                     'table_nameY' : { 'module'        : 'xxxxxx',
                                       'class'         : 'ssssss',
                                       'factorymodule' : 'dddddd',
                                       'factoryclass'  : 'wwwwww' },
                     ... ...
                     }
  ... ...
}

# aggregate maps
aggregate_maps = {
  'schema_nameX' : { 'aggregate_nameX' : { 'module'           : 'xxxxxx',
                                           'class'            : 'ssssss',
                                           'factorymodule'    : 'dddddd',
                                           'factoryclass'     : 'wwwwww',
                                           'repositorymodule' : 'wwwwww',
                                           'repositoryclass'  : 'wwwwww' },
                     'aggregate_nameX' : { 'module'           : 'xxxxxx',
                                           'class'            : 'ssssss',
                                           'factorymodule'    : 'dddddd',
                                           'factoryclass'     : 'wwwwww',
                                           'repositorymodule' : 'wwwwww',
                                           'repositoryclass'  : 'wwwwww' },
                     ... ...
                     }
  ... ...
}

# schema/namespace to database map
namespace_maps = {
  'schema_nameX' : { 'namespaceX' : { 'dbname'   : 'databasename',
                                      'host'     : 'hostname',
                                      'username' : 'guest',
                                      'password' : '12345' },
                     ... ...
                     },
  ... ...
}

# database to schema reverse lookup
db_schema_maps = {
  'databaseX' : [ 'schemaX', 'namespaceX' ],
  'databaseY' : [ 'schemaY', 'namespaceY' ],
  ... ...
}

There are also five stragetic flags to indicate what type of data in each
dictionary. They are adapter_strategy, schema_strategy, namespace_strategy,
object_strategy and aggregate_strategy. The value of them can either be
'static' or 'dynamic'. If static, after parsing, the application can directly
call functions in this class to get the data. Otherwise, functions used dynamic
data need to be overridden by children classes or exception will be raised.
Note: namespace_strategy will apply to both namespace_maps and db_schema_maps.

"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import logging
import string
import copy
import imp
import os
import types
import datetime

#Deprecated in Python 2.4
#from _xmlplus.sax import saxutils
#from _xmlplus.sax import make_parser
#from _xmlplus.sax.handler import feature_namespaces
from xml.sax import make_parser
from xml.sax.handler import ContentHandler, feature_namespaces

import util.logger.Logger as Logger
import util.Trace as Trace

import proof.ProofException as ProofException
import proof.mapper.DatabaseMap as DatabaseMap
import proof.mapper.TableMap as TableMap
import proof.mapper.ColumnMap as ColumnMap
import proof.adapter.AdapterFactory as AdapterFactory
import proof.pk.generator.IDGeneratorFactory as IDGeneratorFactory

# constants
STRATEGY_STATIC  = 'static'
STRATEGY_DYNAMIC = 'dynamic'
STRATEGY_VALUES = [ STRATEGY_STATIC,
                    STRATEGY_DYNAMIC,
                    ]

DEFAULT_STRATEGY = STRATEGY_DYNAMIC


class ProofResource:

    def __init__( self,
                  config_filename,
                  logger = None ):

        self.__config_filename = config_filename
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write

        self.__name__    = ''
        self.__id__      = ''
        self.__version__ = ''
        self.__date__    = ''
        
        # these flags only used in this class
        self.__adapter_strategy   = DEFAULT_STRATEGY
        self.__schema_strategy    = DEFAULT_STRATEGY
        self.__namespace_strategy = DEFAULT_STRATEGY
        self.__object_strategy    = DEFAULT_STRATEGY
        self.__aggregate_strategy = DEFAULT_STRATEGY

        self.proof_path        = ''
        self.default_schema    = ''
        self.default_namespace = ''

        self.database_maps  = {}
        self.adapter_maps   = {}
        self.object_maps    = {}
        self.aggregate_maps = {}
        self.namespace_maps = {}
        self.db_schema_maps = {}

        self.init()

    def init(self, config_filename=None):
        """ Parse the config file and initialize data from this file.
            It won't remove existed data, but do overwrite them. This
            makes the config data come from multiple files possible. 
        """
        if config_filename:
            self.__config_filename = config_filename

        if self.__config_filename[-4:] == '.xml':
            self.__parseXMLConfig(self.__config_filename)
        elif self.__config_filename[-3:] == '.py':
            self.__parsePyConfig(self.__config_filename)
        else:
            raise ProofException.ProofImproperUseException( \
                "ProofResource.init: config file can only be .xml or .py file." )

    #==================== Interfaces ==========================

    def getProofPath(self):
        return self.proof_path

    def getSchemaKey(self, database=None):
        if self.__namespace_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getSchemaName: need to be overrided." )

        if not database:
            return self.getDefaultSchema()
            
        if self.db_schema_maps.has_key(database):
            return self.db_schema_maps[database][0]
        else:
            if self.__schema_strategy == STRATEGY_DYNAMIC:
                raise ProofException.ProofNotImplementedException( \
                    "ProofResource.getSchemaName: need to be overrided." )
            if database in self.database_maps.keys():
                return database

        raise ProofException.ProofResourceFailure( "Can't find schema for database %s." \
                                                   % (database) )

    getSchemaName = getSchemaKey

    def getDatabaseName(self, schema=None, namespace=None):
        if self.__namespace_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getDatabaseName: need to be overrided." )

        schema = self.getSchemaName(schema)

        if not namespace:
            namespace = self.getDefaultNameSpace()
        
        try:
            return self.namespace_maps[schema][namespace]['dbname']
        except:
            raise ProofException.ProofResourceFailure( "Can't find namespace %s in schema %s." \
                                                       % (namespace, schema) )

    def getDefaultSchema(self):
        if self.__schema_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getDefaultSchema: need to be overrided." )
        
        return self.default_schema

    def getDefaultNameSpace(self):
        if self.__namespace_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getDefaultNameSpace: need to be overrided." )
        
        return self.default_namespace

    def getDefaultDatabase(self, namespace=None):
        if not namespace:
            namespace = self.getDefaultNameSpace()
        return self.getDatabaseName( self.getDefaultSchema(), namespace )

    def getAdapter(self, database=None):
        if self.__adapter_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getAdapter: need to be overrided." )

        schema = self.getSchemaName(database)
        if schema in self.adapter_maps.keys():
            return self.adapter_maps[schema]

        raise ProofException.ProofResourceFailure( "Can't find adapter for %s." \
                                                   % (database) )

    def getDatabaseMap(self, database):
        if self.__schema_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getDatabaseMap: need to be overrided." )

        schema = self.getSchemaName(database)
        if schema in self.database_maps.keys():
            return self.database_maps[schema]
        
        raise ProofException.ProofResourceFailure( "Can't find DatabaseMap for %s." \
                                                   % (database) )
    
    def getDBConf(self, database=None, namespace=None):
        if self.__namespace_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getDBConf: need to be overrided." )

        schema = self.getSchemaName(database)
        if not namespace:
            namespace = self.getDefaultNameSpace()

        try:
            return self.namespace_maps[schema][namespace]
        except:
            raise ProofException.ProofResourceFailure( "Can't find DBConf for ('%s'=>'%s', '%s')." \
                                                       % (database, schema, namespace) )
    
    def getNameSpaceName(self, database=None):
        if self.__namespace_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getNameSpaceName: need to be overrided." )

        if not database:
            return self.getDefaultNameSpace()

        if self.db_schema_maps.has_key(database):
            return self.db_schema_maps[database][1]

        raise ProofException.ProofResourceFailure( "Can't find namespace for database %s." \
                                                   % (database) )

    def hasNameSpace(self, namespace):
        if self.__namespace_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.hasNameSpace: need to be overrided." )

        schemas = self.namespace_maps.keys()
        if schemas:
            return self.namespace_maps[schemas[0]].has_key(namespace)

        return False

    def getClassForObject(self, database, table):
        if self.__object_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getClassForObject: need to be overrided." )
        
        schema = self.getSchemaName(database)
        try:
            return self.object_maps[schema][table]['class']
        except:
            raise ProofException.ProofResourceFailure( "Can't find object class for (%s, %s)." \
                                                           % (database, table) )

    def getModuleForObject(self, database, table):
        if self.__object_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getModuleForObject: need to be overrided." )

        schema = self.getSchemaName(database)
        try:
            return self.object_maps[schema][table]['module']
        except:
            raise ProofException.ProofResourceFailure( "Can't find object module for (%s, %s)." \
                                                       % (database, table) )

    def getClassForAggregate(self, database, table):
        if self.__aggregate_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getClassForAggregate: need to be overrided." )

        schema = self.getSchemaName(database)
        try:
            return self.aggregate_maps[schema][table]['class']
        except:
            raise ProofException.ProofResourceFailure( "Can't find aggregate class for (%s, %s)." \
                                                       % (database, table) )

    def getModuleForAggregate(self, database, table):
        if self.__aggregate_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getModuleForAggregate: need to be overrided." )

        schema = self.getSchemaName(database)
        try:
            return self.aggregate_maps[schema][table]['module']
        except:
            raise ProofException.ProofResourceFailure( "Can't find aggregate module for (%s, %s)." \
                                                       % (database, table) )

    def getClassForFactory(self, database, table):
        if self.__object_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getClassForFactory: need to be overrided." )

        schema = self.getSchemaName(database)
        try:
            return self.object_maps[schema][table]['factoryclass']
        except:
            raise ProofException.ProofResourceFailure( "Can't find object factory class for (%s, %s)." \
                                                       % (database, table) )

    def getModuleForFactory(self, database, table):
        if self.__object_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getModuleForFactory: need to be overrided." )

        schema = self.getSchemaName(database)
        try:
            return self.object_maps[schema][table]['factorymodule']
        except:
            raise ProofException.ProofResourceFailure( "Can't find object factory module for (%s, %s)." \
                                                       % (database, table) )
        
    def getClassForAggregateFactory(self, database, table):
        if self.__aggregate_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getClassForAggregateFactory: need to be overrided." )

        schema = self.getSchemaName(database)
        try:
            return self.aggregate_maps[schema][table]['factoryclass']
        except:
            raise ProofException.ProofResourceFailure( "Can't find aggregate factory class for (%s, %s):\n%s" \
                                                       % (database, table, Trace.traceBack()) )

    def getModuleForAggregateFactory(self, database, table):
        if self.__aggregate_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getModuleForAggregateFactory: need to be overrided." )

        schema = self.getSchemaName(database)
        try:
            return self.aggregate_maps[schema][table]['factorymodule']
        except:
            raise ProofException.ProofResourceFailure( "Can't find aggregate factory module for (%s, %s):\n%s" \
                                                       % (database, table, Trace.traceBack()) )
        
    def getClassForRepository(self, database, table):
        if self.__aggregate_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getClassForRepository: need to be overrided." )

        schema = self.getSchemaName(database)
        try:
            return self.aggregate_maps[schema][table]['repositoryclass']
        except:
            raise ProofException.ProofResourceFailure( "Can't find aggregate repositoryclass for (%s, %s):\n%s" \
                                                       % (database, table, Trace.traceBack()) )
        
    def getModuleForRepository(self, database, table):
        if self.__aggregate_strategy == STRATEGY_DYNAMIC:
            raise ProofException.ProofNotImplementedException( \
                "ProofResource.getModuleForRepository: need to be overrided." )

        schema = self.getSchemaName(database)
        try:
            return self.aggregate_maps[schema][table]['repositorymodule']
        except:
            raise ProofException.ProofResourceFailure( "Can't find aggregate repositorymodule for (%s, %s):\n%s" \
                                                       % (database, table, Trace.traceBack()) )


    def __parseXMLConfig(self, config):
        """ Parse XML config file.

            @param config The xml config filename.
        """
        # Create a parser
        parser = make_parser()
    
        # Tell the parser we are not interested in XML namespaces
        parser.setFeature(feature_namespaces, 0)

        # Create the handler
        dh = XMLResourceHandler()
    
        # Tell the parser to use our handler
        parser.setContentHandler(dh)
        
        # Parse the input
        parser.parse(config)

        # get resource factory
        rf = dh.getResourceFactory()

        self.__initData(rf)
    
    def __parsePyConfig(self, config):
        """ Create resource dicts by a python config file.

            @param config The python config filename.
        """
        dh = PyResourceHandler(config)
        dh.parse()

        # get resource factory
        rf = dh.getResourceFactory()

        self.__initData(rf)

    def __initData(self, resource_factory):
        """ Initialize all dictionary by a resource factory.
        """
        resourceinfo = resource_factory.createResourceInfo()
        self.__name__    = resourceinfo.get('name','')
        self.__id__      = resourceinfo.get('id','')
        self.__version__ = resourceinfo.get('version','')
        self.__date__    = resourceinfo.get('date','')

        self.proof_path        = resourceinfo.get('proof_path','')
        self.default_schema    = resourceinfo.get('default_schema','')
        self.default_namespace = resourceinfo.get('default_namespace','')
               
        self.__adapter_strategy   = resource_factory.getStrategyFor('adapter')
        self.__schema_strategy    = resource_factory.getStrategyFor('databasemap')
        self.__namespace_strategy = resource_factory.getStrategyFor('namespace')
        self.__object_strategy    = resource_factory.getStrategyFor('object')
        self.__aggregate_strategy = resource_factory.getStrategyFor('aggregate')

        self.database_maps  = resource_factory.createDatabaseMaps()
        self.adapter_maps   = resource_factory.createAdapterMaps()
        self.object_maps    = resource_factory.createObjectMaps()
        self.aggregate_maps = resource_factory.createAggregateMaps()
        self.namespace_maps = resource_factory.createNamespaceMaps()
        self.db_schema_maps = resource_factory.createDBSchameMaps()
    


#==============================================================
# Classes used by ProofResource class

def format_dict(d, level=0):
    s = ''
    for k, v in d.items():
        s += "%s'%s' : " % ('\t'*level, k)
        if type(v) == type({}):
            s += "{\n"
            s += format_dict(v, level+1)
            s += "%s}\n" % ('\t'*(level+1))
        else:
            s += "'%s',\n" % (v)
    return s

class ResourceFactory:
    def __init__(self):
        self.resource = {}
        self.strategies = {}
        self.adapters = {}
        self.databasemaps = {}
        self.namespaces = {}
        self.objects = {}
        self.aggregates = {}

    def createDatabaseMaps(self):
        adapter_maps = self.createAdapterMaps()

        idgf = IDGeneratorFactory.IDGeneratorFactory()

        database_maps = {}
        for schema in self.databasemaps.keys():
            database_map = DatabaseMap.DatabaseMap(schema)
            adapter = adapter_maps[schema]
            id_generator = idgf.create(adapter)
            database_map.addIdGenerator(adapter.getIDMethodType(), id_generator)
            for table in self.databasemaps[schema].keys():
                table_map = TableMap.TableMap(table, database_map)
                table_map.setPrimaryKeyMethod(adapter.getIDMethodType())
                for column in self.databasemaps[schema][table].keys():
                    stype = string.lower(self.databasemaps[schema][table][column]['type'])
                    ctype = self.__sql2py_type(stype)

                    pk   = string.lower(self.databasemaps[schema][table][column]['pk'])
                    if pk == 'true':
                        pk = True
                    else:
                        pk = False

                    fkTable = self.databasemaps[schema][table][column]['fktable']
                    if string.lower(fkTable) == 'none':
                        fkTable = None
                    
                    fkColumn = self.databasemaps[schema][table][column]['fkcolumn']
                    if string.lower(fkColumn) == 'none':
                        fkColumn = None
                    
                    size = int(self.databasemaps[schema][table][column]['size'])

                    notnull = string.lower(self.databasemaps[schema][table][column]['notnull'])
                    if notnull == 'false':
                        notnull = False
                    else:
                        notnull = True

                    column_map = ColumnMap.ColumnMap( column,
                                                      table_map,
                                                      type = ctype,
                                                      pk = pk,
                                                      notNull = notnull,
                                                      fkTable = fkTable,
                                                      fkColumn = fkColumn,
                                                      size = size )

                    table_map.addColumnMap(column_map)

                    if stype == 'timestamp':
                        table_map.setTimestampColumn(column_map)

                # end of column loop
                database_map.addTable(table_map)

            # end of table loop
            database_maps[schema] = database_map

        # end of schema loop
        return database_maps

    def createAdapterMaps(self):
        adapter_maps = {}

        af = AdapterFactory.AdapterFactory()
        for schema in self.adapters.keys():
            adapter_maps[schema] = af.create(self.adapters[schema])

        return adapter_maps

    def createObjectMaps(self):
        object_maps = {}

        for schema in self.objects.keys():
            object_maps[schema] = {}
            for table in self.objects[schema].keys():
                object_maps[schema][table] = {}
                object_maps[schema][table]['module'] = self.objects[schema][table]['module']
                object_maps[schema][table]['class'] = self.objects[schema][table]['class']
                object_maps[schema][table]['factorymodule'] = self.objects[schema][table]['factorymodule']
                object_maps[schema][table]['factoryclass'] = self.objects[schema][table]['factoryclass']

        return object_maps
    
    def createAggregateMaps(self):
        aggregate_maps = {}

        for schema in self.aggregates.keys():
            aggregate_maps[schema] = {}
            for table in self.aggregates[schema].keys():
                aggregate_maps[schema][table] = {}
                aggregate_maps[schema][table]['module'] = self.aggregates[schema][table]['module']
                aggregate_maps[schema][table]['class'] = self.aggregates[schema][table]['class']
                aggregate_maps[schema][table]['factorymodule'] = self.aggregates[schema][table]['factorymodule']
                aggregate_maps[schema][table]['factoryclass'] = self.aggregates[schema][table]['factoryclass']
                aggregate_maps[schema][table]['repositorymodule'] = self.aggregates[schema][table]['repositorymodule']
                aggregate_maps[schema][table]['repositoryclass'] = self.aggregates[schema][table]['repositoryclass']

        return aggregate_maps

    def createNamespaceMaps(self):
        namespace_maps = {}

        for schema in self.namespaces.keys():
            namespace_maps[schema] = {}
            for name in self.namespaces[schema].keys():
                namespace_maps[schema][name] = {}
                namespace_maps[schema][name]['dbname'] = self.namespaces[schema][name]['database']
                namespace_maps[schema][name]['host'] = self.namespaces[schema][name]['host']
                namespace_maps[schema][name]['username'] = self.namespaces[schema][name]['username']
                namespace_maps[schema][name]['password'] = self.namespaces[schema][name]['password']

        return namespace_maps

    def createDBSchameMaps(self):
        db_schema_maps = {}

        for schema in self.namespaces.keys():
            for name in self.namespaces[schema].keys():
                db_schema_maps[self.namespaces[schema][name]['database']] = [schema,name]

        return db_schema_maps        

    def createResourceInfo(self):
        return self.resource

    def getStrategyFor(self, name):
        return self.strategies.get(name, DEFAULT_STRATEGY)
    
    def __str__(self):
        return format_dict( { 'resource'     : self.resource,
                              'strategies'   : self.strategies,
                              'adapters'     : self.adapters,
                              'databasemaps' : self.databasemaps,
                              'namespaces'   : self.namespaces,
                              'objects'      : self.objects,
                              'aggregates'   : self.aggregates } )
        
    def __sql2py_type(self, sql_type):
 
        if sql_type in [ 'tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint' ]:
            return types.IntType
        elif sql_type in [ 'float', 'double', 'real' ]:
            return types.FloatType
        elif sql_type in [ 'decimal', 'dec', 'numeric', 'fixed' ]:
            return types.FloatType
        elif sql_type in [ 'char', 'varchar', 'tinyblob', 'tinytext', 'blob', 'text',
                           'mediumblob', 'mediumtext', 'longblob', 'longtext' ]:
            return types.StringType
        elif sql_type in [ 'enum', 'set' ]:
            return types.StringType
        elif sql_type in [ 'bit', 'bool', 'boolean' ]:
            return types.BooleanType
        elif sql_type in [ 'datetime', 'timestamp' ]:
            return datetime.datetime
        elif sql_type in [ 'date' ]:
            return datetime.date
        elif sql_type in [ 'time' ]:
            return datetime.timedelta
        else:
            return None


class XMLResourceHandler(ContentHandler):
    def __init__(self):
        # section indicators
        self.__KNOWN_SECTIONS = [ 'resource', 'strategies', 'schema', 'namespaces',
                                  'tables', 'columns', 'objects', 'aggregates' ]
        self.__section = []

        # temp value used for parsing values
        self.__value = ''
        self.__current_schema = ''
        self.__current_namespace = ''
        self.__current_table = ''
        self.__current_column = ''
        self.__current_object = ''
        self.__current_aggregate = ''
        self.__current_dict = {}

        # real values parsed from XML
        self.__resource_factory = ResourceFactory()
    
    def startElement(self, name, attrs):
        self.__value = ''
        if name in self.__KNOWN_SECTIONS:
            self.__section.append( name )
            if name == 'schema':
                self.__current_schema = attrs['name'].encode('ascii', 'ignore')
                self.__resource_factory.databasemaps[self.__current_schema] = {}
                self.__resource_factory.namespaces[self.__current_schema] = {}
                self.__resource_factory.objects[self.__current_schema] = {}
                self.__resource_factory.aggregates[self.__current_schema] = {}
        else:
            section = string.join(self.__section)
            if section == 'resource schema namespaces':
                if name == 'namespace':
                    self.__current_namespace = attrs['name'].encode('ascii', 'ignore')
                    self.__current_dict = {}
            elif section == 'resource schema tables':
                if name == 'table':
                    self.__current_table = attrs['name'].encode('ascii', 'ignore')
                    self.__resource_factory.databasemaps[self.__current_schema][self.__current_table] = {}
            elif section == 'resource schema tables columns':
                if name == 'column':
                    self.__current_column = attrs['name'].encode('ascii', 'ignore')
                    self.__current_dict = {}
            elif section == 'resource schema objects':
                if name == 'object':
                    self.__current_object = attrs['name'].encode('ascii', 'ignore')
                    self.__current_dict = {}
            elif section == 'resource schema aggregates':
                if name == 'aggregate':
                    self.__current_aggregate = attrs['name'].encode('ascii', 'ignore')
                    self.__current_dict = {}
                   
    def characters(self, ch):
        if string.strip(ch) == '':
            return

        self.__value += ch

    def endElement(self, name):
        if name in self.__KNOWN_SECTIONS:
            self.__section.pop()
        else:
            section = string.join(self.__section)
            if section == 'resource':
                self.__resource_factory.resource[name] = str(self.__value)
            elif section == 'resource strategies':
                self.__resource_factory.strategies[name] = str(self.__value)
            elif section == 'resource schema':
                if name == 'adapter':
                    self.__resource_factory.adapters[self.__current_schema] = self.__value
            elif section == 'resource schema namespaces':
                if name == 'namespace':
                    self.__resource_factory.namespaces[self.__current_schema][self.__current_namespace] = \
                                            copy.copy(self.__current_dict)
                    self.__current_dict = {}
                else:
                    self.__current_dict[name] = str(self.__value)
            elif section == 'resource schema tables columns':
                if name == 'column':
                    self.__resource_factory.databasemaps[self.__current_schema][self.__current_table][self.__current_column] = \
                                            copy.copy(self.__current_dict)
                    self.__current_dict = {}
                else:
                    self.__current_dict[name] = str(self.__value)
            elif section == 'resource schema objects':
                if name == 'object':
                    self.__resource_factory.objects[self.__current_schema][self.__current_object] = \
                                            copy.copy(self.__current_dict)
                    self.__current_dict = {}
                else:
                    self.__current_dict[name] = str(self.__value)
            elif section == 'resource schema aggregates':
                if name == 'aggregate':
                    self.__resource_factory.aggregates[self.__current_schema][self.__current_aggregate] = \
                                            copy.copy(self.__current_dict)
                    self.__current_dict = {}
                else:
                    self.__current_dict[name] = str(self.__value)

        if self.__value:
            self.__value = ''

    def getResourceFactory(self):
        return self.__resource_factory


class PyResourceHandler:

    def __init__(self, configfile):
        self.__configfile = configfile
        self.__resource_factory = ResourceFactory()

    def parse(self):
        path = None
        hi = string.rfind(self.__configfile, '/')
        path = os.getcwd()
        if hi >= 0:
            path = os.path.join( path, self.__configfile[:hi+1] )
            filename = self.__configfile[hi+1:]
        else:
            filename = self.__configfile

        mname = filename.replace('.py', '')

        try:
            f = open(self.__configfile)
            module = imp.load_source(mname, path, f)
        finally:
            if f: f.close()

        if not module:
            raise ProofException.ProofResourceFailure( \
                "Can't import the config file %s." % (self.__configfile) )

        self.__resource_factory.resource = {
            'name'              : module.__name__,
            'id'                : module.__id__,
            'version'           : module.__version__,
            'date'              : module.__date__,
            'proof_path'        : module.proof_path,
            'default_schema'    : module.default_schema,
            'default_namespace' : module.default_namespace,
            }

        self.__resource_factory.strategies   = module.strategies
        self.__resource_factory.adapters     = module.adapters
        self.__resource_factory.namespaces   = module.namespaces
        self.__resource_factory.databasemaps = module.databasemaps
        self.__resource_factory.objects      = module.objects
        self.__resource_factory.aggregates   = module.aggregates
        

    def getResourceFactory(self):
        return self.__resource_factory
