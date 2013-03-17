"""
The python resource config template.
"""

__name__    = 'PROOF Resource'
__id__      = 'proof_resource'
__version__ = '1.0'
__date__    = '2004-05-09'

proof_path        = ''
default_schema    = 'schema1'
default_namespace = 'mydomain1.com'

strategies = {
    'databasemap' : 'static',
    'aggregate'   : 'static',
    'adapter'     : 'static',
    'object'      : 'static',
    'namespace'   : 'static'
    }

adapters = {
    'schema1' :  'mysql',
    }

namespaces = {
    'schema1' : {
                'mydomain2.com' : {
                        'username' :  'duan',
                        'host' :  'localhost',
                        'database' :  'database2',
                        'password' :  '1234',
                        },
                'mydomain1.com' : {
                        'username' :  'duan',
                        'host' :  'localhost',
                        'database' :  'database1',
                        'password' :  '1234',
                        }
                },
    }

databasemaps = {
    'schema1' : {
                'Foo' : {
                        'Name' : {
                                'fkcolumn' :  'none',
                                'notnull' :  'true',
                                'fktable' :  'none',
                                'pk' :  'false',
                                'type' :  'varchar',
                                'size' :  '25',
                                },
                        'Foo_Id' : {
                                'fkcolumn' :  'none',
                                'notnull' :  'true',
                                'fktable' :  'none',
                                'pk' :  'true',
                                'type' :  'int',
                                'size' :  '10',
                                },
                        'Bar_Id' : {
                                'fkcolumn' :  'Bar_Id',
                                'notnull' :  'true',
                                'fktable' :  'Bar',
                                'pk' :  'false',
                                'type' :  'int',
                                'size' :  '8',
                                },
                        },
                'Bar' : {
                        'Name' : {
                                'fkcolumn' :  'none',
                                'notnull' :  'true',
                                'fktable' :  'none',
                                'pk' :  'false',
                                'type' :  'varchar',
                                'size' :  '25',
                                },
                        'Bar_Id' : {
                                'fkcolumn' :  'Bar_Id',
                                'notnull' :  'true',
                                'fktable' :  'Foo',
                                'pk' :  'true',
                                'type' :  'int',
                                'size' :  '8',
                                },
                        },
                },
    }

objects = {
    'schema1' : {
                'Foo' : {
                        'factorymodule' :  'ddl.myservice.schema1.FooFactory',
                        'factoryclass' :  'FooFactory',
                        'class' :  'Foo',
                        'module' :  'ddl.myservice.schema1.Foo',
                        },
                'Bar' : {
                        'factorymodule' :  'ddl.myservice.schema1.BarFactory',
                        'factoryclass' :  'BarFactory',
                        'class' :  'Bar',
                        'module' :  'ddl.myservice.schema1.Bar',
                        },
                },
    }

aggregates = {
    'schema1' : {
                'Foo' : {
                        'factorymodule' :  'ddl.myservice.schema1.FooAggregateFactory',
                        'repositorymodule' :  'ddl.myservice.schema1.FooAggregateRepository',
                        'factoryclass' :  'FooAggregateFactory',
                        'repositoryclass' :  'FooAggregateRepository',
                        'class' :  'FooAggregate',
                        'module' :  'ddl.myservice.schema1.FooAggregate',
                        },
                },
    }
