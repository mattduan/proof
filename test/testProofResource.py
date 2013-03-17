"""
PyUnit TestCase for ProofResource.
"""

import os
import os.path
import sys
import unittest

import proof.ProofResource as ProofResource
import proof.ProofException as ProofException
import proof.adapter.Adapter as Adapter

class testProofResource(unittest.TestCase):

    name = 'resource.xml'

    def setUp(self):
        # init ProofResource
        #print dir(self.__class__), self.__module__, os.getcwd()
        res_file = self.name
        if sys.modules.has_key(self.__module__):
            res_file = os.path.join(sys.modules[self.__module__].__path__, self.name)

        self.resource  = ProofResource.ProofResource(res_file)
    
    def tearDown(self):
        # clean up resources
        del self.resource

    def test_getProofPath(self):
        self.assertEqual(self.resource.getProofPath(), '')
        self.assert_(not self.resource.getProofPath())

    def test_getSchemaName(self):
        self.assertEqual(self.resource.getSchemaName('database1'), 'schema1')

    def test_getDatabaseName(self):
        self.assertEqual(self.resource.getDatabaseName('schema1', 'mydomain1.com'), 'database1')

    def test_getDefaultSchema(self):
        self.assertEqual(self.resource.getDefaultSchema(), 'schema1')

    def test_getDefaultDatabase(self):
        self.assertEqual(self.resource.getDefaultDatabase('mydomain1.com'), 'database1')

    def test_getDefaultNameSpace(self):
        self.assertEqual(self.resource.getDefaultNameSpace(), 'mydomain1.com')

    def test_hasNameSpace(self):
        self.assert_(self.resource.hasNameSpace('mydomain1.com'))

    def test_getAdapter(self):
        adapter = self.resource.getAdapter('schema1')
        self.assert_( issubclass(adapter.__class__, Adapter.Adapter) )
        self.assertEqual(adapter.getResourceType(), 'mysql')

    def test_getDatabaseMap(self):
        database_map1 = self.resource.getDatabaseMap('schema1')
        self.assertEqual( database_map1.__class__.__name__, 'DatabaseMap' )
        database_map2 = self.resource.getDatabaseMap('database1')
        self.assert_( database_map1 is database_map2 )
        self.assertEqual(database_map1.getName(), 'schema1')
        self.assert_( database_map1.containsTable('Foo') )
        tableBar = database_map1.getTable('Bar')
        self.assertEqual( tableBar.__class__.__name__, 'TableMap' )
        self.assertEqual( tableBar.getName(), 'Bar' )
        self.assert_( tableBar.containsColumn('Bar_Id') )
        columnBarId = tableBar.getColumn('Bar_Id')
        self.assertEqual( columnBarId.__class__.__name__, 'ColumnMap' )
        self.assertEqual( columnBarId.getTableName(), 'Bar' )
        self.assertEqual( columnBarId.getColumnName(), 'Bar_Id' )
        self.assertEqual( columnBarId.getFullyQualifiedName(), 'Bar.Bar_Id' )
        self.assert_( columnBarId.isPrimaryKey() )
        self.assert_( columnBarId.isNotNull() )
        self.assert_( columnBarId.isForeignKey() )
        self.assertEqual( columnBarId.getRelatedName(), 'Foo.Bar_Id' )

    def test_getDBConf(self):
        dbconf1 = self.resource.getDBConf('database1')
        dbconf2 = self.resource.getDBConf('schema1', 'mydomain1.com')
        self.assertEqual( dbconf1, dbconf2 )
        self.assertEqual( dbconf1['dbname'], 'database1' )
        self.assertEqual( dbconf1['host'], 'localhost' )
        self.assertEqual( dbconf1['username'], 'duan' )
        self.assertEqual( dbconf1['password'], '1234' )

    def test_getNameSpaceName(self):
        self.assertEqual( self.resource.getNameSpaceName('database1'), 'mydomain1.com' )

    def test_getClassForObject(self):
        self.assertEqual( self.resource.getClassForObject('schema1', 'Foo'),
                          'Foo' )

    def test_getModuleForObject(self):
        self.assertEqual( self.resource.getModuleForObject('schema1', 'Foo'),
                          'ddl.myservice.schema1.Foo' )

    def test_getClassForFactory(self):
        self.assertEqual( self.resource.getClassForFactory('schema1', 'Foo'),
                          'FooFactory' )

    def test_getModuleForFactory(self):
        self.assertEqual( self.resource.getModuleForFactory('schema1', 'Foo'),
                          'ddl.myservice.schema1.FooFactory' )

    def test_getClassForAggregate(self):
        self.assertEqual( self.resource.getClassForAggregate('schema1', 'Foo'),
                          'FooAggregate' )
    
    def test_getModuleForAggregate(self):
        self.assertEqual( self.resource.getModuleForAggregate('schema1', 'Foo'),
                          'ddl.myservice.schema1.FooAggregate' )

    def test_getClassForAggregateFactory(self):
        self.assertEqual( self.resource.getClassForAggregateFactory('schema1', 'Foo'),
                          'FooAggregateFactory' )

    def test_getModuleForAggregateFactory(self):
        self.assertEqual( self.resource.getModuleForAggregateFactory('schema1', 'Foo'),
                          'ddl.myservice.schema1.FooAggregateFactory' )

    def test_getClassForRepository(self):
        self.assertEqual( self.resource.getClassForRepository('schema1', 'Foo'),
                          'FooAggregateRepository' )

    def test_getModuleForRepository(self):
        self.assertEqual( self.resource.getModuleForRepository('schema1', 'Foo'),
                          'ddl.myservice.schema1.FooAggregateRepository' )

    
class testPyResource(testProofResource):
    name = 'resource.py'

class testDynResource(testProofResource):
    name = 'resource_dyn.xml'

    def test_getSchemaName(self):
        self.assertRaises( ProofException.ProofNotImplementedException,
                           self.resource.getSchemaName, 'database1' )

    def test_getDatabaseName(self):
        self.assertRaises( ProofException.ProofNotImplementedException,
                           self.resource.getDatabaseName, 'schema1', 'mydomain1.com' )

    def test_getDefaultSchema(self):
        pass

    def test_getDefaultDatabase(self):
        pass

    def test_getDefaultNameSpace(self):
        pass
    
    def test_getAdapter(self):
        self.assertRaises( ProofException.ProofNotImplementedException,
                           self.resource.getAdapter, 'schema1' )

    def test_getDatabaseMap(self):
        pass
    
    def test_getDBConf(self):
        pass

    def test_hasNameSpace(self):
        self.assertRaises( ProofException.ProofNotImplementedException,
                           self.resource.hasNameSpace, 'mydomain1.com' )
        
    def test_getNameSpaceName(self):
        self.assertRaises( ProofException.ProofNotImplementedException,
                           self.resource.getNameSpaceName, 'database1' )

    def test_getClassForObject(self):
        pass

    def test_getModuleForObject(self):
        pass

    def test_getClassForFactory(self):
        pass

    def test_getModuleForFactory(self):
        pass
    
    def test_getClassForAggregate(self):
        pass
    
    def test_getModuleForAggregate(self):
        pass

    def test_getClassForAggregateFactory(self):
        pass

    def test_getModuleForAggregateFactory(self):
        pass

    def test_getClassForRepository(self):
        pass

    def test_getModuleForRepository(self):
        pass
    
