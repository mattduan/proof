"""
This is the base aggregate factory class for all aggregate factory classes in
the system. AggregateFactory is the place all aggregates are created.
"""

__version__='$Revision: 3220 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import string
import logging

try:
    import thread as _thread
except (ImportError, AttributeError):
    import dummy_thread as _thread

import util.UniqueList as UniqueList
from util.Import import my_import

import proof.pk.ObjectKey as ObjectKey
import proof.pk.ComboKey as ComboKey
import proof.Aggregate as Aggregate
import proof.BaseFactory as BaseFactory
import proof.ProofException as ProofException
import proof.sql.Criteria as Criteria
import proof.sql.SQLConstants as SQLConstants


class AggregateFactory(BaseFactory.BaseFactory):

    def __init__( self,
                  proof_instance,
                  schema_name = "SCHEMA_NAME",
                  root_name   = "TABLE_NAME",
                  logger      = None ):
        """ Constructor.

            @param proof_instance The instance of proof.
            @param schema_name The schema name.
            @param root_name The root object table name.
            @param logger The logger object.
        """
        BaseFactory.BaseFactory.__init__( self,
                                          proof_instance,
                                          schema_name,
                                          logger )

        # root table name
        self.__root_name = root_name

        # all table names
        self.__repository = proof_instance.getInstanceForRepository( self.__root_name,
                                                                     schema = schema_name )
        self.__relation_map = self.__repository.getRelationMap()
        self.__tables = self.__relation_map.keys()

        if self.__root_name not in self.__tables:
            self.__tables.append(self.__root_name)
        
        # select columns
        self.__as_columns = {}

        # primary key columns of the root table
        self.__pk_columns = []

        # timestamp column
        self.__timestamp_column = None

        # whether self.init_select() called
        self.__initialized = 0
    
    def initialize(self):
        """ Initialize required select and where fields for the aggregate.
        """
        self.log( "start %s initialize" % (self.__class__.__name__) )
        
        self.__as_columns.clear()
        self.__pk_columns = []

        db_map      = self.getProofInstance().getDatabaseMap(self.getDBName())
        table_map   = db_map.getTable(self.__root_name)
        column_maps = table_map.getColumns()
        for column_map in column_maps:
            column_name = column_map.getFullyQualifiedName()
            self.__as_columns[column_name] = column_name
            if column_map.isPrimaryKey():
                # primary key list
                self.__pk_columns.append( column_map )

        self.__timestamp_column = table_map.getTimestampColumn()

        self.log( "finish %s initialize" % (self.__class__.__name__) )
        self.__initialized = 1

    def isInitialized(self):
        return self.__initialized

    # INSERT
    #===========

    def create(self, *args, **kwargs):
        raise ProofException.ProofNotImplementedException( \
            "%s.create(): need to be overrided by subclass." % (self.__class__.__name__) )

    def _create(self, data):
        """ Insert one object record into database.

            @param data A dictionary. It should only contain data for one table.
                        For example: {  'Table.Column1' : 'somedata',
                                        'Table.Column2' : 'somedata', }
                        The key has to be 'Table_Name.Column_Name' format.
        """
        # check data is valid
        table = UniqueList.UniqueList( [key.split('.')[0] for key in data.keys()] )
        if len(table) != 1:
            raise ProofException.ProofImproperUseException( \
                "%s._create(): arg data should only contain one table, but get '%s'" % \
                (self.__class__.__name__, str(table)) )

        if not table[0] in self.__tables:
            raise ProofException.ProofImproperUseException( \
                "%s._create(): table '%s' doesn't belong to the aggregate '%s'" % \
                (self.__class__.__name__, table[0], self.__root_name) )

        # construct insert criteria
        criteria = Criteria.Criteria( self.getProofInstance(),
                                      db_name     = self.getDBName(),
                                      logger      = self.getLogger() )
        for key in data.keys():
            #self.log( "insert add '%s' => '%s'" % (key, data[key]) )
            criteria.add( key, data[key] )

        return self.doInsert( criteria )
        
    def _makeObject(self, aggregate, object_name, pk):
        """ A common function used to make an Object with the pk specified.

            @return An Object object.
        """
        if object_name in self.__tables:
            # if object exists, return it
            if object_name == aggregate.getRootName():
                root = aggregate.getRoot()
                if root:
                    return root
            else:
                obj = aggregate.getObject(object_name, pk)
                if obj:
                    return obj

            # create a new object
            proof = self.getProofInstance()
            module_name = proof.getModuleForObject(object_name, schema=self.getSchemaName())
            class_name  = proof.getClassForObject(object_name, schema=self.getSchemaName())
            object_module = my_import(module_name)
            obj = getattr(object_module, class_name)
            return obj(aggregate, pk, logger=self.getLogger())

        return None
    
    # SELECT
    #===========

    def doSelectSingleAggregate(self, criteria):
        """ Returns single aggregate.
        
            @param criteria A Criteria.
            @return A single aggregate or None.
        """
        aggregates = self.doSelectAggregate(criteria)
        if aggregates: return aggregates[0]
        return None

    def doSelectAggregate(self, criteria):
        """ Returns a list of aggregates.
        
            @param criteria A Criteria.
            @return A list of aggregates.
        """
        self.log( "start %s doSelectAggregate" % (self.__class__.__name__) )

        finger_print = criteria.__finger_print__()
        
        #  check repository thread session cached result
        cached = self.__repository.get_thread_session_query_result(finger_print)

        if cached != None:
            return cached
        
        if not self.isInitialized():
            self.initialize()

        criteria.setSelectColumns(UniqueList.UniqueList())
        criteria.setDistinct()
        criteria.setAsColumns(self.__as_columns)

        results = self.doSelect(criteria, ret_dict=1)

        aggregates = self.constructAggregates(results)

        # add results to thread session
        self.__repository.add_thread_session_query_result(finger_print, aggregates)
        
        return aggregates

    def constructAggregates(self, rows):
        results = []
        if rows and type(results)==type([]):
            for row in rows:
                aggr = self.constructAggregate(row)
                if aggr:
                    results.append(aggr)
        return results

    def constructAggregate(self, row):
        #self.log( "start %s constructAggregate" % (self.__class__.__name__) )
        
        if not self.isInitialized():
            self.initialize()

        # construct root object
        root_pk  = None
        if len(self.__pk_columns) == 1:
            column_map = self.__pk_columns[0]
            column_name = column_map.getFullyQualifiedName()
            column_value = row[column_name]
            # primary key can't be None
            if column_value == None:
                return None
            root_pk = ObjectKey.ObjectKey(column_value, column_name)
        elif len(self.__pk_columns) > 1:
            pk_list = []
            for pkc in self.__pk_columns:
                column_name = pkc.getFullyQualifiedName()
                column_value = row[column_name]
                pk_list.append(ObjectKey.ObjectKey(column_value, column_name))
            root_pk = ComboKey.ComboKey(pk_list)

        if not root_pk:
            return None

        #self.log( "found root pk" )
        
        aggregate = self.__repository.get(root_pk)

        self.log( "Get aggregate '%s'." % (aggregate) )
        
        # check timestamp column value
        timestamp_column_name  = None
        timestamp_column_value = None
        if self.__timestamp_column:
            timestamp_column_name  = self.__timestamp_column.getColumnName()
            timestamp_column_value = row.get("%s.%s" % (self.__root_name, timestamp_column_name), None)

        if aggregate and timestamp_column_name and timestamp_column_value and \
               hasattr(aggregate, "get%s" % (timestamp_column_name)):
            func = getattr(aggregate, "get%s" % (timestamp_column_name))
            old_timestamp = func()
            if not old_timestamp or old_timestamp < timestamp_column_value:
                self.log( "Aggregate '%s' is out-of-date." % (aggregate) )
                aggregate = None
        
        if not aggregate:
            # create a new aggregate
            self.log( "create a new aggregate" )
            proof = self.getProofInstance()
            module_name = proof.getModuleForAggregate(self.__root_name, schema=self.getSchemaName())
            class_name  = proof.getClassForAggregate(self.__root_name, schema=self.getSchemaName())
            aggre_module = my_import(module_name)
            obj = getattr(aggre_module, class_name)
            aggregate = obj(proof, root_pk, logger=self.getLogger())

            # set root object
            #self.log( "set root object" )
            root = self._makeObject(aggregate, self.__root_name, root_pk)
            root.initialize(row)
            aggregate.setRootObject(root)

            # load all related objects
            aggregate.load_objects()

            # add it to the repository
            aggregate = self.__repository.add(aggregate)

        # add timestamp if exists
        if timestamp_column_value:
            self.__repository.add_timestamp(timestamp_column_value, aggregate)

        #self.log( "return aggregate '%s'" % (aggregate) )
        return aggregate
    

    def doTotalSelect(self, criteria):
        """ Override to add Join and Group statements.
        """
        if not self.isInitialized():
            self.initialize()
        
        # make distinct
        count_columns = UniqueList.UniqueList()
        for pkc in self.__pk_columns:
            column_name = pkc.getFullyQualifiedName()
            criteria.add(column_name, 'None', comparison=SQLConstants.NOT_EQUAL)
            count_columns.append(column_name)

        count_str = None
        if count_columns:
            count_str = "COUNT(DISTINCT %s) AS count" % (string.join(count_columns, ", "))

        return BaseFactory.BaseFactory.doTotalSelect(self, criteria, count_str)

    def doSelectAll(self, order_by=None):
        """ This should only be called in a small table. Otherwise, use
            <code>doPageSelect</code> instead.
            
            @param order_by A list of order criteria in the format:
                        [ [ 'Table.ColumnName', 'asc|desc' ], ... ].
        """
        # use an empty criteria
        criteria = Criteria.Criteria( self.getProofInstance(),
                                      db_name     = self.getDBName(),
                                      logger      = self.getLogger() )
        
        # check the feasibility
        total = self.doTotalSelect(criteria)

        limit = self.getProofInstance().getSelectAllLimit()
        criteria.setLimit(limit)

        if limit > 0 and total > limit:
            self.log( "%s.doSelectAll: %s exceeds max limit %s." % \
                      (self.__class__.__name__, total, limit),
                      logging.WARNING )
        
        if order_by and type(order_by) == type([]):
            for row in order_by:
                if len(row) == 2:
                    if string.lower(row[1]) == 'desc':
                        criteria.addDescendingOrderByColumn(row[0])
                    else:
                        criteria.addAscendingOrderByColumn(row[0])
 
        return self.doSelectAggregate(criteria)
