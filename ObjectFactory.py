"""
This is the base factory class for all factory classes in the system. Factory
classes are responsible for isolating all of the database access for all specific
business objects. They execute all of the SQL against the database. Over time
this class has grown to include utility methods which ease execution of
cross-database queries and the implementation of concrete Factories --
AggregateFactory.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import logging

from util.Import import my_import
import util.UniqueList as UniqueList

import proof.ProofException as ProofException
import proof.BaseFactory as BaseFactory
import proof.pk.ObjectKey as ObjectKey
import proof.pk.ComboKey as ComboKey
import proof.Aggregate as Aggregate


class ObjectFactory(BaseFactory.BaseFactory):
    
    def __init__( self,
                  aggregate,
                  schema_name = "SCHEMA_NAME",
                  table_name  = "TABLE_NAME",
                  logger      = None,
                  ):
        """ Constructor.
        """
        assert issubclass(aggregate.__class__, Aggregate.Aggregate)

        self.__aggregate   = aggregate
        self.__table_name  = table_name

        if not self.__aggregate.hasObject(self.__table_name):
            raise ProofException.ProofImproperUseException( \
                    "%s: table_name '%s' has to be an object in '%s'." % \
                    ( self.__class__.__name__,
                      self.__table_name,
                      self.__aggregate.__class__.__name__ ) )
        
        BaseFactory.BaseFactory.__init__( self,
                                          aggregate.getProofInstance(),
                                          schema_name,
                                          logger )
        # select columns
        self.__as_columns = {}

        # whether self.init_select() called
        self.__initialized = 0

    def initialize(self):
        """ Initialize required select and where fields for the aggregate.
        """
        self.__as_columns.clear()
        
        db_map = self.__aggregate.getProofInstance().getDatabaseMap(self.getDBName())
        table_map = db_map.getTable(self.__table_name)
        column_maps = table_map.getColumns()
        for column_map in column_maps:
            column_name = column_map.getFullyQualifiedName()
            self.__as_columns[column_name] = column_name

        self.__initialized = 1

    def isInitialized(self):
        return self.__initialized

    #================= Functions to do database queries ==================

        
    # DELETE
    #===========

    def deleteAll( self,
                   column,
                   value
                   ):
        """ Delete multiple rows where column has value.

            @param column The column in the where clause.
            @param value The value of the column.

            @deprecated delete operation should be handled in BaseObject or
            Aggregate objects to make object consistency easier in Repository.
        """
        return BaseFactory.BaseFactory.deleteAll( self,
                                                  column,
                                                  value,
                                                  table = self.__table_name )

    # INSERT
    #===========

    def doInsert( self, criteria ):
        """ Method to perform inserts based on values and keys in a
            Criteria.

            @param criteria Object containing values to insert.
            @return An Object which is the id of the row that was inserted
            (if the table has a primary key) or None (if the table does not
            have a primary key).
        """
        if not self.__isSameTable(criteria):
            raise ProofException.ProofImproperUseException( \
                    "%s.doInsert: only.insert into %s is permitted." % \
                    (self.__class__.__name__, self.__table_name))

        return BaseFactory.BaseFactory.doInsert(self, criteria)

    # SELECT
    #===========

    def doSelectSingleObject(self, criteria):
        """ Returns single result.
        
            @param criteria A Criteria.
            @return A single object or None.
        """
        results = self.doSelectObject(criteria)
        if results: return results[0]
        return None
        
    def doSelectObject(self, criteria):
        """ Returns all results.
        
            @param criteria A Criteria.
            @return A list of objects.
        """
        if not self.isInitialized():
            self.initialize()

        # add AS columns
        criteria.setSelectColumns(UniqueList.UniqueList())
        criteria.setAsColumns(self.__as_columns)

        results = self.doSelect(criteria, ret_dict=1)
        return self.constructObjects(results)
        
    def constructObjects(self, rows):
        """ Convert a list of rows to a list of Objects.

            @param rows A list of rows from the table this Factory represents.
            @return A list of Objects this Factory represents.
        """
        results = []
        #self.log("Rows: %s"%(rows,), level=logging.INFO)
        if rows:
            for row in rows:
                results.append(self.constructObject(row))
        return results

    def constructObject(self, row):
        """ Convert a row to an Object.

            @param rows A rows from the table this Factory represents.
            @return An Object this Factory represents.
        """
        # create the pk object
        proof       = self.__aggregate.getProofInstance()
        db_map      = proof.getDatabaseMap(self.getDBName())
        table_map   = db_map.getTable(self.__table_name)
        column_maps = table_map.getColumns()
        pk_list     = []
        for column in column_maps:
            column_name = column.getFullyQualifiedName()
            if column.isPrimaryKey() and \
                   row.has_key(column_name):
                pk_list.append( ObjectKey.ObjectKey( row[column_name],
                                                     column_name ) )
        pk = None
        if pk_list:
            if len(pk_list) == 1:
                pk = pk_list[0]
            else:
                pk = ComboKey.ComboKey(pk_list)

        obj = None
        if pk:
            # assume the tablename is the object name
            obj = self.__aggregate.getObject(self.__table_name, pk)
            
            if not obj:
                # create the object
                module_name = proof.getModuleForObject(self.__table_name, schema=self.getSchemaName())
                class_name  = proof.getClassForObject(self.__table_name, schema=self.getSchemaName())
                object_module = my_import(module_name)
                obj = getattr(object_module, class_name)
                obj = obj(self.__aggregate, pk, logger=self.getLogger())
                obj.initialize(row)
        else:
            self.log("%s: no pk was found in '%s'!" % (self.__class__.__name__, row), logging.WARNING)
        
        return obj

    def getPrimaryKey(self):
        """ Helper method which returns the primary key contained
            in the given Criteria object.
            
            @return ColumnMap if the Criteria object contains a primary
                     key, or None if it doesn't.
        """
        proof     = self.__aggregate.getProofInstance()
        db_map    = proof.getDatabaseMap(self.getDBName())
        table_map = db_map.getTable(self.__table_name)
        columns   = table_map.getColumns()

        pk = None
        for column in columns:
            if column.isPrimaryKey():
                pk = column
                break

        return pk

    # UPDATE
    #===========

    def doUpdate(self, update_criteria, where_criteria=None):
        """ Convenience method used to update rows in the DB. Checks if a
            <i>single</i> int primary key is specified in the Criteria
            object and uses it to perform the udpate. If no primary key is
            specified an Exception will be thrown.
            <p>
            Use this method for performing an update of the kind:
            <p>
            "WHERE primary_key_id = an int"
            <p>
            To perform an update with non-primary key fields in the WHERE
            clause use doUpdate(criteria, criteria).

            @param update_criteria A Criteria object containing values used in
                    set clause.
            @param where_criteria A Criteria object containing values used in
                    where clause. If none, it will try to use pk in update_criteria.
        """
        if self.__isSameTable(update_criteria) and \
               self.__isSameTable(where_criteria):
            result = BaseFactory.BaseFactory.doUpdate(self, update_criteria, where_criteria)
            #self.log("doUpdate table name: %s"%(self.__table_name), logging.INFO)
            #self.log("doUpdate result: %s"%(result), logging.INFO)
            if result:
                return result[self.__table_name]
            else:
                return 0
        else:
            raise ProofException.ProofImproperUseException( \
                    "%s.doUpdate: only %s update is permitted." % \
                    (self.__class__.__name__, self.__table_name))

    #=====================================================================

    def __isSameTable(self, criteria):
        """ Check whether the criteria only contains this table name.
        """
        if criteria:
            for key in criteria.keys():
                if criteria.getTableName(key) != self.__table_name:
                    return False
        return True
