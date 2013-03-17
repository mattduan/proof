"""
This is the base object class for all object classes in the system. An Object
class represent a row in a database table. Beside providing the attributes and
methods, it also encapsulates update and delete operations with database.
These objects are identified by their pk's. They are used by PROOF system and
shouldn't be directly accessed by applications. Aggregates will include these
objects and be directly used by external systems.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import logging
import copy

import util.logger.Logger as Logger
import util.UniqueList as UniqueList

import proof.ProofException as ProofException
import proof.pk.ObjectKey as ObjectKey
import proof.sql.Criteria as Criteria
import proof.ObjectFactory as ObjectFactory

class BaseObject:
    
    def __init__( self,
                  aggregate,
                  pk,
                  db_schema   = "DB_SCHEMA",
                  table_name  = "TABLE_NAME",
                  logger      = None ):
        """ Constructor. Initialize a Object. The aggregate this object belongs to
            has to be included. For details about aggregate, refer to Aggregate class.

            @param aggregate The aggregate instance this object belongs to.
            @param pk An ObjectKey object to identify this object.
            @param logger A logger object.
        """
        self.__aggregate   = aggregate
        self.__pk          = pk
        self.__db_schema   = db_schema
        self.__db_name     = ""
        self.__table_name  = table_name
        self.__attributes  = {}
        self.__dirty_attrs = {}
        self.__initialized = False
        self.__is_dirty    = False

        # Hold timestamp column if exists
        self.__timestamp_column = None

        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write
        self.init()

    def init(self):
        """ Initialize database name and column names.
        """
        proof = self.__aggregate.getProofInstance()
        self.__db_name = proof.getDBName(self.__db_schema)
        db_map      = proof.getDatabaseMap(self.__db_name)
        table_map   = db_map.getTable(self.__table_name)
        column_maps = table_map.getColumns()

        for column_map in column_maps:
            if not column_map.isPrimaryKey():
                self.__attributes[column_map.getColumnName()] = None

        self.__timestamp_column = table_map.getTimestampColumn()


    def isInitialized(self):
        return self.__initialized

    def setInitialized(self, initialized=True):
        self.__initialized = initialized

    def initialize(self, attrs):
        """ Intialize all attributes for this object.

            @param attrs A dictionary with column_name/value pairs.
        """
        for key in self.__attributes.keys():
            if attrs.has_key( "%s.%s" % (self.__table_name, key) ):
                self.__attributes[key] = attrs["%s.%s" % (self.__table_name, key)]
            elif attrs.has_key( key ):
                self.__attributes[key] = attrs[key]
        self.__initialized = True

    def isDirty(self):
        return self.__is_dirty
    
    def getPK(self):
        return self.__pk

    #def setPK(self, pk):
    #    assert isinstance(pk, ObjectKey.ObjectKey)
    #    self.__pk = pk
        
    def getAggregate(self):
        return self.__aggregate
        
    def getDBSchema(self):
        return self.__db_schema
    
    def getDBName(self):
        return self.__db_name
    
    def getTableName(self):
        return self.__table_name

    def get(self, key, default):
        """ Like a dict.
        """
        self.load()
        value = self.__attributes.get(key, default)
        self.__aggregate.touch()
        return value

    def getAttributes(self):
        """ Return the entire dict.
        """
        return self.__attributes

    def getDirtyAttributes(self):
        """ Return the entire dirty dict.
        """
        return self.__dirty_attrs

    def __getitem__(self, key):
        """ Like a dict.
        """
        self.load()
        try:
            value = self.__attributes.__getitem__(key)
            self.__aggregate.touch()
            return value
        except KeyError:
            raise KeyError("%s doesn't have column '%s'" % (self.__table_name, key))

    def __setitem__(self, key, value):
        """ Like a dict.
        """
        self.load()
        if self.__attributes.has_key(key):
            if value != self.__attributes[key]:
                self.__dirty_attrs[key] = value
                self.__is_dirty = True
                self.__aggregate.touch()
                self.__aggregate.updateState()         
        else:
            raise KeyError("%s doesn't have column '%s'" % (self.__table_name, key))

    def has_key(self, key):
        """ Like a dict.
        """
        return self.__attributes.has_key(key)

    def load(self):
        """ Load the attribute values.
        """
        if not self.__initialized:
            proof = self.__aggregate.getProofInstance()
            criteria = Criteria.Criteria( proof,
                                          db_name = self.__db_name,
                                          logger  = self.__logger )
            # where clause
            pk_value = self.__pk.getValue()
            if type(pk_value) == type([]):
                for pk in pk_value:
                    criteria[pk.getFullyQualifiedName()] = pk.getValue()
            else:
                criteria[self.__pk.getFullyQualifiedName()] = pk_value
            
            # select column
            select_columns = UniqueList.UniqueList()
            for key in self.__attributes.keys():
                select_columns.append("%s.%s" % (self.__table_name, key))
            criteria.setSelectColumns(select_columns)

            factory = ObjectFactory.ObjectFactory( self.__aggregate,
                                                   schema_name = self.__db_schema,
                                                   table_name  = self.__table_name,
                                                   logger      = self.__logger )
            results = factory.doSelect(criteria, ret_dict=1)
            if results:
                results = results[0]
                for k in results.keys():
                    self.__attributes[k] = results[k]
            
            self.__initialized = True

    def unload(self):
        """ Unload attribute values from this object.
        """
        for key in self.__attributes.keys():
            self.__attributes[key] = None
        for key in self.__dirty_attrs.keys():
            del self.__dirty_attrs[key]
        self.__initialized = False
        self.__is_dirty = False

    def reload(self):
        """ Reload the attribute values.
        """
        self.unload()
        self.load()
    
    def update(self, force=False):
        """ Update and commit changes into database if dirty.

            @param force A flag used to update Timestamp on an Aggregate root.
                   Basically, if force==True and exists timestamp column and not dirty,
                   then the timestamp column will set to NULL.
        """
        result = 0
        if self.__is_dirty or (force and self.__timestamp_column):
            proof = self.__aggregate.getProofInstance()

            # column to update
            update_criteria = Criteria.Criteria( proof,
                                                 db_name = self.__db_name,
                                                 logger  = self.__logger )
            for key in self.__dirty_attrs.keys():
                update_criteria["%s.%s" % (self.__table_name, key)] = self.__dirty_attrs[key]

            if not self.__is_dirty:
                update_criteria[self.__timestamp_column.getFullyQualifiedName()] = None

            # where clause
            where_criteria  = Criteria.Criteria( proof,
                                                 db_name = self.__db_name,
                                                 logger  = self.__logger )
            pk_value = self.__pk.getValue()
            if type(pk_value) == type([]):
                for pk in pk_value:
                    where_criteria[pk.getFullyQualifiedName()] = pk.getValue()
            else:
                where_criteria[self.__pk.getFullyQualifiedName()] = pk_value

            factory = ObjectFactory.ObjectFactory( self.__aggregate,
                                                   schema_name = self.__db_schema,
                                                   table_name  = self.__table_name,
                                                   logger      = self.__logger )

            result = factory.doUpdate(update_criteria, where_criteria)

            # update the attribute dict
            for key, value in self.__dirty_attrs.items():
                self.__attributes[key] = value
                
            # clean up dirty attrs
            self.__dirty_attrs = {}
            self.__is_dirty = False

        return result

    def cancel(self):
        """ Revert the changes.
        """
        # clean up dirty attrs
        self.__dirty_attrs = {}
        self.__is_dirty = False

    def delete(self):
        """ Delete this record from database.
        """
        proof = self.__aggregate.getProofInstance()

        delete_criteria = Criteria.Criteria( proof,
                                             db_name = self.__db_name,
                                             logger  = self.__logger )
        pk_value = self.__pk.getValue()
        if type(pk_value) == type([]):
            for pk in pk_value:
                delete_criteria[pk.getFullyQualifiedName()] = pk.getValue()
        else:
            delete_criteria[self.__pk.getFullyQualifiedName()] = pk_value

        factory = ObjectFactory.ObjectFactory( self.__aggregate,
                                               schema_name = self.__db_schema,
                                               table_name  = self.__table_name,
                                               logger      = self.__logger )

        factory.doDelete(delete_criteria)

        # remove itself from aggregate
        self.__aggregate.removeObject(self.__table_name, self.__pk)
    
        # delete self
        del self
    
    def __eq__(self, obj):
        if obj is self:
            return True
        
        if isinstance(obj, self.__class__):
            if obj.getPK() == self.__pk:
                return True

        return False

    #=====================================================================
    
    def getLogger(self):
        return self.__logger
    
    def setLogger(self, logger):
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write
    
    def __getstate__(self):
        """ Used by pickle when the class is serialized.
            Remove the 'proof', 'logger' attributes before serialization.
            @hidden
        """
        self.log = None
        self.__logger = None
        d = copy.copy(self.__dict__)
        del d["log"]
        del d["_BaseObject__logger"]
        return d
    
    def __setstate__(self, d):
        """ Used by pickle when the class is unserialized.
            Add the 'proof', 'logger' attributes.
            @hidden
        """
        cls_name = self.__class__.__name__
        logger = Logger.makeLogger(None)
        d["_BaseObject__logger"] = logger
        d["log"] = logger.write
        self.__dict__ = d
