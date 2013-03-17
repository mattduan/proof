"""
TableMap is used to model a table in a database.
"""

__version__= '$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"

import string
import types
import datetime

import proof.pk.IDMethod as IDMethod

import proof.mapper.ColumnMap as ColumnMap

class TableMap:

    def __init__( self,
                  tableName,
                  containingDB,
                  prefix = "" ):
        """ Constructor.
        
            @param tableName The name of the table.
            @param numberOfColumns The number of columns in the table.
            @param containingDB A DatabaseMap that this table belongs to.
            @param prefix The prefix for the table name (ie: SCARAB for
             SCARAB_PROJECT).
        """

        # The columns in the table
        self.__columns = {}

        # The database this table belongs to.
        self.__dbMap = containingDB

        # The name of the table.
        self.__tableName = tableName

        # The prefix on the table name.
        self.__prefix = prefix

        # The primary key generation method. */
        self.__primaryKeyMethod = IDMethod.NO_ID_METHOD

        # Object to store information that is needed if the
        # for generating primary keys.
        self.__pkInfo = None

        # the timestamp column
        self.__timestamp_column = None

    def containsColumn(self, column):
        """ Does this table contain the specified column?
        
            @param column A String/ColumnMap representation of the column.
            @return True if the table contains the column.
        """
        if isinstance(column, ColumnMap.ColumnMap):
            column = column.getColumnName()
        
        if string.find(column, '.') > 0:
            column = string.replace(column, self.__tableName + '.', '')

        return self.__columns.has_key( column )

    def getDatabaseMap(self):
        """ Get the DatabaseMap containing this TableMap.
        
            @return A DatabaseMap.
        """
        return self.__dbMap

    def containsObjectColumn(self):
        """ Returns true if this tableMap contains a column with object
            data.  If the type of the column is not a string, a number or a
            date, it is assumed that it is object data.
            
            @return True if map contains a column with object data.
        """
        for key, value in self.__columns.items():
            if not value.getType() in [ types.IntType,
                                        types.LongType,
                                        types.FloatType,
                                        types.StringType,
                                        types.UnicodeType,
                                        datetime.date,
                                        datetime.datetime,
                                        datetime.timedelta ]:
                return True

        return False

    def getName(self):
        """ Get the name of the Table.
            
            @return A String with the name of the table.
        """
        return self.__tableName

    def getPrefix(self):
        """ Get table prefix name.
        
            @return A String with the prefix.
        """
        return self.__prefix

    def setPrefix(self, prefix):
        """ Set table prefix name.
        
            @param prefix The prefix for the table name (ie: SCARAB for
            SCARAB_PROJECT).
        """
        self.__prefix = prefix

    def getPrimaryKeyMethod(self):
        """ Get the method used to generate primary keys for this table.
        
            @return A String with the method.
        """
        return self.__primaryKeyMethod

    def getIdGenerator(self):
        """ Get the value of idGenerator.

            @return value of idGenerator.
        """
        return self.getDatabaseMap().getIdGenerator(self.__primaryKeyMethod)

    def getPrimaryKeyMethodInfo(self):
        """ Get the information used to generate a primary key
            
            @return An Object.
        """
        return self.__pkInfo

    def getColumns(self):
        """ Get a ColumnMap list of the columns in this table.
        
            @return A ColumnMap list.
        """
        return self.__columns.values()

    def getColumn(self, name):
        """ Get a ColumnMap for the named table.
        
            @param name A String with the name of the table.
            @return A ColumnMap.
        """
        return self.__columns.get(name)

    def addColumnMap(self, cmap):
        """ Add a pre-created column to this table.  It will replace any
            existing column.
            
            @param cmap A ColumnMap.
        """
        self.__columns[cmap.getColumnName()] = cmap

    def addColumn( self,
                   name,
                   type,
                   pk = False,
                   fkTable = None,
                   fkColumn = None,
                   size = 0 ):
        """ Add a column to the table.
        
            @param name A String with the column name.
            @param type An Object specifying the type.
            @param pk True if column is a primary key.
            @param fkTable A String with the foreign key table name.
            @param fkColumn A String with the foreign key column name.
            @param size An int specifying the size.
        """
        # If the tablename is prefixed with the name of the column,
        # remove it ie: SCARAB_PROJECT.PROJECT_ID remove the
        # SCARAB_PROJECT.
        if string.find( name, self.__tableName+"." ) == 0:
            name = name.replace(self.__tableName+".", "")

        if fkTable and fkColumn:
            if string.find(fkColumn, fkTable+".") == 0:
                fkColumn = fkColumn.replace(fkTable+".", "")

        col = ColumnMap.ColumnMap(name, self)
        
        col.setType(type)
        col.setPrimaryKey(pk)
        col.setForeignKey(fkTable, fkColumn)
        col.setSize(size)
        self.__columns[name] = col

    def addPrimaryKey(self, name, type, size=0):
        """ Add a primary key column to this Table.
        
            @param name A String with the column name.
            @param type An Object specifying the type.
            @param size An int specifying the size.
        """
        self.addColumn(name, type, True, size=size)

    def addForeignKey( self,
                       name,
                       type,
                       fkTable,
                       fkColumn,
                       size=0 ):
        """ Add a foreign key column to the table.
     
            @param name A String with the column name.
            @param type An Object specifying the type.
            @param fkTable A String with the foreign key table name.
            @param fkColumn A String with the foreign key column name.
            @param size An int specifying the size.
        """
        self.addColumn(name, type, False, fkTable, fkColumn, size)

    def addForeignPrimaryKey( self,
                              name,
                              type,
                              fkTable,
                              fkColumn,
                              size=0 ):
        """ Add a foreign primary key column to the table.
     
            @param name A String with the column name.
            @param type An Object specifying the type.
            @param fkTable A String with the foreign key table name.
            @param fkColumn A String with the foreign key column name.
            @param size An int specifying the size.
        """
        self.addColumn(name, type, True, fkTable, fkColumn, size)

    def setPrimaryKeyMethod(self, method):
        """ Sets the method used to generate a key for this table. Valid
            values are as specified in the IDMethod.
            
            @param method The ID generation method type name.
        """
        if method in IDMethod.VALID_ID_METHODS:
            self.__primaryKeyMethod = method

    def setPrimaryKeyMethodInfo(self, pkInfo):
        """ Sets the pk information needed to generate a key
        
            @param pkInfo information needed to generate a key
        """
        self.__pkInfo = pkInfo

    def getTimestampColumn(self):
        """ Get the timestamp column.

            @return A ColumnMap object or None.
        """
        return self.__timestamp_column

    def setTimestampColumn(self, column):
        """ Set the timestamp column if exists.

            @param column A ColumnMap object.
        """
        self.__timestamp_column = column

    def removeUnderScores(self, value):
        """ Removes the PREFIX, removes the underscores and makes
            first letter caps.
            
            SCARAB_FOO_BAR becomes FooBar.
            
            @param value A String.
            @return A String with value processed.
        """
        value = self.__removePrefix(value)
        value_list = map(string.capitalize, value.split("_"))
        return string.join(value_list, '')

    # Utility methods for doing intelligent lookup of table names

    def __hasPrefix(self, value):
        """ Tell me if i have PREFIX in my string.
        
            @param value A String.
            @return True if prefix is contained in value.
        """
        return string.find(value, self.getPrefix())!=-1

    def __removePrefix(self, value):
        """ Removes the PREFIX from a string.
        
            @param value A String.
            @return A String with value, but with prefix removed.
        """
        return value.replace(self.getPrefix(), '')

