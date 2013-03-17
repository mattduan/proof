"""
ColumnMap is used to model a column of a table in a database.
"""

class ColumnMap:

    def __init__( self,
                  name,
                  containingTable,
                  type = None,
                  pk = False,
                  notNull = False,
                  fkTable = None,
                  fkColumn = None,
                  size = 0 ):
        """ Constructor.
     
            @param name The name of the column.
            @param containingTable TableMap of the table this column is in.
        """
        # The name of the column.
        self.__columnName = name

        # The TableMap for this column.
        self.__table = containingTable

        # Type of the column.
        self.__type = type

        # Size of the column.
        self.__size = size

        # Is it a primary key?
        self.__pk = pk

        # Is null value allowed ?
        self.__notNull = notNull

        # Name of the table that this column is related to.
        self.__relatedTableName = fkTable

        # Name of the column that this column is related to.
        self.__relatedColumnName = fkColumn

    def getColumnName(self):
        """ Get the name of a column.
        
            @return A String with the column name.
        """
        return self.__columnName

    def getFullyQualifiedName(self):
        """ Get the table name + column name.
        
            @return A String with the full column name.
        """
        return self.__table.getName() + "." + self.__columnName

    def getTableName(self):
        """ Get the name of the table this column is in.
        
            @return A String with the table name.
        """
        return self.__table.getName()

    def setType(self, ctype):
        """ Set the type of this column.
        
            @param type An Object specifying the type.
        """
        self.__type = ctype

    def setSize(self, size):
        """ Set the size of this column.
        
            @param size An int specifying the size.
        """
        self.__size = size

    def setPrimaryKey(self, pk):
        """ Set if this column is a primary key or not.
        
            @param pk True if column is a primary key.
        """
        self.__pk = pk

    def setNotNull(self, nn):
        """ Set if this column may be null.
        
            @param nn True if column may be null.
        """
        self.__notNull = nn

    def setForeignKey(self, columnName, tableName="" ):
        """ Set the foreign key for this column.
        
            @param tableName The name of the table that is foreign.
            @param columnName The name of the column that is foreign.
        """
        if columnName and not tableName:
            if columnName.find(".") > 0:
                tableName, columnName = columnName.split(".", 1)

        if tableName and columnName:
            self.__relatedTableName  = tableName
            self.__relatedColumnName = columnName
        else:
            self.__relatedTableName  = ""
            self.__relatedColumnName = ""

    def getType(self):
        """ Get the type of this column.
        
            @return An Object specifying the type.
        """
        return self.__type

    def getSize(self):
        """ Is this column a primary key?
        
            @return True if column is a primary key.
        """
        return self.__size

    def isPrimaryKey(self):
        """ Is this column a primary key?
        
            @return True if column is a primary key.
        """
        return self.__pk

    def isNotNull(self):
        """ Is null value allowed ?
        
            @return True if column may be null.
        """
        return (self.__notNull or self.isPrimaryKey())

    def isForeignKey(self):
        """ Is this column a foreign key?
        
            @return True if column is a foreign key.
        """
        return bool(self.__relatedTableName)

    def getRelatedName(self):
        """ Get the table.column that this column is related to.
        
            @return A String with the full name for the related column.
        """
        return self.__relatedTableName + "." + self.__relatedColumnName

    def getRelatedTableName(self):
        """ Get the table name that this column is related to.
        
            @return A String with the name for the related table.
        """
        return self.__relatedTableName

    def getRelatedColumnName(self):
        """ Get the column name that this column is related to.
        
            @return A String with the name for the related column.
        """
        return self.__relatedColumnName
    
