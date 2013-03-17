"""
DatabaseMap is used to model a database.
"""

__version__= '$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import string

import proof.pk.IDMethod as IDMethod
import proof.pk.generator.IDBroker as IDBroker
import proof.mapper.TableMap as TableMap

class DatabaseMap:

    def __init__(self, name):
        """ Constructor.
        
            @param name Name of the database.
        """
        # Name of the database.
        self.__name = name

        # Name of the tables in the database.
        self.__tables = {}

        # A special table used to generate primary keys for the other
        # tables.
        self.__idTable = None

        # The IDBroker that goes with the idTable.
        self.__idBroker = None

        # The IdGenerators, keyed by type of idMethod.
        self.__idGenerators = {}

    def containsTable(self, table):
        """ Does this database contain this specific table?
        
            @param table The TableMap/String representation of the table.
            @return True if the database contains the table.
        """
        if isinstance( table, TableMap.TableMap ):
            table = table.getName()
        if string.find( table, "." ) > 0:
            table, column = string.split(table, ".", 1)

        return self.__tables.has_key(table)

    def getIdTable(self):
        """ Get the ID table for this database.
        
            @return A TableMap.
        """
        return self.__idTable

    def getIDBroker(self):
        """ Get the IDBroker for this database.
        
            @return An IDBroker.
        """
        return self.__idBroker

    def getName(self):
        """ Get the name of this database.
        
            @return A String.
        """
        return self.__name

    def getTable(self, name):
        """ Get a TableMap for the table by name.
     
            @param name Name of the table.
            @return A TableMap, null if the table was not found.
        """
        return self.__tables.get(name)

    def getTables(self):
        """ Get all of the tables in the database.
        """
        return self.__tables.values()

    def addTable(self, table):
        """ Add a new TableMap to the database.
        
            @param map The Name/TableMap representation.
        """
        if not isinstance( table, TableMap.TableMap ):
            table = TableMap.TableMap( table, self )

        self.__tables[table.getName()] = table

    def setIdTable(self, idTable):
        """ Set the ID table for this database.
        
            @param idTable The Name/TableMap representation for the ID table.
        """
        if not isinstance( idTable, TableMap.TableMap ):
            idTable = TableMap.TableMap( idTable, self )

        self.__idTable = idTable
        self.addTable(idTable)
        idBroker = IDBroker.IDBroker(idTable)
        self.addIdGenerator(IDMethod.ID_BROKER, idBroker)

    def addIdGenerator(self, idType, idGen):
        """ Add a type of id generator for access by a TableMap.
        
            @param type a <code>IDMethod</code> value
            @param idGen an <code>IdGenerator</code> value
        """
        if idType in IDMethod.VALID_ID_METHODS:
            self.__idGenerators[ idType ] = idGen

    def getIdGenerator(self, idType):
        """ Get a type of id generator.  Valid values are listed in the
            IDMethod.
            
            @param type a <code>IDMethod</code> value
            @return an <code>IdGenerator</code> value
        """
        return self.__idGenerators.get(idType)
        
