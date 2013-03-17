"""
This generator works with databases that have an sql syntax that
allows the retrieval of the last id used to insert a row for a
Connection.
"""

__version__='$Revision: 3194 $'[11:-2]


import logging

import proof.pk.generator.IDGenerator as IDGenerator


class AutoIncrementIDGenerator(IDGenerator.IDGenerator):

    __implements__ = 'IDGenerator'

    def __init__(self, adapter, logger=None):
        """ Creates an IdGenerator which will work with the specified database.
        
            @param adapter the adapter that knows the correct sql syntax.
        """
        self.__adapter = adapter
        IDGenerator.IDGenerator.__init__(self, logger=logger)

    def getId(self, connection=None, key_info=None):
        """ Returns the last ID used by this connection.
        
            @param connection A Connection.
            @param key_info an Object that contains additional info.
            @return An ID with the last value auto-incremented as a
            result of an insert.
        """
        id_sql = self.__adapter.getIDMethodSQL(key_info)

        if connection:
            cursor = connection.getCursor()
            cursor.query(id_sql)
            result = cursor.fetchone()
            if result:
                return result[0]

        self.log( "No Id is generated by AutoIncrementIDGenerator with adapter %s." \
                  % (self.__adapter.__class__.__name__), logging.WARNING )
        return None

    def isPriorToInsert(self):
        """ A flag to determine the timing of the id generation.
        """
        return False

    def isPostInsert(self):
        """ A flag to determine the timing of the id generation.
        """
        return True

    def isConnectionRequired(self):
        """ A flag to determine whether a Connection is required to
            generate an id.
        """
        return True
    