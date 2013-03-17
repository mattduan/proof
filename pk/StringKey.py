"""
This class can be used as an ObjectKey to uniquely identify an
object within an application where the id  consists
of a single entity such a GUID or the value of a db row's primary key.
"""

__version__= '$Revision: 3194 $'[11:-2]


import types

import proof.pk.ObjectKey as ObjectKey


class StringKey(ObjectKey.ObjectKey):

    def __init__(self, value, column_name=None, table_name=None):
        """ Creates a StringKey whose internal representation is a String
        
            @param key the key value
        """
        ObjectKey.ObjectKey.__init__(self, value, column_name, table_name)

    def setKey(self, key):
        """ Sets the internal representation to a String
            
            @param key the key value
        """
        if isinstance(key, self.__class__):
            key = key.getValue()

        assert isinstance(key, types.StringTypes)

        ObjectKey.ObjectKey.setKey(self, key)

    setValue = setKey

    def __str__(self):
        """ String representation of the key.

            Each Key is represented by [type S][full_column]|[value].
        """
        return "%s%s%s%s" % ( ObjectKey.STRING_KEY_TYPE,
                              self.getFullyQualifiedName(),
                              ObjectKey.COL_SEPARATOR,
                              self.__key )
    
