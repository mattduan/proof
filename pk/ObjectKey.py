"""
This class can be used to uniquely identify an object within
an application.  There are four subclasses: StringKey, NumberKey,
and DateKey, and ComboKey which is a Key made up of a combination
of the first three.
"""

__version__= '$Revision: 3194 $'[11:-2]


import string
import types
import time
import datetime

import proof.ProofException as ProofException


# The single character used to separate column and value in a string.
COL_SEPARATOR = '|'

# The single character used to represent the key type
UNKNOWN_KEY_TYPE = 'U'
STRING_KEY_TYPE  = 'S'
NUMBER_KEY_TYPE  = 'N'
DATE_KEY_TYPE    = 'D'

KEY_TYPES = [ UNKNOWN_KEY_TYPE,
              STRING_KEY_TYPE,
              NUMBER_KEY_TYPE,
              DATE_KEY_TYPE ]

class ObjectKey:
    """ The top abstract class for all primary keys.
    """

    def __init__(self, value, column_name=None, table_name=None):
        """ Constructor.

            Initializes the internal key value to <code>None</code>.
        """
        if not column_name and \
               isinstance(value, types.StringTypes) and \
               value[0] in KEY_TYPES and \
               value.find(COL_SEPARATOR) > 0:
            # this is constructed key
            table_name, column_name, value = self.__parse_key(value)
        
        elif column_name.find('.') > 0:
            table_name, column_name = column_name.split('.')

        if not table_name or not column_name:
            raise ProofException.ProofImproperUseException( \
                "table.column is not specified for ObjectKey." )

        self.__table_name  = table_name
        self.__column_name = column_name
        
        self.__key = None
        self.setKey(value)

    def getKey(self):
        """ Get the underlying object.
        
            @return the underlying object
        """
        return self.__key

    getValue = getKey
    
    def setKey(self, key):
        """ Reset the underlying object.
            
            @param key a value
        """
        if hasattr(key, '__class__') and issubclass(key.__class__, ObjectKey):
            key = key.getKey()

        self.__key = key

    setValue = setKey

    def getTableName(self):
        return self.__table_name

    def getColumnName(self):
        return self.__column_name

    def getFullyQualifiedName(self):
        return "%s.%s" % (self.__table_name, self.__column_name)

    def __parse_key(self, key_str):
        """ Parse a constructed key string.
        """
        key_type = key_str[0]
        full_column, value = key_str[1:].split(COL_SEPARATOR,1)
        table_name, column_name = full_column.split('.')
        if key_type == NUMBER_KEY_TYPE:
            value = int(value)
        elif key_type == DATE_KEY_TYPE:
            t_data = time.strptime(value, '%Y-%m-%d')
            value = datetime.date(t_data[0], t_data[1], t_data[2])

        return table_name, column_name, value

    def __guess_key_type(self):
        """ Guess the key type based on the key value. Since 99% of time,
            we only use int or string as primary key, so we only depend on
            data type w/o parsing the value for a date type. To be specific,
            please use subclass instead.

            @return A key type constant.
        """
        if self.__key:
            if isinstance(self.__key, types.IntType) or \
               isinstance(self.__key, types.LongType) or \
               isinstance(self.__key, types.FloatType):
                return NUMBER_KEY_TYPE
            elif isinstance(self.__key, types.StringTypes):
                return STRING_KEY_TYPE

        return UNKNOWN_KEY_TYPE

    def __str__(self):
        """ String representation of the key.

            Each Key is represented by [type U][full_column]|[value].
        """
        key_type = self.__guess_key_type()
        return "%s%s%s%s" % ( key_type,
                              self.getFullyQualifiedName(),
                              COL_SEPARATOR,
                              self.__key )

    def __repr__(self):
        return "<%s instance '%s' at 0x%x>" % (
            self.__class__.__name__,
            self.__str__(),
            id(self) )

    def __eq__(self, pk):
        if isinstance(pk, self.__class__):
            return ( self.getFullyQualifiedName() == pk.getFullyQualifiedName() \
                     and self.__key == pk.getKey() )

        return False

    def __cmp__(self, pk):
        if isinstance(pk, self.__class__) and \
               self.getFullyQualifiedName() == pk.getFullyQualifiedName():
            return cmp( self.__key, pk.getKey() )
        else:
            raise TypeError( "can't compare %s %s to %s %s" % \
                             (self.__class__.__name__, self.getFullyQualifiedName(),
                              type(pk), pk.getFullyQualifiedName()) )
    
