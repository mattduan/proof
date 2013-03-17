"""
This class can be used as an ObjectKey to uniquely identify an
object within an application where the id  consists
of a single entity such a GUID or the value of a db row's primary key.
"""

__version__= '$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import datetime
import time

import proof.pk.ObjectKey as ObjectKey


class DateKey(ObjectKey.ObjectKey):

    def __init__(self, value, column_name=None, table_name=None):
        """ Creates a DateKey whose internal representation is a Date.
        """
        ObjectKey.ObjectKey.__init__(self, value, column_name, table_name)

    def setKey(self, key):
        """ Sets the internal representation to a Date
        
            @param key the key value
        """
        # A DateKey
        if isinstance(key, self.__class__):
            key = key.getKey()
        # A timestamp
        elif isinstance(key, types.IntType) or \
               isinstance(key, types.LongType):
            key = datetime.date.fromtimestamp(key)
        # A date string with iso format
        elif isinstance(key, types.StringType):
            t_data = time.strptime(key, '%Y-%m-%d')
            key = datetime.date(t_data[0], t_data[1], t_data[2])

        assert isinstance(key, datetime.date)

        ObjectKey.ObjectKey.setKey(self, key)

    setValue = setKey

    def __str__(self):
        """ String representation of the key.

            Each Key is represented by [type D][full_column]|[value].
        """
        return "%s%s%s%s" % ( ObjectKey.DATE_KEY_TYPE,
                              self.getFullyQualifiedName(),
                              ObjectKey.COL_SEPARATOR,
                              self.__key.__str__() )
        
