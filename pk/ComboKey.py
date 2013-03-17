"""
This class can be used as an ObjectKey to uniquely identify an
object within an application where the key consists of multiple
entities (such a String[] representing a multi-column primary key).
"""

__version__= '$Revision: 117 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import string
import types

import ObjectKey
import NumberKey
import StringKey
import DateKey

# The single character used to separate key values in a string.
KEY_SEPARATOR = ':'


class ComboKey(ObjectKey.ObjectKey):

    def __init__(self, keys):
        """ Constructor.

            @param keys A list of ObjectKeys or a String representation of it.
        """
        self.__key = []

        self.setKey(keys)
        
    def setKey(self, keys):
        """ Sets the internal representation of the key.

            @param keys A ComboKey or a list of ObjectKeys or a String representation of it.
        """
        self.__key = []

        # a ComboKey
        if isinstance(keys, self.__class__):
            self.__key = keys.getKey()
        
        # a list of ObjectKeys
        if isinstance(keys, types.ListType):
            for key in keys:
                if isinstance( key, ObjectKey.ObjectKey ):
                    self.__key.append( key )
                else:
                    raise TypeError( "ComboKey() requires a list of ObjectKey objects, not %s" % \
                                     type(key) )

        # a string represent with format <code>[type N|S|D][value][:]</code>
        elif isinstance(keys, types.StringType):
            sk_list = keys.split( KEY_SEPARATOR )
            for sk in sk_list:
                t = sk[0]
                v = sk[1:]
                # NumberKey
                if t == 'N':
                    self.__key.append(NumberKey.NumberKey(sk))
                # StringKey
                elif t == 'S':
                    self.__key.append(StringKey.StringKey(sk))
                # DateKey
                elif t == 'D':
                    self.__key.append(DateKey.DateKey(sk))
                else:
                    self.__key.append(ObjectKey.ObjectKey(sk))
        
    setValue = setKey

    def getTableName(self):
        # all table names should be same
        return self.__key[0].getTableName()

    def getColumnName(self):
        return [ k.getColumnName() for k in self.__key ]

    def getFullyQualifiedName(self):
        return [ k.getFullyQualifiedName() for k in self.__key ]

    def __str__(self):
        """ A String that may consist of one section or multiple sections
            separated by a colon. <br/>
            Each Key is represented by <code>[type N|S|D][value][:]</code>. <p/>
            Example: <br/>
            the ComboKey(StringKey("key1"), NumberKey(2)) is represented as
            <code><b>Skey1:N2:</b></code>
            
            @return a String representation
        """
        s_list = []
        for k in self.__key:
            s_list.append( str(k) )

        return string.join(s_list, KEY_SEPARATOR)

    
    def __eq__(self, combokey):
        """ Compare to another ComboKey.

            Return True when they have same list of keys.
        """
        if combokey is self:
            return True
        
        try:
            combokey = ComboKey.ComboKey( combokey )
        except:
            return False

        if len(self.__key) == len(combokey.getKey()):
            keys = combokey.getKey()
            for i in range(len(self.__key)):
                if not self.__key[i] == keys[i]:
                    return False
            return True

        return False
    
