"""
This class represents a part of an SQL query found in the <code>WHERE</code>
section.  For example:
<pre>
table_a.column_a = table_b.column_a
column LIKE 'F%'
table.column < 3
</pre>
This class is used primarily by proof.BaseFactory.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import string
import datetime
import types
import decimal

import proof.sql.SQLConstants as SQLConstants
import util.UniqueList as UniqueList


class SQLExpression:

    def __init__(self):
        """ Constructor.
        """
        pass

    def buildInnerJoin( self,
                        column,
                        relatedColumn,
                        ignoreCase = False,
                        db = None ):
        """ Used to specify a join on two columns.
     
            @param column A column in one of the tables to be joined.
            @param relatedColumn The column in the other table to be joined.
            @param ignoreCase If true and columns represent Strings, the appropriate
                   function defined for the database will be used to ignore
                   differences in case.
            @param db Represents the database in use for vendor-specific functions
            @return A join expression, e.g. UPPER(table_a.column_a) =
                    UPPER(table_b.column_b).
        """
        if ignoreCase and db:
            return "%s = %s" % ( db.ignoreCase(column),
                                 db.ignoreCase(relatedColumn) )
        else:
            return "%s = %s" % ( column, relatedColumn )

    def build( self,
               columnName,
               criteria,
               comparison,
               ignoreCase = False,
               db = None ):
        """ Builds a simple SQL expression.
        
            @param columnName A column.
            @param criteria The value to compare the column against.
            @param comparison One of =, &lt;, &gt;, ^lt;=, &gt;=, &lt;&gt;,
                   !=, LIKE, etc.
            @return A simple SQL expression, e.g. UPPER(table_a.column_a)
                    LIKE UPPER('ab%c').
        """
        if str(criteria) == "None":
            criteria = None

        # If the criteria is None, check to see comparison
        # is an =, <>, or !=.  If so, replace the comparison
        # with the proper IS or IS NOT.
        if criteria == None:
            if comparison == SQLConstants.EQUAL:
                comparison = SQLConstants.ISNULL
            elif comparison == SQLConstants.NOT_EQUAL:
                comparison = SQLConstants.ISNOTNULL
            elif comparison == SQLConstants.ALT_NOT_EQUAL:
                comparison = SQLConstants.ISNOTNULL

        if comparison in [ SQLConstants.LIKE,
                           SQLConstants.NOT_LIKE ]:
            columnValue = self.__safeSQLString( criteria,
                                                ignoreCase,
                                                db )
            columnValue, comparison = self.__processLikeString( columnValue,
                                                                comparison )
        elif comparison in [ SQLConstants.IN,
                             SQLConstants.NOT_IN ]:
            self.__processInString( criteria,
                                    ignoreCase,
                                    db )
            columnValue = "(%s)" % ( self.__processInString( criteria,
                                                             ignoreCase,
                                                             db ) )
        elif comparison in [ SQLConstants.ISNULL,
                             SQLConstants.ISNOTNULL ]:
            return "%s%s" % (columnName, comparison)
        else:
            columnValue = self.__safeSQLString( criteria,
                                                ignoreCase,
                                                db )
            
        if ignoreCase and db:
            columnName  = db.ignoreCase(columnName)

        return "%s%s%s" % ( columnName,
                            comparison,
                            columnValue )

    def buildInsertList( self,
                         column_list,
                         criteria_list,
                         ignoreCase = False,
                         db = None ):
        """ Build the insert column and value list.

            @param column_list The column fullnames
            @param criteria_list The insert values
            @param ignoreCase If true and columns represent Strings, the appropriate
                   function defined for the database will be used to ignore
                   differences in case.
            @param db Represents the database in use for vendor-specific functions
            @return Two lists of built columns and values.
        """
        n = len(criteria_list)
        assert len(column_list) == n

        if ignoreCase and db:
            column_list = map(db.ignoreCase, column_list)

        criteria_list = map( self.__safeSQLString,
                             criteria_list,
                             [ignoreCase]*n,
                             [db]*n )

        return (column_list, criteria_list)

    def quoteAndEscapeText(self, rawText, db=None):
        """ Quotes and escapes raw text for placement in a SQL expression.
            For simplicity, the text is assumed to be neither quoted nor
            escaped.
            
            @param rawText The <i>unquoted</i>, <i>unescaped</i> text to process.
            @param db the db
            @return Quoted and escaped text.
        """
        if db and not db.escapeText():
            escapeString = SQLConstants.BACKSLASH
        else:
            escapeString = SQLConstants.BACKSLASH + SQLConstants.BACKSLASH

        data = str(rawText)
        data = data.replace( SQLConstants.SINGLE_QUOTE,
                             SQLConstants.SINGLE_QUOTE + SQLConstants.SINGLE_QUOTE )
        data = data.replace( SQLConstants.BACKSLASH, escapeString )

        return "%s%s%s" % ( SQLConstants.SINGLE_QUOTE,
                            data,
                            SQLConstants.SINGLE_QUOTE )
        

    def __processLikeString( self,
                             criteria,
                             comparison ):
        """ Takes a criteria and builds an SQL phrase based on whether
            wildcards are present. Multicharacter wildcards % and * may
            be used as well as single character wildcards, _ and ?. These
            characters can be escaped with \.
            
            e.g. = "fre%" -> LIKE 'fre%'
                          -> LIKE UPPER('fre%')
                 = "50\%" -> = '50%'
            
            @param criteria The value to compare the column against.
            @param comparison Whether to do a LIKE or a NOT LIKE
            @return A list with appropriate criteria and comparison.
        """
        # If selection criteria contains wildcards use LIKE otherwise
        # use = (equals).  Wildcards can be escaped by prepending
        # them with \ (backslash).
        equalsOrLike = " = "

        like_str = ""
        criteria = str(criteria)
        position = 0
        length = len(criteria)
        while position < length:
            c = criteria[position]
            if c == SQLConstants.BACKSLASH:
                # Determine whether to skip over next character.
                if (position+1) < length and \
                   criteria[position+1] in [ '%',
                                             '_',
                                             '*',
                                             '?',
                                             SQLConstants.BACKSLASH ]:
                    position += 1
                    c = criteria[position+1]
            elif c in [ '%', '_' ]:
                equalsOrLike = comparison
            elif c == '*':
                equalsOrLike = comparison
                c = '%'
            elif c == '?':
                equalsOrLike = comparison
                c = '_'

            like_str += c
            position += 1

        # make sure like_str is quoted
        if like_str:
            if like_str[0]  != SQLConstants.SINGLE_QUOTE:
                like_str = SQLConstants.SINGLE_QUOTE + like_str
            if like_str[-1] != SQLConstants.SINGLE_QUOTE:
                like_str = like_str + SQLConstants.SINGLE_QUOTE
        else:
            like_str = "''"
        
        return ( like_str, equalsOrLike )

    def __processInString(self, criteria, ignoreCase=False, db=None):
        """ Creates an appropriate string for an 'IN' clause from an
            object.  Adds quoting and/or UPPER() as appropriate.

            @param value The value to process.
            @param ignoreCase Coerce the value suitably for ignoring case.
            @param db Represents the database in use for vendor specific functions.
            @return Processed value as String.
        """
        assert (isinstance(criteria, types.ListType) or \
                 isinstance(criteria, UniqueList.UniqueList ))

        n = len(criteria)
        return string.join( map( self.__safeSQLString,
                                 criteria,
                                 [ignoreCase]*n,
                                 [db]*n ),
                            "," )

    def __safeSQLString(self, criteria, ignoreCase=False, db=None):
        """ Creates an appropriate string an object.  Adds quoting
            and/or UPPER() as appropriate.

            @param value The value to process.
            @param ignoreCase Coerce the value suitably for ignoring case.
            @param db Represents the database in use for vendor specific functions.
            @return Processed value as String. If it was a string value, the return
            value will be quoted. e.g. "string" => "'string'"
        """
        ret = ""
        if criteria == None:
            ret = "null"

        elif isinstance(criteria, types.BooleanType):
            if db:
                ret = db.getBooleanString(criteria)
            else:
                ret = str(int(criteria))
        elif isinstance(criteria, types.StringTypes):
            ret = self.quoteAndEscapeText( criteria, db )
        elif isinstance(criteria, types.IntType ) or \
             isinstance(criteria, types.FloatType ) or \
             isinstance(criteria, decimal.Decimal):
            ret = str(criteria)
        elif isinstance(criteria, types.LongType ):
            if db:
                ret = db.getLongString(criteria)
            else:
                ret = str(criteria)
        elif isinstance(criteria, datetime.datetime ):
            if db:
                ret = db.getDateTimeString(criteria)
            else:
                ret = "'%s'" % (criteria.isoformat())
        elif isinstance(criteria, datetime.date ):
            if db:
                ret = db.getDateString(criteria)
            else:
                ret = "'%s'" % (criteria.isoformat())
        else:
            # cPickle should be handled in class level with corresponding unpickler
            # when retrieving
            raise TypeError( "SQLExpression.build: criteria type %s (%s) isn't supported." %
                             (type(criteria), str(criteria)) )

        if ignoreCase and db:
            ret = db.ignoreCase(ret)

        return ret
