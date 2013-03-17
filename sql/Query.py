"""
Used to assemble an SQL SELECT query.  Attributes exist for the
sections of a SELECT: modifiers, columns, from clause, where
clause, and order by clause.  The various parts of the query are
appended to buffers which only accept unique entries.  This class
is used primarily by BasePeer.
"""

__version__= '$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import string

import util.UniqueList as UniqueList
import proof.sql.SQLConstants as SQLConstants


class Query:

    def __init__(self):
        """ Constructor.
        """
        self.__selectModifiers = UniqueList.UniqueList()
        self.__selectColumns   = UniqueList.UniqueList()
        self.__fromTables      = UniqueList.UniqueList()
        self.__whereCriteria   = UniqueList.UniqueList()
        self.__orderByColumns  = UniqueList.UniqueList()
        self.__groupByColumns  = UniqueList.UniqueList()
        self.__having          = ""
        self.__limit           = ""
        self.__rowcount        = ""

    def getSelectModifiers(self):
        """ Retrieve the modifier buffer in order to add modifiers to this
            query.  E.g. DISTINCT and ALL.
            
            @return A UniqueList used to add modifiers.
        """
        return self.__selectModifiers

    def setSelectModifiers(self, modifiers):
        """ Set the modifiers. E.g. DISTINCT and ALL.
            
            @param modifiers the modifiers
        """
        self.__selectModifiers = modifiers

    def getSelectClause(self):
        """ Retrieve the columns buffer in order to specify which columns
            are returned in this query.
            
            @return An UniqueList used to add columns to be selected.
        """
        return self.__selectColumns

    def setSelectClause(self, columns):
        """ Set the columns.
        
            @param columns columns list
        """
        self.__selectColumns = columns

    def getFromClause(self):
        """ Retrieve the from buffer in order to specify which tables are
            involved in this query.
            
            @return An UniqueList used to add tables involved in the query.
        """
        return self.__fromTables

    def setFromClause(self, tables):
        """ Set the from clause.
            
            @param tables the tables
        """
        self.__fromTables = tables

    def getWhereClause(self):
        """ Retrieve the where buffer in order to specify the selection
            criteria E.g. column_a=3.  Expressions added to the buffer will
            be separated using AND.
            
            @return An UniqueList used to add selection criteria.
        """
        return self.__whereCriteria

    def setWhereClause(self, where):
        """ Set the where clause.
            
            @param where where clause
        """
        self.__whereCriteria = where

    def getOrderByClause(self):
        """ Retrieve the order by columns buffer in order to specify which
            columns are used to sort the results of the query.
            
            @return An UniqueList used to add columns to sort on.
        """
        return self.__orderByColumns

    def setOrderByClause(self, orderBy):
        self.__orderByColumns = orderBy

    def getGroupByClause(self):
        """ Retrieve the group by columns buffer in order to specify which
            columns are used to group the results of the query.
            
            @return An UniqueList used to add columns to group on.
        """
        return self.__groupByColumns

    def setGroupByClause(self, groupBy):
        self.__groupByColumns = groupBy
    
    def setHaving(self, having):
        """ Set the having clause.  This is used to restrict which rows
            are returned.
            
            @param having A String.
        """
        self.__having = having

    def setLimit(self, limit):
        """ Set the limit number.  This is used to limit the number of rows
            returned by a query, and the row where the resultset starts.
            
            @param limit A constructed limit string.
        """
        self.__limit = limit

    def setRowcount(self, rowcount):
        """ Set the rowcount number.  This is used to limit the number of
            rows returned by Sybase and MS SQL/Server.
            
            @param rowcount An integer.
        """
        self.__rowcount = rowcount

    def getHaving(self):
        """ Get the having clause.  This is used to restrict which
            rows are returned based on some condition.
            
            @return A String that is the having clause.
        """
        return self.__having

    def getLimit(self):
        """ Get the limit number.  This is used to limit the number of
            returned by a query in Postgres.
            
            @return A String of the database-specific limit.
        """
        return self.__limit

    def getRowcount(self):
        """ Get the rowcount number.  This is used to limit the number of
            returned by a query in Sybase and MS SQL/Server.
            
            @return An integer of the row count.
        """
        return self.__rowcount
    
    def __str__(self):
        """ Outputs the query statement.
            
            @return A String with the query statement.
        """
        stmt = ""
        
        if self.__rowcount:
            stmt += SQLConstants.ROWCOUNT + \
                    str(self.__rowcount) + \
                    " "
        stmt += SQLConstants.SELECT + \
                string.join(map(str, self.__selectModifiers), " ") + \
                " " + \
                string.join(map(str, self.__selectColumns), ", ") + \
                SQLConstants.FROM + \
                string.join(map(str, self.__fromTables), ", ")

        if self.__whereCriteria:
            stmt += SQLConstants.WHERE + \
                    string.join(map(str, self.__whereCriteria), SQLConstants.AND)

        if self.__groupByColumns:
            stmt += SQLConstants.GROUP_BY + \
                    string.join(map(str, self.__groupByColumns), ", ")

        if self.__having:
            stmt += SQLConstants.HAVING + \
                    str(self.__having)

        if self.__orderByColumns:
            stmt += SQLConstants.ORDER_BY + \
                    string.join(map(str, self.__orderByColumns), ", ")

        if self.__limit:
            stmt += SQLConstants.LIMIT + \
                    str(self.__limit)
        
        if self.__rowcount:
            stmt += SQLConstants.ROWCOUNT + \
                    str(self.__rowcount)

        return stmt
