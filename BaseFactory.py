"""
This is the base factory class for all factory classes in the system. Factory
classes are responsible for isolating all of the database access for all specific
business objects. They execute all of the SQL against the database. Over time
this class has grown to include utility methods which ease execution of
cross-database queries and the implementation of concrete Factories --
ObjectFactory and AggregateFactory.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import re
import string
import logging
import copy

import util.logger.Logger as Logger
import util.UniqueList as UniqueList
from util.Trace import traceBack

import proof.ProofInstance as ProofInstance
import proof.ProofException as ProofException
import proof.sql.SQLConstants as SQLConstants
import proof.sql.SQLExpression as SQLExpression
import proof.sql.Criteria as Criteria
import proof.sql.PageSelect as PageSelect
import proof.transaction.Transaction as Transaction
import proof.sql.Query as Query


class BaseFactory:
    
    def __init__( self,
                  proof_instance,
                  schema_name = "SCHEMA_NAME",
                  logger      = None,
                  ):
        """ Constructor.
        """

        self.__proof = proof_instance
        
        # need to be overrided in child classes
        self.__schema_name = schema_name
        self.__db_name     = self.__proof.getDBName(schema_name)
        self.__logger      = Logger.makeLogger(logger)
        self.log           = self.__logger.write

    def getProofInstance(self):
        return self.__proof

    def getSchemaName(self):
        return self.__schema_name

    def getDBName(self):
        return self.__db_name

    #================= Functions to do database queries ==================

    def __query(self, sql, con=None, ret_dict=0):
        """ A convenient method to make query to database.

            @param sql A complete SQL query string.
            @param con A existed Connection instance.
            @param ret_dict If true, return as dictionary.
        """
        if con:
            # execute only without closing the connection
            try:
                cursor = con.getCursor(ret_dict=ret_dict)
                #self.log("cursor: %s"%(cursor))
                cursor.execute(sql)
                #self.log("returning results.")
                return cursor.fetchall()
            except:
                self.log( "Exception in query: %s\nException: %s" % \
                                    (sql, traceBack()), logging.ERROR )
        else:
            try:
                try:
                    con = self.__proof.getConnection(self.__db_name)
                    cursor = con.getCursor(ret_dict=ret_dict)
                    cursor.execute(sql)
                    return cursor.fetchall()
                except:
                    self.log( "Exception in query: %s" % (traceBack()), logging.ERROR )
            finally:
                self.__proof.closeConnection(con)

    def __execute(self, sql, con=None):
        """ A convenient method to make insert/update/delete to database.

            @param con A existed Connection instance.
            @param sql A complete SQL executing string.
        """
        if con:
            # execute only without closing the connection
            try:
                cursor = con.getCursor()
                return cursor.execute(sql)
            except:
                self.log( "Exception in execute: %s" % (traceBack()), logging.ERROR )
        else:
            try:
                try:
                    con = self.__proof.getConnection(self.__db_name)
                    cursor = con.getCursor()
                    return cursor.execute(sql)
                except:
                    self.log( "Exception in execute: %s" % (traceBack()), logging.ERROR )
            finally:
                self.__proof.closeConnection(con)

    def __commit(self, sql_list):
        """ A convenient method to make insert/update/delete query to
            database with Transaction.

            @param sql_list A list of SQL executing string
        """
        results = []
        
        if type(sql_list) == type(""):
            sql_list = [sql_list]

        transaction = Transaction.Transaction(self.__proof, logger=self.__logger)

        try:
            con = transaction.begin(self.__db_name)
            for sql in sql_list:
                result = self.__execute(sql, con)
                results.append(result)
            transaction.commit()
        except:
            self.log( "Exception in transaction: %s" % (traceBack()), logging.ERROR )
            transaction.safeRollback()
    
        return results
    
    # DELETE
    #===========

    def deleteAll( self,
                   column,
                   value,
                   table = None
                   ):
        """ Delete multiple rows where column has value.

            @param column The column in the where clause.
            @param value The value of the column.

            @deprecated delete operation should be handled in BaseObject or
            Aggregate objects to make object consistency easier in Repository.
        """
        if column.find('.') > 0:
            table, column = column.split('.', 1)

        if table:
            assert str(table).find(";") == -1
            sql_expr = SQLExpression.SQLExpression()
            sql = "DELETE FROM %s WHERE %s" % ( table,
                                                sql_expr.build( column,
                                                                value,
                                                                SQLConstants.EQUAL ) )
            self.log("%s.deleteAll: %s" % (self.__class__.__name__, sql))
            
            return self.__commit( [sql] )

    def doDelete( self, criteria ):
        """ Method to perform deletes based on values and keys in a Criteria.
            
            @param criteria The criteria to use.
            @param con A Connection.

            @deprecated delete operation should be handled in BaseObject or
            Aggregate objects to make object consistency easier in Repository.
        """
        transaction = Transaction.Transaction(self.__proof, logger=self.__logger)

        result = 0
        try:
            con = transaction.begin( self.__db_name,
                                     useTransaction=criteria.isUseTransaction() )
            result = self.__delete(criteria, con)
            transaction.commit()
        except:
            self.log( "Exception in doDelete: %s" % (traceBack()), logging.ERROR )
            transaction.safeRollback()

        return result
        
    def __delete( self,
                  criteria,
                  con ):
        """ Method to perform deletes based on values and keys in a Criteria.
            
            @param criteria The criteria to use.
            @param con A Connection.

            @deprecated delete operation should be handled in BaseObject or
            Aggregate objects to make object consistency easier in Repository.
        """
        assert self.__db_name == criteria.getDbName()

        results = {}

        adapter = self.__proof.getAdapter(self.__db_name)
        db_map  = self.__proof.getDatabaseMap(self.__db_name)
        
        tables = UniqueList.UniqueList()

        for column in criteria.keys():
            c = criteria.getCriterion(column)
            tableNames = c.getAllTables()
            
            for table_name in tableNames:
                tableName2 = criteria.getTableForAlias(table_name)

                if tableName2:
                    tables.append("%s %s" % (tableName2, table_name))
                else:
                    tables.append(table_name)

            if criteria.isCascade():
                table_maps = db_map.getTables()
                for table_map in table_maps:
                    column_maps = table_map.getColumns()
                    for column_map in column_maps:
                        if column_map.isForeignKey() and \
                           column_map.isPrimaryKey() and \
                           column == column_map.getRelatedName():
                            tables.append(table_map.getName())
                            criteria.add(column_map.getFullyQualifiedName(),criteria[column])

        sql_expr = SQLExpression.SQLExpression()
        for table in tables:
            where_clause = UniqueList.UniqueList()
            
            column_maps = db_map.getTable(table).getColumns()
            for column_map in column_maps:
                k = "%s.%s"%(column_map.getTableName(),column_map.getColumnName())
                if criteria.has_key( k ):
                    if criteria.getComparison(k) == SQLConstants.CUSTOM:
                        where_clause.append(criteria[key])
                    else:
                        where_clause.append(sql_expr.build( column_map.getColumnName(),
                                                            criteria.getValue(k),
                                                            criteria.getComparison(k),
                                                            criteria.isIgnoreCase(),
                                                            adapter ))

            sql = "DELETE FROM %s" % (table)
            whereStr = string.join(map(str, where_clause), SQLConstants.AND)
            if whereStr:
                sql += " WHERE %s" % (whereStr)

                self.log( "%s.doDelete: %s" % (self.__class__.__name__, sql) )
                
                results[table] = self.__execute(sql, con)

        return results

    # INSERT
    #===========

    def doInsert( self, criteria ):
        """ Method to perform inserts based on values and keys in a
            Criteria.

            @param criteria Object containing values to insert.
            @return An Object which is the id of the row that was inserted
            (if the table has a primary key) or None (if the table does not
            have a primary key).
        """
        transaction = Transaction.Transaction(self.__proof, logger=self.__logger)

        id = None
        try:
            con = transaction.begin( self.__db_name,
                                     useTransaction=criteria.isUseTransaction() )
            id = self.__insert(criteria, con)
            transaction.commit()
        except:
            self.log( "Exception in doInsert: %s" % (traceBack()), logging.ERROR )
            transaction.safeRollback()

        return id
        
    def __insert( self, criteria, con ):
        """ Method to perform inserts based on values and keys in a
            Criteria. Currently, only single primary key is supported.
            If there is real need for combo primary keys, we change to
            support it. However, we should avoid to create combo primary
            keys in database to make identity clear in the OR layer.
            <p>
            If the primary key is auto incremented the data in Criteria
            will be inserted and the auto increment value will be returned.
            <p>
            If the primary key is included in Criteria then that value will
            be used to insert the row.
            <p>
            If no primary key is included in Criteria then we will try to
            figure out the primary key from the database map and insert the
            row with the next available id using util.db.IDBroker.
            <p>
            If no primary key is defined for the table the values will be
            inserted as specified in Criteria and -1 will be returned.

            @param criteria Object containing values to insert.
            @param con A Connection.
            @return The id of the row that was inserted (if the table has a
            primary key) or null (if the table does not have a primary key).
        """
        id         = None
        table_name = ''

        keys  = criteria.keys()
        if keys:
            table_name = criteria.getTableName(keys[0])
        else:
            raise ProofException.ProofImproperUseException( \
                    "Database insert attempted without anything specified to insert." )

        db_map      = self.__proof.getDatabaseMap(self.__db_name)
        table_map   = db_map.getTable(table_name)
        column_maps = table_map.getColumns()
        key_info    = table_map.getPrimaryKeyMethodInfo()
        key_gen     = table_map.getIdGenerator()

        # create primary key
        pk = None
        for column_map in column_maps:
            if column_map.isPrimaryKey():
                pk = column_map
                break

        if pk and not criteria.has_key(pk.getFullyQualifiedName()):
            if not key_gen:
                raise ProofException.ProofNotFoundException( \
                    "IDGenerator for table '%s' is None" % (table_name) )

            if key_gen.isPriorToInsert():
                id = key_gen.getId(connection=con, key_info=key_info)
                criteria[pk.getFullyQualifiedName()] = id

        # perform the insert
        column_list = []
        value_list  = []
        for column in column_maps:
            column_name = column.getFullyQualifiedName()
            if criteria.has_key(column_name):
                column_list.append(column_name)
                value_list.append(criteria[column_name])

        sql_expr = SQLExpression.SQLExpression()
        #self.log("doInsert: (%s) (%s)"%(column_list, value_list), level=logging.INFO)
        (column_list, value_list) = sql_expr.buildInsertList(column_list, value_list)

        sql = "INSERT INTO %s (%s) VALUES (%s)" % ( table_name,
                                                    string.join(column_list, ", "),
                                                    string.join(value_list, ", ") )

        self.log( "%s.doInsert: %s" % (self.__class__.__name__, sql) )

        self.__execute(sql, con)

        if pk and key_gen and key_gen.isPostInsert():
            id = key_gen.getId(connection=con, key_info=key_info)

        return id

    # SELECT
    #===========

    def createQueryString(self, criteria):
        """ Method to create an SQL query for actual execution based on values in a
            Criteria.
            
            @param criteria A Criteria.
            @return the SQL query for actual execution
        """
        query = self.createQuery(criteria)

        #adapter = self.__proof.getAdapter(criteria.getDbName())
        #
        #limit = criteria.getLimit();
        #offset = criteria.getOffset()
        #
        #sql = ''
        #if (limit or offset) and \
        #       adapter.getLimitStyle() == SQLConstants.LIMIT_STYLE_ORACLE:
        #    raise ProofException.ProofNotImplementedException( \
        #        "ORACLE support is not implemented yet!" )
        #else:
        #    if offset and adapter.supportsNativeOffset():
        #        # Now set the criteria's limit and offset to return the
        #        # full resultset since the results are limited on the
        #        # server.
        #        criteria.setLimit(-1)
        #        criteria.setOffset(0)
        #    elif limit and adapter.supportsNativeLimit():
        #        # Now set the criteria's limit to return the full
        #        # resultset since the results are limited on the server.
        #        criteria.setLimit(-1)

        sql = str(query)

        self.log( "SQL: %s" % (sql) )

        return sql

    def createTotalQueryString(self, criteria, count_str=None):
        """ Method to create a SQL query for the total records using actual execution
            based on values in a Criteria.
            
            @param criteria A Criteria.
            @return the SQL query for the total records
        """
        query = self.createQuery(criteria)

        if count_str:
            query.setSelectClause([count_str])
        else:
            query.setSelectClause(["COUNT(*) AS count"])
        
        query.setSelectModifiers(UniqueList.UniqueList())
        
        sql = str(query)

        self.log( "SQL for total: %s" % (sql) )

        return sql
    
    def createRawQueryString(self, criteria, select_clause=[]):
        """ Method to create a SQL query for any columns using actual execution
            based on values in a Criteria.
            
            @param criteria A Criteria.
            @return the SQL query string.
        """
        query = self.createQuery(criteria)
        query.setSelectClause(select_clause)
        query.setSelectModifiers(UniqueList.UniqueList())
        
        sql = str(query)

        self.log( "SQL for raw query: %s" % (sql) )

        return sql


    def createQuery(self, criteria):
        """ Method to create a SQL query based on values in a Criteria.  Note that
            final manipulation of the limit and offset are performed when the query
            is actually executed.
            
            @param criteria A Criteria.
            @return the Query object
        """
        self.log( "start %s create query" % (self.__class__.__name__) )
        query     = Query.Query()
        adapter   = self.__proof.getAdapter(criteria.getDbName())
        db_map    = self.__proof.getDatabaseMap(criteria.getDbName())

        order_by  = criteria.getOrderByColumns()
        group_by  = criteria.getGroupByColumns()
        select    = criteria.getSelectColumns()
        aliases   = criteria.getAsColumns()
        modifiers = criteria.getSelectModifiers()

        str_delimiter = adapter.getStringDelimiter()

        query.setSelectModifiers( modifiers )

        select_clause = UniqueList.UniqueList()
        from_clause   = UniqueList.UniqueList()
        for column in select:
            if column.find('.') == -1 and column.find('*') == -1:
                raise ProofException.ProofImproperUseException( \
                    "MalformedColumnName: table_name.column_name or * expected but get %s." % (column) )

            table_name = ''
            select_clause.append(column)
            paren_pos = column.find("(")
            if paren_pos == -1:
                table_name, column_name = column.split(".", 1)
            elif column.find(".") != -1:
                table_name = column[paren_pos+1:column.find(".")]
                last_space = table_name.rfind(" ")
                if last_space != -1:
                    table_name = table_name[last_space+1:]

            table_name2 = criteria.getTableForAlias(table_name)
            if table_name2:
                from_clause.append( "%s %s" % (table_name2,table_name) )
            else:
                from_clause.append( table_name )

        for key in aliases.keys():
            select_clause.append( "%s AS %s%s%s" % (aliases[key], str_delimiter, key, str_delimiter))

        where_clause = UniqueList.UniqueList()
        for key in criteria.keys():
            criterion = criteria.getCriterion(key)
            attached_criteria = criterion.getAttachedCriterion()
            for crit in attached_criteria:
                table_name  = crit.getTable()
                table_name2 = criteria.getTableForAlias(table_name)
                if table_name2:
                    from_clause.append( "%s %s" % (table_name2,table_name) )
                else:
                    from_clause.append( table_name )
                    table_name2 = table_name

                ignorCase = (criteria.isIgnoreCase() or crit.isIgnoreCase()) and \
                            (db_map.getTable(table_name2).getColumn(crit.getColumn()).getType()==type(""))
                crit.setIgnoreCase(ignorCase)

            criterion.setAdapter(adapter)
            where_clause.append( str(criterion) )

        sql_expr = SQLExpression.SQLExpression()
        joinL = criteria.getJoinL()
        joinR = criteria.getJoinR()
        if joinL and joinR:
            for L, R in zip(joinL, joinR):
                if L.find(".") == -1:
                    raise ProofException.ProofImproperUseException( \
                        "MalformedLeftJoinColumn: table_name.column_name expected but get %s." % (L) )
                if R.find(".") == -1:
                    raise ProofException.ProofImproperUseException( \
                        "MalformedRightJoinColumn: table_name.column_name expected but get %s." % (R) )

                table_name, column_name = L.split(".", 1)
                table_name2 = criteria.getTableForAlias(table_name)
                if table_name2:
                    from_clause.append( "%s %s" % (table_name2,table_name) )
                else:
                    from_clause.append( table_name )
                    table_name2 = table_name

                table_name, column_name = R.split(".", 1)
                table_name2 = criteria.getTableForAlias(table_name)
                if table_name2:
                    from_clause.append( "%s %s" % (table_name2,table_name) )
                else:
                    from_clause.append( table_name )
                    table_name2 = table_name

                ignorCase = (criteria.isIgnoreCase() and \
                             db_map.getTable(table).getColumn(column_name).getType()==type(""))

                where_clause.append(sql_expr.buildInnerJoin(L, R, ignorCase, adapter))

        group_by_clause = UniqueList.UniqueList()
        if group_by:
            for group in group_by:
                if group.find('.') == -1:
                    raise ProofException.ProofImproperUseException( \
                        "MalformedGroupByColumn: table_name.column_name expected but get %s." % (group) )
                table_name, column_name = group.split('.', 1)
                table_name2 = criteria.getTableForAlias(table_name)
                if not table_name2:
                    table_name2 = table_name
                from_clause.append(table_name2)
                group_by_clause.append(group)

        having = criteria.getHaving()
        if having:
            query.setHaving(str(having))
        
        order_by_clause = UniqueList.UniqueList()
        if order_by:
            for order in order_by:
                if order.find('.') == -1:
                    raise ProofException.ProofImproperUseException( \
                        "MalformedOrderByColumn: table_name.column_name expected but get %s." % (order) )
                
                order_column = ''
                cast_type = ''
                sort_type = ''
                m = re.match('(?iu)CAST\(([^\s]+) AS ([a-z]+(?:\([0-9]+\))?)\)(\s+desc|asc)?', order)
                if m:
                    order_column = m.group(1)
                    cast_type = m.group(2)
                    sort_type = m.group(3)
                else:
                    tokens = order.split(' ')
                    if len(tokens) > 1:
                        order_column = tokens[0]
                        sort_type = tokens[1]
                
                table_name, column_name = order_column.split('.', 1)
                table_name2 = criteria.getTableForAlias(table_name)
                if not table_name2:
                    table_name2 = table_name

                from_clause.append(table_name2)
                
                self.log("TABLE NAME 2: %s"%(table_name2))
                self.log("Column Name: %s" %(column_name))
                column = db_map.getTable(table_name2).getColumn(column_name)
                if not cast_type and column.getType() == type(""):
                    icolumn_name = adapter.ignoreCaseInOrderBy("%s.%s" % (table_name2,column_name))
                    order_by_clause.append( icolumn_name+' '+sort_type )
                    #select_clause.append("%s AS %s%s%s" % (icolumn_name, str_delimiter, icolumn_name, str_delimiter))
                else:
                    order_by_clause.append(order)
        
        limit_string = ''
        limit  = criteria.getLimit()
        if limit > 0:
            offset = criteria.getOffset()
            limit_style = adapter.getLimitStyle()
            if offset!=None and adapter.supportsNativeOffset():
                if limit_style == SQLConstants.LIMIT_STYLE_MYSQL:
                    limit_string = "%s, %s" % (offset, limit)
                elif limit_style == SQLConstants.LIMIT_STYLE_POSTGRES:
                    limit_string = "%s offset %s" % (limit, offset)
            elif adapter.supportsNativeOffset() and \
                     limit_style != SQLConstants.LIMIT_STYLE_ORACLE:
                limit_string = str(limit)
            elif limit_style == SQLConstants.LIMIT_STYLE_ORACLE:
                raise ProofException.ProofNotImplementedException( \
                                  "ORACLE support is not implemented yet!" )

        query.setSelectClause(select_clause)
        query.setFromClause(from_clause)
        query.setWhereClause(where_clause)
        query.setOrderByClause(order_by_clause)
        query.setGroupByClause(group_by_clause)
        
        #self.log("LIMIT String: %s"%(limit_string), level=logging.INFO)
        if limit_string:
            query.setLimit(limit_string)

        self.log( "return query" )
        return query

    def createQueryDisplayString(self, criteria):
        """ Method to create an SQL query for display only based on values in a
            Criteria.
            
            @param criteria A Criteria.
            @return the SQL query for display
        """
        query = self.createQuery(criteria)
        return str(query)

    def doSelect(self, criteria, ret_dict=0):
        """ Returns all results.
        
            @param criteria A Criteria.
            @return A list of rows.
        """
        self.log( "start %s doSelect" % (self.__class__.__name__) )
        
        transaction = Transaction.Transaction(self.__proof, logger=self.__logger)

        results = []
        try:
            #self.log("before tran.begin.")
            con = transaction.begin( self.__db_name,
                                     useTransaction=criteria.isUseTransaction() )
            #self.log("after tran.begin.")
            results = self.__select(criteria, con, ret_dict)
            transaction.commit()
        except:
            self.log( "Exception in doSelect: %s" % (traceBack()), logging.ERROR )
            transaction.safeRollback()

        self.log( "return %s doSelect result" % (self.__class__.__name__) )
        return results

    def __select(self, criteria, con, ret_dict=0):
        """ Returns all results.
        
            @param criteria A Criteria.
            @param con A Connection.
            @return A list of Objects this Factory represents.
        """
        sql = self.createQueryString(criteria)
        results = self.__query(sql, con, ret_dict)

        return results

    def doTotalSelect(self, criteria, count_str=None):
        """ Retrieve the total number of query records.

            @param criteria A Criteria.
            @return An integer indicating the total.
        """
        transaction = Transaction.Transaction(self.__proof, logger=self.__logger)

        total = 0
        try:
            con = transaction.begin( self.__db_name,
                                     useTransaction=criteria.isUseTransaction() )
            total = self.__select_total(criteria, con, count_str)
            transaction.commit()
        except:
            self.log( "Exception in doTotalSelect: %s" % (traceBack()), logging.ERROR )
            transaction.safeRollback()

        return total
        
    def __select_total(self, criteria, con, count_str=None):
        """ Retrieve the total number of query records.
        
            @param criteria A Criteria.
            @param con A Connection.
            @return An integer indicating the total.
        """
        sql = self.createTotalQueryString(criteria, count_str)
        result = self.__query(sql, con)
        
        return result[0][0]
    
    def doRawSelect(self, criteria, select_clause=[]):
        """ Retrieve a list of query records.

            @param criteria A Criteria.
            @return A list of record dicts.
        """
        transaction = Transaction.Transaction(self.__proof, logger=self.__logger)

        result = [[]]
        try:
            con = transaction.begin( self.__db_name,
                                     useTransaction=criteria.isUseTransaction() )
            result = self.__select_raw(criteria, con, select_clause)
            transaction.commit()
        except:
            self.log( "Exception in doRawSelect: %s" % (traceBack()), logging.ERROR )
            transaction.safeRollback()

        return result
        
    def __select_raw(self, criteria, con, select_clause=[]):
        """ Retrieve the raw query.
        
            @param criteria A Criteria.
            @param con A Connection.
            @return A list of record dicts.
        """
        sql = self.createRawQueryString(criteria, select_clause)
        return self.__query(sql, con)
        
    
    def doPageSelect(self, criteria):
        """ Return a PageSelect.

            @param criteria A Criteria.
            @return A PageSelect object.
        """
        limit = criteria.getLimit();
        
        return PageSelect.PageSelect( criteria,
                                      limit,
                                      self,
                                      logger = self.__logger )

    def getPrimaryKey(self, criteria):
        """ Helper method which returns the primary key contained
            in the given Criteria object.
            
            @return ColumnMap if the Criteria object contains a primary
                     key, or None if it doesn't.
        """
        pk = None

        keys  = criteria.keys()
        if keys:
            table_name = criteria.getTableName(keys[0])
            db_map     = self.__proof.getDatabaseMap(self.__db_name)
            table_map  = db_map.getTable(table_name)
            columns    = table_map.getColumns()
            
            for column in columns:
                if column.isPrimaryKey():
                    pk = column
                    break
        
        return pk

    # UPDATE
    #===========

    def doUpdate(self, update_criteria, where_criteria=None):
        """ Convenience method used to update rows in the DB. Checks if a
            <i>single</i> int primary key is specified in the Criteria
            object and uses it to perform the udpate. If no primary key is
            specified an Exception will be thrown.
            <p>
            Use this method for performing an update of the kind:
            <p>
            "WHERE primary_key_id = an int"
            <p>
            To perform an update with non-primary key fields in the WHERE
            clause use doUpdate(criteria, criteria).

            @param update_criteria A Criteria object containing values used in
                    set clause.
            @param where_criteria A Criteria object containing values used in
                    where clause. If none, it will try to use pk in update_criteria.
        """
        transaction = Transaction.Transaction(self.__proof, logger=self.__logger)

        result = 0
        try:
            con = transaction.begin( self.__db_name,
                                     useTransaction=update_criteria.isUseTransaction() )
            result = self.__update(update_criteria, con, where_criteria)
            transaction.commit()
        except:
            self.log( "Exception in doUpdate: %s" % (traceBack()), logging.ERROR )
            transaction.safeRollback()

        return result

    def __update(self, update_criteria, con, where_criteria=None):
        """ Convenience method used to update rows in the DB.

            @param update_criteria A Criteria object containing values used in
                    set clause.
            @param con A Connection.
            @param where_criteria A Criteria object containing values used in
                    where clause.
        """
        if where_criteria:
            assert isinstance(where_criteria, Criteria.Criteria)
        else:
            pk = self.getPrimaryKey(update_criteria)

            where_criteria = Criteria.Criteria( self.getProofInstance(),
                                                db_name     = self.getDBName(),
                                                logger      = self.getLogger() )
            if pk and update_criteria.has_key(pk.getFullyQualifiedName()):
                where_criteria[pk.getFullyQualifiedName()] = \
                          update_criteria.remove(pk.getFullyQualifiedName())

        if len(where_criteria) == 0:
            raise ProofException.ProofImproperUseException( \
                       "No where clause in %s.doUpdate." % (self.__class__.__name__) )

        results = {}

        db_map  = self.__proof.getDatabaseMap(self.__db_name)
        adapter = self.__proof.getAdapter(self.__db_name)
        sql_expr = SQLExpression.SQLExpression()
        
        tables = UniqueList.UniqueList()
        for k in where_criteria.keys():
            tables.append(where_criteria.getTableName(k))

        for table in tables:
            table_map   = db_map.getTable(table)
            column_maps = table_map.getColumns()

            where_clause = UniqueList.UniqueList()
            column_list  = []
            value_list   = []
            for column_map in column_maps:
                k = column_map.getFullyQualifiedName()
                if where_criteria.has_key(k):
                    if where_criteria.getComparison(k) == SQLConstants.CUSTOM:
                        where_clause.append(where_criteria[k])
                    else:
                        where_clause.append( sql_expr.build(
                            column_map.getColumnName(),
                            where_criteria.getValue(k),
                            where_criteria.getComparison(k),
                            where_criteria.isIgnoreCase(),
                            adapter ) )
              
                # build set clause using buildInsertList method
                if update_criteria.has_key(k):
                    column_list.append(k)
                    value_list.append(update_criteria[k])
        
            (column_list, value_list) = sql_expr.buildInsertList(column_list, value_list)

            set_clause = UniqueList.UniqueList()
            for column, value in zip(column_list, value_list):
                set_clause.append("%s=%s" % (column, value))

            set_str   = string.join(set_clause, ", ")
            where_str = string.join(where_clause, SQLConstants.AND)
            
            sql = "UPDATE %s set %s WHERE %s" % (table, set_str, where_str)

            self.log("%s.doUpdate: %s" % (self.__class__.__name__, sql))
            
            results[table] = self.__execute(sql, con)

        return results

    #=====================================================================

    def setProofInstance(self, inst):
        """ Associate this factory to a PROOF instance.
        """
        assert issubclass(inst.__class__, ProofInstance.ProofInstance)

        self.__proof = inst

    def getLogger(self):
        return self.__logger
 
    def setLogger(self, logger):
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write
 
    def __getstate__(self):
        """ Used by pickle when the class is serialized.
            Remove the 'proof', 'logger' attributes before serialization.
            @hidden
        """
        d = copy.copy(self.__dict__)
        del d["__logger"]
        del d["log"]
        del d["__proof"]
        return d
    
    def __setstate__(self, d):
        """ Used by pickle when the class is unserialized.
            Add the 'proof', 'logger' attributes.
            @hidden
        """
        logger = Logger.makeLogger(None)
        d["__logger"] = logger
        d["log"] = logger.write
        d["__proof"] = None
        self.__dict__ = d
            
