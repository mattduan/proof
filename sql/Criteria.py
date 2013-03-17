"""
 This is a utility class that is used for retrieving different types
 of values from a dictionary based on a simple name string.

 NOTE: other methods will be added as needed and as time permits.
"""

__version__= '$Revision: 3212 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import string
import logging

import util.logger.Logger as Logger
import util.UniqueList as UniqueList
import proof.ProofException as ProofException
import proof.sql.SQLConstants as SQLConstants
import proof.sql.SQLExpression as SQLExpression


# constants used in this module
DEFAULT_CAPACITY = 10


class Criteria(dict):

    def __init__( self,
                  proof_instance,
                  schema_name = None,
                  db_name = None,
                  initial_capacity = DEFAULT_CAPACITY,
                  logger = None ):
        """ Constructor.

            Creates a new instance with the specified capacity which corresponds to
            the specified database.

            @param proof_instance   The PROOF instance.
            @param db_name          The dabase name.
            @param initial_capacity The initial capacity.
        """
        dict.__init__(self)
        self.__proof_instance   = proof_instance
        self.__default_capacity = initial_capacity
        self.__ignoreCase       = False
        self.__singleRecord     = False
        self.__cascade          = False
        self.__selectModifiers  = UniqueList.UniqueList()
        self.__selectColumns    = UniqueList.UniqueList()
        self.__orderByColumns   = UniqueList.UniqueList()
        self.__groupByColumns   = UniqueList.UniqueList()
        self.__having           = None # Criterion
        self.__asColumns        = {}
        self.__joinL            = []
        self.__joinR            = []

        # The name of the database schema.
        self.__original_schema_name = self.__schema_name = schema_name
        
        # The name of the database.
        self.__dbName           = ""

        # The name of the database as given in the contructor.
        self.__originalDbName   = ""

        # To limit the number of rows to return. <code>-1</code>
        # means return all rows.
        self.__limit            = -1

        # To start the results at a row other than the first one.
        self.__offset           = 0

        self.__aliases          = {}
        
        self.__useTransaction   = False
        
        # the log.
        self.__logger           = Logger.makeLogger(logger)
        self.log                = self.__logger.write

        if schema_name:
            db_name = proof_instance.getDBName(schema_name)
        
        if db_name:
            self.__originalDbName = self.__dbName = db_name
        else:
            self.__originalDbName = self.__dbName = proof_instance.getDefaultDB()

        if not self.__schema_name:
            self.__original_schema_name = self.__schema_name = \
                                          proof_instance.getSchemaName(self.__dbName)

    def clear(self):
        """ Brings this criteria back to its initial state, so that it
            can be reused as if it was new. Except if the criteria has grown in
            capacity, it is left at the current capacity.
        """
        dict.clear(self)
        self.__ignoreCase       = False
        self.__singleRecord     = False
        self.__cascade          = False
        self.__selectModifiers.clear()
        self.__selectColumns.clear()
        self.__orderByColumns.clear()
        self.__groupByColumns.clear()
        self.__having           = None
        self.__asColumns.clear()
        self.__joinL            = []
        self.__joinR            = []
        self.__dbName           = self.__originalDbName
        self.__schema_name      = self.__original_schema_name
        self.__offset           = 0
        self.__limit            = -1
        self.__aliases.clear()
        self.__useTransaction   = False

    #------------------------------------------------------------------------
    #
    # Start of get/set methods
    #
    #------------------------------------------------------------------------

    def getProofInstance(self):
        return self.__proof_instance

    def getDefaultCapacity(self):
        return self.__default_capacity

    def setDefaultCapacity(self, capacity):
        self.__default_capacity = capacity
        
    def getIgnoreCase(self):
        return self.__ignoreCase

    def setIgnoreCase(self, ignoreCase):
        """ Sets ignore case.
            
            @param ignoreCase True if case should be ignored.
            @return A modified Criteria object.
        """
        self.__ignoreCase = ignoreCase
        return self

    def getSingleRecord(self):
        return self.__singleRecord

    def setSingleRecord(self, singleRecord):
        """ Set single record?  Set this to <code>true</code> if you expect the query
            to result in only a single result record (the default behaviour is to
            throw a ProofException if multiple records are returned when the query
            is executed).  This should be used in situations where returning multiple
            rows would indicate an error of some sort.  If your query might return
            multiple records but you are only interested in the first one then you
            should be using setLimit(1).
            
            @param singleRecord set to <code>true</code> if you expect the query to select just
            one record.
            @return A modified Criteria object.
        """
        self.__singleRecord = singleRecord
        return self

    def getCascade(self):
        return self.__cascade

    def setCascade(self, cascade):
        """ Set cascade.
            
            @param cascade True if cascade is set.
            @return A modified Criteria object.
        """
        self.__cascade = cascade
        return self

    def getSelectModifiers(self):
        return self.__selectModifiers

    def setSelectModifiers(self, selectModifiers):
        if isinstance(selectModifiers, UniqueList.UniqueList):
            self.__selectModifiers = selectModifiers
        else:
            raise TypeError( """Criteria.setSelectModifiers() argument must be a UniqueList (not "%s")""" % \
                             type(selectModifiers) )
        
    def getSelectColumns(self):
        return self.__selectColumns

    def setSelectColumns(self, selectColumns):
        if isinstance(selectColumns, UniqueList.UniqueList):
            self.__selectColumns = selectColumns
        else:
            raise TypeError( """Criteria.setSelectColumns() argument must be a UniqueList (not "%s")""" % \
                             type(selectColumns) )

    def getOrderByColumns(self):
        return self.__orderByColumns

    def setOrderByColumns(self, orderByColumns):
        if isinstance(orderByColumns, UniqueList.UniqueList):
            self.__orderByColumns = orderByColumns
        else:
            raise TypeError( """Criteria.setOrderByColumns() argument must be a UniqueList (not "%s")""" % \
                             type(orderByColumns) )
        
    def getGroupByColumns(self):
        return self.__groupByColumns

    def setGroupByColumns(self, groupByColumns):
        if isinstance(groupByColumns, UniqueList.UniqueList):
            self.__groupByColumns = groupByColumns
        else:
            raise TypeError( """Criteria.setGroupByColumns() argument must be a UniqueList (not "%s")""" % \
                             type(groupByColumns) )

    def getOriginalDbName(self):
        return self.__originalDbName

    def getDbName(self):
        """ Get the Database(Map) name.
        
            @return A String with the Database(Map) name. By default, this
            is PoolBrokerService.DEFAULT.
        """
        return self.__dbName

    def setDbName(self, dbName):
        """ Set the DatabaseMap name.  If <code>NONE</code> is supplied, uses value
            provided by <code>Proof.getDefaultDB()</code>.
        
            @param dbName A String with the Database(Map) name.
        """
        if dbName:
            self.__dbName = string.strip(dbName)
        else:
            self.__dbName = self.__proof_instance.getDefaultDB()
        self.__schema_name = self.__proof_instance.getSchemaName(self.__dbName)

    def getSchemaName(self):
        return self.__schema_name

    def setSchemaName(self, schema_name):
        self.__schema_name = schema_name
        self.__dbName = self.__proof_instance.getDBName(schema_name)
    
    def getLimit(self):
        return self.__limit

    def setLimit(self, limit):
        """ Set limit.
            
            @param limit An int with the value for limit.
            @return A modified Criteria object.
        """
        self.__limit = limit
        return self

    def getOffset(self):
        return self.__offset

    def setOffset(self, offset):
        """ Set offset.
        
            @param offset An int with the value for offset.
            @return A modified Criteria object.
        """
        self.__offset = offset
        return self

    def getAlias(self):
        return self.__aliases

    def setAlias(self, alias):
        self.__aliases = alias

    def getAsColumns(self):
        """ Get the column aliases.
            
            @return A dictionary which map the column alias names
            to the alias clauses.
        """
        return self.__asColumns

    def setAsColumns(self, asColumns):
        for column_name, alias in asColumns.items():
            self.addAsColumn( column_name, alias )

    def getUseTransaction(self):
        return self.__useTransaction
    
    def setUseTransaction(self, bool):
        """ Will force the sql represented by this criteria to be executed within
            a transaction.
        """
        self.__useTransaction = bool

    def getHaving(self):
        """ Get Having Criterion.
     
            @return A Criterion that is the having clause.
        """
        return self.__having

    def setHaving(self, having):
        """ This method adds a prepared Criterion object to the Criteria as a having
            clause. You can get a new, empty Criterion object with the
            getNewCriterion() method.
            
            <p>
            <code>
            Criteria crit = Criteria()
            c = crit.getNewCriterion(BasePeer.ID, 5, SQLConstants.LESS_THAN)
            crit.addHaving(c)
            </code>
            
            @param having A Criterion object
            @return A modified Criteria object.
        """
        if isinstance(having, Criterion):
            self.__having = having
            return self
        else:
            raise TypeError( """Criteria.setHaving argument must be a Criterion (not "%s")""" % \
                             type(having) )

    def getJoinL(self):
        return self.__joinL

    def setJoinL(self, joinL):
        self.__joinL = joinL

    def getJoinR(self):
        return self.__joinR

    def setJoinR(self, joinR):
        self.__joinR = joinR

    def setLogger(self, logger):
        self.__logger = logger
        
    #------------------------------------------------------------------------
    
    def addAsColumn(self, name, clause):
        """ Add an AS clause to the select columns. Usage:
            <p>
            <code>
            
            myCrit = Criteria()
            myCrit.addAsColumn("alias", "ALIAS("+MyPeer.ID+")")
            
            </code>
            
            @param name  wanted Name of the column
            @param clause SQL clause to select from the table
            
            If the name already exists, it is replaced by the new clause.
            
            @return A modified Criteria object.
        """
        self.__asColumns[name] = clause
        return self

    def addAlias(self, alias, table):
        """ Allows one to specify an alias for a table that can
            be used in various parts of the SQL.
            
            @param alias a <code>String</code> value
            @param table a <code>String</code> value
        """
        self.__aliases[alias] = table

    def getTableForAlias(self, alias):
        """ Returns the table name associated with an alias.
            
            @param alias a <code>String</code> value
            @return a <code>String</code> value
        """
        self.__aliases.get(alias)

    def isUseTransaction(self):
        return self.__useTransaction

    def getCriterion(self, column, table=None):
        """  Method to return criteria related to columns in a table.
             
             @param column String name of column.
             @return A Criterion.
        """
        if table and column.find(".") == -1:
            return dict.get( self, "%s.%s" % (table, column) )
        else:
            return dict.get( self, column )

    def getNewCriterion( self,
                         column,
                         value,
                         table=None,
                         comparison=SQLConstants.EQUAL ):
        """ Method to return criterion that is not added automatically
            to this Criteria.  This can be used to chain the
            Criterions to form a more complex where clause.
            
            @param table String name of table.
            @param column String name of column.
            @return A Criterion.
        """
        return Criterion(self, column, value, table=table, comparison=comparison)

    def add( self,
             column,
             value,
             table = None,
             comparison = SQLConstants.EQUAL ):
        """ This method adds a new criterion to the list of criterias.
            If a criterion for the requested column already exists, it is
            replaced. If is used as follows:
            
            <p>
            <code>
            Criteria crit = Criteria().add(&quot;column&quot;,
                                             &quot;value&quot;,
                                             &quot;table&quot;,
                                             &quot;SQLConstants.GREATER_THAN&quot;)
            </code>

            Any comparison can be used.
            @param table Name of table which contains the column
            @param column The column to run the comparison on
            @param value An Object.
            @param comparison String describing how to compare the column with
                   the value
            @return A modified Criteria object.
        """
        criterion = Criterion(self, column, value, table=table, comparison=comparison)
        self.addCriterion(criterion)
        return self

    def addCriterion( self, criterion, key=None ):
        """ This method adds a prepared Criterion object to the Criteria.
            You can get a new, empty Criterion object with the
            getNewCriterion() method. If a criterion for the requested column
            already exists, it is replaced. This is used as follows:
            
            <p>
            <code>
            crit = Criteria()
            c = crit.getNewCriterion(BasePeer.ID, 5, SQLConstants.LESS_THAN)
            crit.add(c)
            </code>
            
            @param criterion A Criterion object
            @param key A alternative key for the Object
            @return A modified Criteria object.
        """
        assert isinstance(criterion, Criterion)
        # call super
        if key:
            dict.__setitem__( self,
                              key,
                              criterion )
        else:
            dict.__setitem__( self,
                              "%s.%s" % (criterion.getTable(), criterion.getColumn()),
                              criterion )
        return self
    
    def getColumnName(self, name):
        """ Method to return a String table name.
            
            @param name A String with the name of the key.
            @return A String with the value of the object at key.
        """
        return self.getCriterion(name).getColumn()

    def getComparison(self, column, table=None):
        """ Method to return a comparison String.
            
            @param column String name of the column.
            @param table String name of the table.
            @return A String with the value of the object at key.
        """
        if table and column.find(table) != 0:
            return self.getCriterion("%s.%s"%(table, column)).getComparison()
        else:
            return self.getCriterion(column).getComparison()

    def getValue(self, column, table=None):
        """ Convenience method to return a Value.
     
            @param table String name of table.
            @param column String name of column.
            @return A value with the Criterion at column.
        """
        if table and column.find(table) != 0:
            return self.getCriterion("%s.%s"%(table, column)).getValue()
        else:
            return self.getCriterion(column).getValue()

    def getTableName(self, name):
        """ Method to return a String table name.
            
            @param name A String with the name of the key.
            @return A String with the value of object at key.
        """
        return self.getCriterion(name).getTable()

    def __getitem__(self, key):
        """ Overrides dict get, so that the value placed in the
            Criterion is returned instead of the Criterion.
            
            @param key An Object.
            @return An Object.
        """
        if self.has_key(key):
            return self.getValue(key)
        else:
            raise KeyError("Criteria[%s]" % (key))

    def get(self, key, default=None):
        """ Overrides dict get, so that the value placed in the
            Criterion is returned instead of the Criterion.
            
            @param key An Object.
            @return An Object.
        """
        if self.has_key( key ):
            return self.__getitem__(key)
        else:
            return default

    def __setitem__(self, key, value):
        """ Overrides dict set, so that this object is returned
            instead of the value previously in the Criteria object.
            The reason is so that it more closely matches the behavior
            of the add() methods. If you want to get the previous value
            then you should first Criteria.get() it yourself. Note, if
            you attempt to pass in an Object that is not a String, it will
            throw a NPE. The reason for this is that none of the add()
            methods support adding anything other than a String as a key.
            
            @param key An Object. Must be instanceof String!
            @param value An Object.
            @return Instance of self.
        """
        return self.add(key, value)

    def addDict(self, other):
        """ Copies all of the mapping from the specified dictionary to this Criteria
            These mappings will replace any mappings that this Criteria had for any
            of the keys currently in the specified dictionary.
            
            if the map was another Criteria, its attributes are copied to this
            Criteria, overwriting previous settings.
            
            @param other The dictionary to be stored in this Criteria.
        """
        for key, value in other.items():
            if isinstance(value, Criterion):
                self.addCriterion(value, key)
            else:
                self.add(key, value)

        if isinstance(other, Criteria):
            self.setJoinL( other.getJoinL() )
            self.setJoinR( other.getJoinR() )

            """
            #this would make a copy, not included
            #but might want to use some of it.
            self.__ignoreCase      = other.getIgnoreCase()
            self.__singleRecord    = other.getSingleRecord()
            self.__cascade         = other.getCascade()
            self.__selectModifiers = other.getSelectModifiers()
            self.__selectColumns   = other.getSelectColumns()
            self.__orderByColumns  = other.getOrderByColumns()
            self.__dbName          = other.getDbName()
            self.__limit           = other.getLimit()
            self.__offset          = other.getOffset()
            self.__aliases         = other.getAliases()
            """

        return self

    def addJoin(self, left, right):
        """ This is the way that you should add a join of two tables.
            For example:
            
            <p>
            AND PROJECT.PROJECT_ID=FOO.PROJECT_ID
            <p>
            
            left = PROJECT.PROJECT_ID
            right = FOO.PROJECT_ID
            
            @param left A String with the left side of the join.
            @param right A String with the right side of the join.
            @return A modified Criteria object.
        """
        self.__joinL.append(left)
        self.__joinR.append(right)

        return self

    def addIn(self, column, vars, table=None):
        """ Adds an 'IN' clause with the criteria supplied as a List.
            For example:
            
            <p>
            FOO.NAME IN ('FOO', 'BAR', 'ZOW')
            <p>

            where 'vars' contains three objects that evaluate to the
            respective strings.
            
            If a criterion for the requested column already exists, it is
            replaced.
            
            @param column The column to run the comparison on
            @param vars A List with the allowed values.
            @return A modified Criteria object.
        """
        if type(vars) != type([]):
            vars = [vars]
        self.add(column, vars, table=table, comparison=SQLConstants.IN)
        return self

    def addNotIn(self, column, vars, table=None):
        """ Adds a 'NOT IN' clause with the criteria supplied as an int
            array.  For example:
            
            <p>
            FOO.ID NOT IN ('2', '3', '7')
            <p>
            
            where 'vars' contains those three integers.
            
            If a criterion for the requested column already exists, it is
            replaced.
            
            @param column The column to run the comparison on
            @param vars An int[] with the disallowed values.
            @return A modified Criteria object.
        """
        if type(vars) != type([]):
            vars = [vars]
        self.add(column, vars, table=table, comparison=SQLConstants.NOT_IN)
        return self

    def setAll(self):
        """ Adds "ALL " to the SQL statement.
        """
        self.__selectModifiers.append( SQLConstants.ALL )

    def setDistinct(self):
        self.__selectModifiers.append( SQLConstants.DISTINCT )

    isIgnoreCase = getIgnoreCase

    isSingleRecord = getSingleRecord

    isCascade = getCascade

    def addSelectColumn(self, name):
        """ Add select column.
            
            @param name A String with the name of the select column.
            @return A modified Criteria object.
        """
        self.__selectColumns.append(name)
        return self

    def addGroupByColumn(self, groupBy):
        """ Add group by column name.
        
            @param groupBy The name of the column to group by.
            @return A modified Criteria object.
        """
        self.__groupByColumns.append(groupBy)
        return self

    def addAscendingOrderByColumn(self, name):
        """ Add order by column name, explicitly specifying ascending.
        
            @param name The name of the column to order by.
            @return A modified Criteria object.
        """
        self.__orderByColumns.append("%s %s" % (name, SQLConstants.ASC))
        return self

    def addDescendingOrderByColumn(self, name):
        """ Add order by column name, explicitly specifying descending.
        
            @param name The name of the column to order by.
            @return A modified Criteria object.
        """
        self.__orderByColumns.append("%s %s" % (name, SQLConstants.DESC))
        return self

    addHaving = setHaving

    def remove(self, key):
        """ Remove an object from the criteria.
        
            @param key A String with the key to be removed.
            @return The removed object value.
        """
        #removed = dict.get(self, key, None)
        removed = self.get(key, None)
        if removed:
            dict.__delitem__(self, key)
        return removed

    def __str__(self):
        """ Build a string representation of the Criteria.
     
            @return A String with the representation of the Criteria.
        """
        s = "Criteria:\n"
        for k in self.keys():
            s += "    %s <=> %s\n" % (k, self[k])
        s += "\n"

        s += "Current Query SQL (may not be complete or applicable):\n"
        import proof.BaseFactory as BaseFactory
        factory = BaseFactory.BaseFactory(self.__proof_instance, self.__schema_name)
        s += factory.createQueryDisplayString(self)

        return s

    def __repr__(self):
        return "<%s instance at 0x%x>" % (
            self.__class__.__name__,
            id(self) )

    def __eq__(self, crit):
        """ This method checks another Criteria to see if they contain
            the same attributes and key/value entries.
        """
        if crit is self:
            return True

        if isinstance(crit, self.__class__) and \
               len(crit) == len(self) and \
               self.__offset == crit.getOffset() and \
               self.__limit  == crit.getLimit() and \
               self.__ignoreCase == crit.isIgnoreCase() and \
               self.__singleRecord == crit.isSingleRecord() and \
               self.__cascade == crit.isCascade() and \
               self.__dbName == crit.getDbName() and \
               self.__selectModifiers == crit.getSelectModifiers() and \
               self.__selectColumns == crit.getSelectColumns() and \
               self.__orderByColumns == crit.getOrderByColumns() and \
               self.__having == crit.getHaving() and \
               self.__joinL == crit.getJoinL() and \
               self.__joinR == crit.getJoinR():
            
            for k in crit.keys():
                if not self.has_key(k) or \
                       not self.getCriterion(k) == crit.getCriterion(k):
                    return Flase

            return True

        return False

    def __finger_print__(self):
        """ An unique string used to represent this object.
        """
        s = "%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s" % ( len(self),
                                                         self.__offset,
                                                         self.__limit,
                                                         self.__ignoreCase,
                                                         self.__singleRecord,
                                                         self.__cascade,
                                                         self.__dbName,
                                                         self.__selectModifiers,
                                                         self.__selectColumns,
                                                         self.__orderByColumns,
                                                         self.__having,
                                                         self.__joinL,
                                                         self.__joinR )
        for k in self.keys():
            criterion = self.getCriterion(k)
            s += ":%s:%s" % (k, criterion.__finger_print__())

        return s
    
        
    #------------------------------------------------------------------------
    #
    # Start of the "and" methods
    #
    #------------------------------------------------------------------------
    
    def andCriterion(self, criterion):
        """ This method adds a prepared Criterion object to the Criteria.
            You can get a new, empty Criterion object with the
            getNewCriterion() method. If a criterion for the requested column
            already exists, it is "AND"ed to the existing criterion.
            This is used as follows:
            
            <p>
            <code>
            crit = Criteria()
            c = crit.getNewCriterion(BasePeer.ID, 5, SQLConstants.LESS_THAN)
            crit.andCriterion(c)
            </code>

            @param c A Criterion object
            @return A modified Criteria object.
        """
        oc = self.getCriterion( "%s.%s" % ( criterion.getTable(),
                                            criterion.getColumn() ) )

        if oc:
            oc.andCriterion( criterion )
        else:
            self.addCriterion( criterion )

        return self

    def and_( self,
              column,
              value,
              table = None,
              comparison = SQLConstants.EQUAL ):
        """  This method adds a new criterion to the list of criterias.
             If a criterion for the requested column already exists, it is
             "AND"ed to the existing criterion. If is used as follows:
             
             <p>
             <code>
             crit = Criteria().and_(&quot;column&quot;,
                                     &quot;value&quot;,
                                     &quot;table&quot;,
                                     &quot;SQLConstants.GREATER_THAN&quot;);
             </code>
             
             Any comparison can be used.
             
             @param table Name of table which contains the column
             @param column The column to run the comparison on
             @param value An Object.
             @param comparison String describing how to compare the column with
             the value
             @return A modified Criteria object.
        """
        oc = self.getCriterion(column, table=table)
        nc = Criterion(self, column, value, table=table, comparison=comparison)

        if oc:
            oc.andCriterion( nc )
        else:
            self.addCriterion( nc )

        return self

    def andIn(self, column, vars, table=None):
        """ Adds an 'IN' clause with the criteria supplied as an Object array.
            For example:
            
            <p>
            FOO.NAME IN ('FOO', 'BAR', 'ZOW')
            <p>
            
            where 'vars' contains three objects that evaluate to the
            respective strings.
            
            If a criterion for the requested column already exists, it is
            "AND"ed to the existing criterion.
            
            @param column The column to run the comparison on
            @param vars An Object[] with the allowed values.
            @return A modified Criteria object.
        """
        if type(vars) != type([]):
            vars = [vars]
        self.and_(column, vars, table=table, comparison=SQLConstants.IN)

        return self

    def andNotIn(self, column, vars, table=None):
        if type(vars) != type([]):
            vars = [vars]
        self.and_(column, vars, table=table, comparison=SQLConstants.NOT_IN)
        return self

    #------------------------------------------------------------------------
    #
    # Start of the "or" methods
    #
    #------------------------------------------------------------------------

    def orCriterion(self, criterion):
        """ This method adds a prepared Criterion object to the Criteria.
            You can get a new, empty Criterion object with the
            getNewCriterion() method. If a criterion for the requested column
            already exists, it is "OR"ed to the existing criterion.
            This is used as follows:
            
            <p>
            <code>
            crit = Criteria()
            c = crit.getNewCriterion(BasePeer.ID, 5, SQLConstants.LESS_THAN)
            crit.orCriterion(c)
            </code>
            
            @param c A Criterion object
            @return A modified Criteria object.
        """
        oc = self.getCriterion( "%s.%s" % ( criterion.getTable(),
                                            criterion.getColumn() ) )

        if oc:
            oc.orCriterion( criterion )
        else:
            self.addCriterion( criterion )

        return self

    def or_( self,
             column,
             value,
             table = None,
             comparison = SQLConstants.EQUAL ):
        """ This method adds a new criterion to the list of criterias.
            If a criterion for the requested column already exists, it is
            "OR"ed to the existing criterion. If is used as follows:
            
            <p>
            <code>
            crit = Criteria().or_(&quot;column&quot;,
                                   &quot;value&quot;,
                                   &quot;table&quot;,
                                   &quot;SQLConstants.GREATER_THAN&quot;)
            </code>
            
            Any comparison can be used.
            
            @param table Name of table which contains the column
            @param column The column to run the comparison on
            @param value An Object.
            @param comparison String describing how to compare the column with the value
            @return A modified Criteria object.
        """
        oc = self.getCriterion(column, table=table)
        nc = Criterion(self, column, value, table=table, comparison=comparison)
        
        if oc:
            oc.orCriterion( nc )
        else:
            self.addCriterion( nc )
            
        return self

    def orIn(self, column, vars, table=None):
        """ Adds an 'IN' clause with the criteria supplied as an Object
            array.  For example:
            
            <p>
            FOO.NAME IN ('FOO', 'BAR', 'ZOW')
            <p>
            
            where 'values' contains three objects that evaluate to the
            respective strings above when .toString() is called.
            
            If a criterion for the requested column already exists, it is
            "OR"ed to the existing criterion.
            
            @param column The column to run the comparison on
            @param values An Object[] with the allowed values.
            @return A modified Criteria object.
        """
        if type(vars) != type([]):
            vars = [vars]
        self.or_(column, vars, table=table, comparison=SQLConstants.IN)

        return self

    def orNotIn(self, column, vars, table=None):
        if type(vars) != type([]):
            vars = [vars]
        self.or_(column, vars, table=table, comparison=SQLConstants.NOT_IN)
        return self

    #------------------------------------------------------------------------


##
# This is an inner class that describes an object in the
# criteria.
#

class Criterion:

    def __init__( self,
                  criteria,
                  column,
                  value,
                  table = None,
                  comparison = SQLConstants.EQUAL ):

        # A reference to criteria
        self.__criteria = criteria

        # value of the CO.
        self.__value = value

        # comparison value.
        if comparison in SQLConstants.COMPARISON_LIST or \
                comparison == SQLConstants.CUSTOM:
            self.__comparison = comparison
        else:
            raise ProofException.ProofImproperUseException( \
                "Criterion.__init__(): comparison %s is not in %s!" % \
                (comparison, SQLConstants.COMPARISON_LIST) )

        # table/column name.
        if string.find(column, '.') > 0:
            self.__table, self.__column = column.split('.', 1)
        elif table:
            self.__table  = table
            self.__column = column
        else:
            raise ProofException.ProofImproperUseException( \
                "Criterion.__init__(): no table name is found!" )

        # flag to ignore case in comparision
        self.__ignoreCase = False
        
        # The DB adaptor which might be used to get db specific
        # variations of sql.
        self.__db = None
        
        # other connected criteria and their conjunctions.
        self.__clauses = []
        self.__conjunctions = []

    def getCriteria(self):
        """ Get the Criteria.

            @return A Criteria object.
        """
        return self.__criteria

    def setCriteria(self, criteria):
        """ Set the Criteria.

            @param criteria A Criteria object.
        """
        assert isinstance(criteria, Criteria)
        self.__criteria = criteria

    def getColumn(self):
        """ Get the column name.
         
            @return A String with the column name.
        """
        return self.__column

    def setTable(self, name):
        """ Set the table name.
         
            @param name A String with the table name.
        """
        self.__table = name

    def getTable(self):
        """ Get the table name.
         
            @return A String with the table name.
        """
        return self.__table

    def getComparison(self):
        """ Get the comparison.
         
            @return A String with the comparison.
        """
        return self.__comparison

    def getValue(self):
        """ Get the value.
        
            @return An Object with the value.
        """
        return self.__value

    def getDb(self):
        """ Get the value of db.
            The DB adaptor which might be used to get db specific
            variations of sql.
            @return value of db.
        """
        db = self.__db
        if not db:
            try:
                db = self.__criteria.getProofInstance().getDB(self.__criteria.getDbName())
            except:
                self.__criteria.log( "Could not get a DB adapter, so sql may be wrong",
                                     logging.WARNING )
        return db

    getAdapter = getDb

    def setDb(self, db):
        """ Set the value of db.
            The DB adaptor might be used to get db specific
            variations of sql.
            @param db  Value to assign to db.
        """
        self.__db = db
        for clause in self.__clauses:
            clause.setDb(db)

    setAdapter = setDb

    def setIgnoreCase(self, bool):
        """ Sets ignore case.
         
            @param b True if case should be ignored.
            @return A modified Criteria object.
        """
        self.__ignoreCase = bool
        return self

    def getIgnoreCase(self):
        """ Is ignore case on or off?
        
            @return True if case is ignored.
        """
        return self.__ignoreCase

    isIgnoreCase = getIgnoreCase

    def getClauses(self):
        """ get the list of clauses in this Criterion
        """
        return self.__clauses

    def getConjunctions(self):
        """ get the list of conjunctions in this Criterion
        """
        return self.__conjunctions

    def andCriterion(self, criterion):
        """ Append an AND Criterion onto this Criterion's list.
        """
        assert isinstance(criterion, self.__class__)
        self.__clauses.append(criterion)
        self.__conjunctions.append(SQLConstants.AND)
        return self

    def orCriterion(self, criterion):
        """ Append an OR Criterion onto this Criterion's list.
        """
        assert isinstance(criterion, self.__class__)
        self.__clauses.append(criterion)
        self.__conjunctions.append(SQLConstants.OR)
        return self

    def __str__(self):
        """ The string representation of the Criterion.
        """
        if not self.__column:
            return ""

        s = "(" * len(self.__clauses)

        if self.__comparison == SQLConstants.CUSTOM:
            if self.__value:
                s += self.__value
        else:
            field = "%s.%s" % (self.__table, self.__column)
            sql_expr = SQLExpression.SQLExpression()
            s += sql_expr.build( field,
                                 self.__value,
                                 self.__comparison,
                                 self.__ignoreCase,
                                 self.getDb() )

        for i in range(0, len(self.__clauses)):
            s += self.__conjunctions[i]
            s += self.__clauses[i].__str__()
            s += ")"

        return s

    def __repr__(self):
        return "<%s instance at 0x%x>" % (
            self.__class__.__name__,
            id(self) )

    def __eq__(self, criterion):
        """ This method checks another Criteria to see if they contain
            the same attributes and dict entries.
        """
        if criterion is self:
            return True

        if criterion and \
           isinstance(criterion, self.__class__) and \
           self.__table == criterion.getTable() and \
           self.__column == criterion.getColumn() and \
           self.__comparison == criterion.getComparison() and \
           self.__value == criterion.getValue() and \
           self.__clauses == criterion.getClauses():
            return True

        return False

    equals = __eq__

    def __finger_print__(self):
        """ A string can be used to uniquely represent this object.
        """
        s = "%s:%s:%s:%s" % ( self.__table,
                              self.__column,
                              self.__comparison,
                              self.__value )
        
        for i in range(0, len(self.__clauses)):
            s += ":%s" % (self.__conjunctions[i])
            s += ":%s" % (self.__clauses[i].__finger_print__())

        return s

    def getAllTables(self):
        """ get all tables from nested criterion objects
            
            @return the list of tables
        """
        tables = UniqueList.UniqueList()
        tables.append( self.__table )
        for clause in self.__clauses:
            tables += clause.getAllTables()

        return tables

    def getAttachedCriterion(self):
        """ get an array of all criterion attached to this
            recursing through all sub criterion
        """
        crits = []
        crits.append( self )
        for clause in self.__clauses:
            crits += clause.getAttachedCriterion()

        return crits
