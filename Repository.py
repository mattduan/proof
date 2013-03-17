"""
This is the base class of Repository. All repositories of PROOF will be extended
from this class.

The Repository class is designed based on the Repository Pattern described in
Eric Evans's book "Domain-Driven Design". Its responsibility is to provide
aggregates objects with the same type to applications outside PROOF. It works
like a container that maintains all created aggregates and provides methods to
give different types of aggregates (by different criterias).

To make the container maintainable, it is important to garbage collect un-
frequently used aggregates from the Repository. Here we use three time-out
limits to control an aggregate lifetime. They are responding to three AGEs
in Aggregate (refer to Aggregate).

There should also have different mechanisms to keep these aggregates, like
memory, dbm, session, files, etc. These are not implemented here and will be
considered in the future.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import time
import logging
import thread

import util.logger.Logger as Logger
import util.memory as memory
from util.Import import my_import
from util.Trace import traceBack

import proof.ProofConstants as ProofConstants
import proof.pk.ObjectKey as ObjectKey
import proof.sql.Criteria as Criteria

class Repository:

    def __init__( self,
                  proof_instance,
                  db_schema    = "DB_SCHEMA",
                  aggr_name    = "AGGR_NAME",
                  relation_map = {},
                  logger       = None,
                  ):
        """ Constructor.

            @param proof_instance A ProofInstance object.
            @param db_schema A string indicating the schema name.
            @param aggr_name A string indicating the aggregate name.
            @param relation_map The relation creteria between root and included objects,
                   for example: { 'object1' : [ [left join column list], [right join column list] ],
                                  'object2' : [ [left join column list], [right join column list] ],
                                  ... }
            @param logger A Logger object.
        """
        
        # proof instance 
        self.__proof = proof_instance

        # database schema
        self.__db_schema = db_schema

        # database
        self.__db_name = self.__proof.getDBName(self.__db_schema)

        # aggregate table name
        self.__aggr_name = aggr_name

        # relationship between root and objects
        # NOTE: this will be used by both aggregate and aggregate factory
        self.__relation_map = relation_map

        # aggregate container
        self.__container = {}

        # timestamp container
        self.__timestamps = {}
        
        # a thread based session cache
        # structure: { 'thread id' : [ timestamp, { 'finger_print' : [ aggregate id list ],
        #                                           ... ... } ],
        #              ... ... }
        self.__thread_sessions = {}

        # logger
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write

        # acquire a thread lock for serializing access to its data
        self.lock = thread.allocate_lock()

    def getProofInstance(self):
        return self.__proof
    
    def getDBSchema(self):
        return self.__db_schema
    
    def getDBName(self):
        return self.__db_name

    def getRelationMap(self):
        return self.__relation_map
    
    def getRelationList(self, obj_name):
        """ Get the left/right join list from relation map.

            @param obj_name A string for an object name.
            @return A list of left and right list or None.
        """
        # abort if obj_name doesn't belong to this aggregate
        if not self.__relation_map.has_key(obj_name):
            self.log( "Abort: object '%s' doesn't belong to aggregate '%s'." % \
                      (obj_name, self.__aggr_name), logging.WARNING )
            return None

        # abort if relation_map is not defined
        relation = self.__relation_map.get(obj_name, [])
        if not relation:
            self.log( "Abort: no relationship was defined between aggregate root '%s' and object '%s'" % \
                      (self.__aggr_name, obj_name), logging.WARNING )
            return None
        
        # abort if relation map is not required format
        if len(relation) != 2 or len(relation[0]) != len(relation[1]):
            self.log( "Abort: relation map '%s' is wrong format." % \
                      (`relation`), logging.WARNING )
            return None

        return relation

    def add(self, aggregate, id=None):
        """ Add an aggregate to the container. Make sure it is thread-safe.

            @aggregate The aggregate.
        """
        if not id:
            pk = aggregate.getPK()
            id = str(pk)

        # be thread safe
        self.lock.acquire()
        try:
            #old = self.__container.get(id, None)
            #if old and old == aggregate:
            #    old.load()
            #    return old
            
            self.__container[id] = aggregate
        finally:
            self.lock.release()

        aggregate.touch()

        return aggregate

    def add_timestamp(self, timestamp, aggregate, id=None):
        """ Add an aggregate timestamp to timestamp dict. Make sure it is thread-safe.

            @param timestamp A datetime.datetime object.
            @param aggregate The aggregate.
        """
        if not id:
            pk = aggregate.getPK()
            id = str(pk)

        # be thread safe
        self.lock.acquire()
        try:
            self.__timestamps[id] = timestamp
        finally:
            self.lock.release()

    def remove(self, aggregate, id=None):
        """ Remove an aggregate from the container. Make sure it is thread-safe.

            @aggregate The aggregate.
        """
        if not id:
            pk = aggregate.getPK()
            id = str(pk)

        # be thread safe
        self.lock.acquire()
        try:
            if self.__container.has_key(id):
                del self.__container[id]
            if self.__timestamps.has_key(id):
                del self.__timestamps[id]
        finally:
            self.lock.release()

    def get(self, id):
        """ Get an aggregate from container.

            @param id An ObjectKey object or its string.
            @return An aggregate or None.
        """
        # be thread safe
        self.lock.acquire()
        try:
            return self.__container.get(str(id), None)
        finally:
            self.lock.release()

    def add_thread_session_query_result(self, finger_print, aggregates):
        """ Add a query result to a thread session. Make sure it is thread-safe.

            @param finger_print A string represents a Criteria.
            @param aggregates A list of aggregates, which is the search result by
                   Criteria identified by finger_print.
        """

        # convert aggregates to id list
        aggr_id_list = [ aggr.getPK().__str__() for aggr in aggregates ]

        thread_id = thread.get_ident()
        
        # be thread safe
        self.lock.acquire()
        try:
            if not self.__thread_sessions.has_key(thread_id):
                self.__thread_sessions[thread_id] = [ time.time(),
                                                      { finger_print : aggr_id_list } ]
            else:
                self.__thread_sessions[thread_id][0] = time.time()
                self.__thread_sessions[thread_id][1][finger_print] = aggr_id_list
        finally:
            self.lock.release()

    def touch_thread_session(self):
        """ Update timestamp for a thread session.
        """

        thread_id = thread.get_ident()
        
        # be thread safe
        self.lock.acquire()
        try:
            try:
                self.__thread_sessions[thread_id][0] = time.time()
            except:
                pass
        finally:
            self.lock.release()

    def get_thread_session_query_result(self, finger_print):
        """ Try to get a query result from thread session.
            Make sure it is thread-safe.

            @param finger_print A string represents a Criteria.

            @return A list of aggregates from the query or None.
        """
        thread_id = thread.get_ident()
        query_result = None

        # be thread safe
        self.lock.acquire()
        try:
            if self.__thread_sessions.has_key(thread_id) and \
                            self.__thread_sessions[thread_id][1].has_key(finger_print):
                query_result = self.__thread_sessions[thread_id][1][finger_print]
        finally:
            self.lock.release()
        
        if query_result != None and type(query_result) == type([]):
            aggregates = []
            for id in query_result:
                # convert id to aggregate
                aggregate = self.get(id)
                # all aggregate should exist in container
                if not aggregate:
                    return None
                aggregates.append(aggregate)
            
            # update the timestamp before return
            self.touch_thread_session()
            
            return aggregates

        return None


    def remove_thread_session(self, thread_id):
        """ Remove a thread session. Make sure it is thread-safe.

            @param thread_id A thread id.
        """
        self.lock.acquire()
        try:
            try:
                del self.__thread_sessions[thread_id]
            except:
                pass
        finally:
            self.lock.release()

    def gc(self, new_age, loaded_age, dirty_age):
        """ garbage collection on the repository.
        """
        mem = memory.memory()
        if mem > self.__proof.getMemoryThreshold():
            #self.log( "Start clean-up %s aggregates ..." % (self.__class__.__name__) )
            self.gc_container(new_age, loaded_age, dirty_age)
        #self.log( "Start clean-up %s sessions ..." % (self.__class__.__name__) )
        self.gc_session()

    def gc_container(self, new_age, loaded_age, dirty_age):
        """ garbage collection on container.
        """
        now = time.time()

        # clean up container
        for id, aggregate in self.__container.items():
                    
            # state transitions
            state = aggregate.getState()
            last_access = aggregate._access_time()

            if state == ProofConstants.AGGR_DIRTY:

                if (now - last_access) > dirty_age:
                    if aggregate.isAutoCommit():
                        aggregate.commit()
                    else:
                        self.log( "%s change was cancelled by %s.gc. The changed attributes are '%s'." % \
                                  ( str(aggregate.getPK()),
                                    self.__class__.__name__,
                                    aggregate.getDirtyAttributes() ) )
                        aggregate.cancel()

            elif state == ProofConstants.AGGR_LOADED:

                if (now - last_access) > loaded_age:
                    aggregate.unload()

            else:

                if (now - last_access) > new_age:
                    #del aggregate
                    self.remove(aggregate, id)


    def gc_session(self):
        """ garbage collection on thread_sessions.
        """

        now = time.time()

        # clean up sessions
        for thread_id, session in self.__thread_sessions.items():
            if (now - session[0]) > ProofConstants.THREAD_SESSION_LIFETIME:
                self.remove_thread_session(thread_id)
            

    def findByCriteria(self, criteria):
        """ Return aggregates based on a Criteria.

            @param criteria A Criteria object.
        """
        self.log( "start %s findByCriteria" % (self.__class__.__name__) )
        factory = self.__proof.getInstanceForAggregateFactory( self.__aggr_name,
                                                               schema=self.__db_schema )
        return factory.doSelectAggregate(criteria)

    getByCriteria = findByCriteria

    def findPageSelectByCriteria(self, criteria):
        """ Return a PageSelect object based on a Criteria.

            @param criteria A Criteria object.
        """
        factory = self.__proof.getInstanceForAggregateFactory( self.__aggr_name,
                                                               schema=self.__db_schema )
        return factory.doPageSelect(criteria)

    getPageSelectByCriteria = findPageSelectByCriteria

    def findTotalByCriteria(self, criteria):
        """ Return the total result based on the Criteria.

            @param criteria A Criteria object.
        """
        factory = self.__proof.getInstanceForAggregateFactory( self.__aggr_name,
                                                               schema=self.__db_schema )
        return factory.doTotalSelect(criteria)
    
    getTotalByCriteria = findTotalByCriteria

    def findTotal(self):
        crit = Criteria.Criteria( self.getProofInstance(),
                                  db_name     = self.getDBName(),
                                  logger      = self.getLogger() )
        return self.findTotalByCriteria(crit)

    getTotal = findTotal
    
    def findRawResultByCriteria(self, criteria, select_clause=[]):
        """ Return the raw result based on the Criteria.

            @param criteria A Criteria object.
        """
        factory = self.__proof.getInstanceForAggregateFactory( self.__aggr_name,
                                                               schema=self.__db_schema )
        return factory.doRawSelect(criteria, select_clause)

    getRawResultByCriteria = findRawResultByCriteria

    def findAll(self, order_by=None):
        """ Return All aggregates. NOTE: use with caution.
            
            @param order_by A list of order criteria in the format:
                            [ [ 'Table.ColumnName', 'asc|desc' ], ... ].
        """
        factory = self.__proof.getInstanceForAggregateFactory( self.__aggr_name,
                                                               schema=self.__db_schema )
        return factory.doSelectAll(order_by=order_by)

    getAll = findAll

    def findByPK(self, pk):
        """ Return an aggregate based on pk.

            @param pk An ObjectKey object.
        """
        assert( issubclass(pk.__class__, ObjectKey.ObjectKey) )

        self.log( "start findByPK for '%s'" % (str(pk)) )
        
        aggregate = self.__container.get(str(pk), None)
        if aggregate:
            return aggregate

        criteria = Criteria.Criteria( self.__proof,
                                      db_name = self.__db_name,
                                      logger  = self.__logger )
        pk_value = pk.getValue()
        if type(pk_value) == type([]):
            for pk in pk_value:
                criteria[pk.getFullyQualifiedName()] = pk.getValue()
        else:
            criteria[pk.getFullyQualifiedName()] = pk_value

        results = self.getByCriteria(criteria)

        #self.log( "return findByPK for '%s'" % (str(pk)) )
        if len(results) == 1:
            return results[0]
        elif len(results) > 1:
            self.log("%s.getByPK: > 1 results returned (%s)." % \
                     (self.__class__.__name__, results))
            return results[0]

        return None

    getByPK = findByPK

    def findById(self, id, col='Id'):
        """ Return an aggregate based on Id value.

            @param id An integer or string.
        """
        self.log( "start findById for '%s' from '%s'" % (id, col) )
        
        if not id:
            return None
        
        if col.find(".") != -1:
            col = col.split(".")[-1]
        column_name = "%s.%s" % (self.__aggr_name, col)
        pk = ObjectKey.ObjectKey(id, column_name=column_name)

        return self.findByPK(pk)

    getById = findById


    #===========================================================================

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
        del d["__proof"]
        del d["log"]
        del d["lock"]
        return d
    
    def __setstate__(self, d):
        """ Used by pickle when the class is unserialized.
            Add the 'proof', 'logger' attributes.
            @hidden
        """
        import thread
        d["__logger"] = None
        d["__proof"]  = None
        d["lock"]     = thread.allocate_lock()
        self.__dict__.update(d)

