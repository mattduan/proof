"""
The core of PROOF's implementation. The class is the basic unit of the PROOF.
All aggregates, repositories and factories in PROOF are using this class as
their backbone.

This class maintains the connection pools and repositories for each database.
It provides all the necessary resources by delegating the calls to a
ProofResource object.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"

import sys
import logging
import time
import thread
import threading

import util.logger.Logger as Logger
import util.memory as memory
from util.Import import my_import
from util.Trace import traceBack

import proof.pool.ConnectionPool as ConnectionPool
import proof.ProofResource as ProofResource
import proof.ProofConstants as ProofConstants


class ProofInstance:

    def __init__( self,
                  resource,
                  namespace,
                  aggr_new_age     = None,
                  aggr_loaded_age  = None,
                  aggr_dirty_age   = None,
                  gc_interval      = None,
                  mem_threshold    = None,
                  logger           = None ):
        """ Constructor.

            @param resource ProofResource object.
            @param namespace A string of valid namespace, e.g. domainname.
            @param aggr_new_age An integer in seconds. Refer to Aggregate.py.
            @param aggr_loaded_age An integer in seconds. Refer to Aggregate.py.
            @param aggr_dirty_age An integer in seconds. Refer to Aggregate.py.
            @param gc_interval An integer in seconds. It is the monitor cycle to
                   clean-up caches in repositories.
            @param mem_threshold An integer in bytes. It is the memory threshold for
                   the monitor to do the clean-up work.
            @param logger A logger object.
        """
        
        assert(issubclass(resource.__class__, ProofResource.ProofResource))

        self.__resource = resource

        self.__namespace = namespace

        self.__logger = Logger.makeLogger(logger)
        
        # a shortcut function for logger
        self.log = self.__logger.write

        # repository pool
        self.__repository_pool = {}

        # connection pool
        self.__connection_pool = {}
    
        # new age
        if self.__is_age(aggr_new_age):
            self.__aggr_new_age = aggr_new_age
        else:
            self.__aggr_new_age = ProofConstants.DEFAULT_AGGR_NEW_AGE

        # loaded age
        if self.__is_age(aggr_loaded_age):
            self.__aggr_loaded_age = aggr_loaded_age
        else:
            self.__aggr_loaded_age = ProofConstants.DEFAULT_AGGR_LOADED_AGE

        # dirty age
        if self.__is_age(aggr_dirty_age):
            self.__aggr_dirty_age = aggr_dirty_age
        else:
            self.__aggr_dirty_age = ProofConstants.DEFAULT_AGGR_DIRTY_AGE


        # gc interval
        if type(gc_interval) == type(1) and \
               gc_interval > ProofConstants.MIN_REPOSITORY_GC_INTERVAL:
            self.__gc_interval = gc_interval
        else:
            self.__gc_interval = ProofConstants.DEFAULT_REPOSITORY_GC_INTERVAL

        # memory threshold
        if type(mem_threshold) == type(1):
            self.__mem_threshold = mem_threshold
        else:
            self.__mem_threshold = ProofConstants.DEFAULT_MEMORY_THRESHOLD

        # acquire a thread lock for serializing access to its data.
        self.lock = thread.allocate_lock()

        self.initPath()

        # recycling monitor thread
        self.__monitor = None

        if self.__gc_interval > 0:
            self.log( "Starting %s for '%s' Monitor Thread with GC Interval %s Seconds" % \
                      (self.__class__.__name__, self.__namespace, self.__gc_interval), logging.DEBUG )
 
            # Create monitor thread
            self.__monitor = _Monitor(self)
 
            self.__monitor.setDaemon(True)
            self.__monitor.start()

    def initPath(self):
        path = self.__resource.getProofPath()
        if path and path not in sys.path:
            sys.path += [path]

    def getDefaultDB(self):
        return self.__resource.getDefaultDatabase(self.__namespace)

    def getDefaultSchema(self):
        return self.__resource.getDefaultSchema()
    
    def getNamespace(self):
        return self.__namespace

    def getSchemaName(self, database):
        return self.__resource.getSchemaName(database)

    def getDBName(self, schema):
        return self.__resource.getDatabaseName(schema=schema, namespace=self.__namespace)

    def getAdapter(self, database):
        return self.__resource.getAdapter(database)

    def getDatabaseMap(self, database):
        return self.__resource.getDatabaseMap(database)

    def getClassForObject(self, table, schema=None):
        return self.__resource.getClassForObject(schema, table)

    def getModuleForObject(self, table, schema=None):
        return self.__resource.getModuleForObject(schema, table)

    def getClassForAggregate(self, table, schema=None):
        return self.__resource.getClassForAggregate(schema, table)

    def getModuleForAggregate(self, table, schema=None):
        return self.__resource.getModuleForAggregate(schema, table)

    def getClassForFactory(self, table, schema=None):
        return self.__resource.getClassForFactory(schema, table)

    def getModuleForFactory(self, table, schema=None):
        return self.__resource.getModuleForFactory(schema, table)

    def getClassForAggregateFactory(self, table, schema=None):
        return self.__resource.getClassForAggregateFactory(schema, table)

    def getModuleForAggregateFactory(self, table, schema=None):
        return self.__resource.getModuleForAggregateFactory(schema, table)

    def getClassForRepository(self, table, schema=None):
        return self.__resource.getClassForRepository(schema, table)

    def getModuleForRepository(self, table, schema=None):
        return self.__resource.getModuleForRepository(schema, table)
    
    def getInstanceForRepository( self,
                                  aggregate_name,
                                  schema      = None,
                                  new_age     = None,
                                  loaded_age  = None,
                                  dirty_age   = None,
                                  gc_interval = None ):
        #self.log("start get repository instance for '%s'" % (aggregate_name))

        db_name = self.getDBName(schema)
        
        # make this thread safe
        self.lock.acquire()
        try:
            if not self.__repository_pool.has_key(db_name):
                self.__repository_pool[db_name] = {}
        
            if not self.__repository_pool[db_name].has_key(aggregate_name):
                # create a new repository
                module_name = self.getModuleForRepository(aggregate_name, schema=schema)
                class_name  = self.getClassForRepository(aggregate_name, schema=schema)
                module = my_import(module_name)
                obj    = getattr(module, class_name)
                
                self.__repository_pool[db_name][aggregate_name] = obj(self,
                                                                      logger = self.__logger)
        finally:
            self.lock.release()

        #self.log("return repository instance for '%s'" % (aggregate_name))
        return self.__repository_pool[db_name][aggregate_name]

    def getInstanceForAggregateFactory( self,
                                        aggregate_name,
                                        schema = None ):
        #self.log("start get factory instance for '%s'" % (aggregate_name))
        module_name = self.getModuleForAggregateFactory(aggregate_name, schema=schema)
        class_name  = self.getClassForAggregateFactory(aggregate_name, schema=schema)
        module = my_import(module_name)
        obj    = getattr(module, class_name)

        #exec("factory = module.%s(self,logger=self.__logger)" % (class_name))

        factory = obj(self, logger=self.__logger)
        
        #self.log("return factory instance for '%s'" % (aggregate_name))
        return factory

    def getConnection(self, database=None, log_interval=0):
        #self.log("start get connection for '%s'" % (database))

        db_name = self.getDBName(database)
        
        # make this thread safe
        self.lock.acquire()
        try:
            if not self.__connection_pool.has_key(db_name) or \
                   not self.__connection_pool[db_name]:
                db_config = self.__resource.getDBConf(db_name, self.__namespace)
                host     = db_config['host']
                username = db_config['username']
                password = db_config['password']
                adapter  = self.getAdapter(db_name)
                
                con = ConnectionPool.ConnectionPool( host, username, password, db_name,
                                                     adapter,
                                                     logger        = self.__logger,
                                                     log_interval  = log_interval )
            
                self.__connection_pool[db_name] = con
            elif self.__connection_pool[db_name].getLogInterval() != log_interval:
                self.__connection_pool[db_name].setLogInterval(log_interval)

        finally:
            self.lock.release()
        
        self.log("return connection for '%s'" % (database))
        return self.__connection_pool[db_name].getConnection()
    
    def closeConnection(self, con):
        con.close()

    def getSelectAllLimit(self):
        """ Return an integer value indicating the maximum limit when calling
            <code>doSelectAll</code> on a table.
            Each PROOF implementation should override this value if necessary.
        """
        return ProofConstants.DEFAULT_SELECTALL_LIMIT

    def getGCInterval(self):
        return self.__gc_interval

    def gc(self):
        """ Loop through all repositories and do housekeeping work.
        """
        for db in self.__repository_pool.keys():
            for repository in self.__repository_pool[db].values():
                repository.gc( self.__aggr_new_age,
                               self.__aggr_loaded_age,
                               self.__aggr_dirty_age )

    def getMemoryThreshold(self):
        return self.__mem_threshold

    def setMemoryThreshold(self, threshold):
        if type(threshold) == type(1):
            self.__mem_threshold = threshold
    
    def __del__(self):
        gc_interval = self.__gc_interval
        self.__gc_interval = 0

        # stop monitor
        while self.__monitor.isAlive():
            time.sleep(3)

        self.__monitor = None

        # delete this
        self = None

    def __is_age(self, age):
        if type(age) == type(1) and age >= 0:
            return True
        else:
            return False


#============================================================================
# This inner class monitors the PROOF's Repositories.
#
# This class is capable of recycling some unfrequently used aggregates in
# the repository periodically. 
#
# Set the <code>gc_interval</code> property of your repository definition
# to the number of seconds you want to elapse between recycling processes.
#============================================================================
 
class _Monitor(threading.Thread):
 
    def __init__(self, proof):
        threading.Thread.__init__(self)
        self.__is_running = False
        self.__proof = proof
     
    def run(self):
        self.__is_running = True
        
        __gc_interval = self.__proof.getGCInterval()
        while __gc_interval > 0 and self.__is_running:
            #self.__proof.log( "Start monitoring ..." )
            try:
                time.sleep(__gc_interval)
 
                self.__proof.gc()

                # stop monitoring in case gc_interval set to 0 dynamically
                __gc_interval = self.__proof.getGCInterval()
            except:
                self.__proof.log( "ProofInstance monitor exception:\n%s" % (traceBack()) ) 
                pass
    
    def __yield(self):
        """ When system resources are enough to maintain all repositories,
            there is no need to gc. This function can be implemented to check
            the system resources.
        """
        mem = memory.memory()
        if mem > self.__proof.getMemoryThreshold():
            return False
        else:
            return True
