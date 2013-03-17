"""
A connection pool management base class. It gives the basic
interfaces for using a connection pool.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import time
import logging
import threading
import Queue

import util.logger.Logger as Logger
from util.Trace import traceBack
from util.Import import my_import

import proof.ProofException as ProofException
import proof.datasource.DataSourceFactory as DataSourceFactory
import proof.datasource.PooledDataSourceFactory as PooledDataSourceFactory

# Some default values

# Default maximum limit of connections from this pool: 5
DEFAULT_MAX_CONNECTIONS = 5 

# Default Expiry Time for a pool: 1/2 hour
DEFAULT_EXPIRY_TIME = 60 * 30

# Default Connect Wait Timeout: 10 Seconds
DEFAULT_CONNECTION_WAIT_TIMEOUT = 10


class ConnectionPool:

    def __init__( self,
                  host,
                  username,
                  password,
                  dbname,
                  adapter,
                  max_connections = DEFAULT_MAX_CONNECTIONS,
                  expiry_time     = DEFAULT_EXPIRY_TIME,
                  wait_timeout    = DEFAULT_CONNECTION_WAIT_TIMEOUT,
                  logger          = None,
                  log_interval    = 0
                  ):
        """ Creates a <code>ConnectionPool</code> with the default
            attributes.
            
            @param host The host address
            @param username The user name for this pool.
            @param password The password for this pool.
            @param dbname The database name for this pool.
            @param max_monnections max number of connections
            @param expiry_time connection expiry time
            @param wait_timeout timeout
            @param logger The logger object.
            @param log_interval log interval
        """
        self.__host              = host
        self.__username          = username
        self.__password          = password
        self.__dbname            = dbname
        self.__total_connections = 0
        self.__max_connections   = max_connections
        self.__pool              = Queue.Queue(max_connections)
        self.__expiry_time       = expiry_time
        self.__wait_count        = 0
        self.__logger            = Logger.makeLogger(logger)
        self.__log_interval      = log_interval
        self.__wait_timeout      = wait_timeout

        self.log = self.__logger.write

        # an internal counter for exceeding max connection limit times
        self.__over_max_connections = 0

        # Monitor thread reporting the pool state
        self.__monitor = None

        # Keep track of when connections were created. Keyed by a
        # PooledConnection and value is a datetime
        self.__timestamps = {}

        self.__adapter = adapter
        # initialize the pooled datasource and datasource.
        dsfactory = DataSourceFactory.DataSourceFactory()
        self.__ds = dsfactory.create( self.__adapter,
                                      host = self.__host,
                                      username = self.__username,
                                      password = self.__password,
                                      dbname = self.__dbname,
                                      logger = self.__logger )

        if not self.__ds:
            raise ProofException.ProofNotFoundException( \
                "Can't initialize DataSource for resource type %s." % (self.__adapter.getResourceType() ) )
        
        pdsfactory = PooledDataSourceFactory.PooledDataSourceFactory()
        self.__pooled_ds = pdsfactory.create( self,
                                              host = self.__host,
                                              username = self.__username,
                                              password = self.__password,
                                              dbname = self.__dbname,
                                              logger = self.__logger )
        
        if not self.__pooled_ds:
            raise ProofException.ProofNotFoundException( \
                "Can't initialize PooledDataSource for resource type %s." % (self.__adapter.getResourceType() ) )
                
        if self.__log_interval > 0:
            self.log( "Starting Pool Monitor Thread with Log Interval %s Seconds" % \
                      (self.__log_interval), logging.DEBUG )

            # Create monitor thread
            self.__monitor = _Monitor(self)

            # Indicate that this is a system thread. JVM will quit only
            # when there are no more active user threads. Settings threads
            # spawned internally by Torque as daemons allows commandline
            # applications using Torque to terminate in an orderly manner.
            self.__monitor.setDaemon(True)
            self.__monitor.start()

    def getConnection(self):
        """ Returns a connection that maintains a link to the pool it came from.
        """
        pcon = None
        if self.__pool.empty() and \
               self.__total_connections < self.__max_connections:
            pcon = self.__getNewPooledConnection()
        else:
            try:
                pcon = self.__getInternalPooledConnection()
            except:
                self.log( "Error in getting pooled connection: %s" % \
                          (traceBack()), logging.ERROR )
                raise ProofException.ProofConnectionException( \
                    "Error in getting pooled connection: %s" % \
                    (traceBack()) )
        return pcon

    def __getNewPooledConnection(self):
        """ Returns a fresh pooled connection to the database. The database type
            is specified by <code>driver</code>, and its connection
            information by <code>url</code>, <code>username</code>, and
            <code>password</code>.
            
            @return A pooled database connection.
        """
        if self.__pooled_ds:
            pcon = self.__pooled_ds.getPooledConnection()

            # Age some connections so that there will not be a run on the db,
            # when connections start expiring
            current_time = time.time()
            ratio = (self.__max_connections - self.__total_connections)/float(self.__max_connections)
            ratio_time = current_time - (self.__expiry_time * ratio / 4)
            if self.__expiry_time < 0: ratio_time = current_time

            self.__timestamps[id(pcon)] = ratio_time
            self.__total_connections += 1

            return pcon
            
        raise ProofException.ProofNotFoundException( \
            "ConnectionPool Pooled DataSource is not initialized." )

    def __getNewConnection(self):
        """ Returns a fresh connection to the database.
            Note: it is not related to this pool and only called when
            the pool is full.
            
            @return A database connection.
        """
        if self.__ds:
            con = self.__ds.getConnection()
            return con

        raise ProofException.ProofNotImplementedException( \
            "ConnectionPool DataSource is not initialized." )

    def __getInternalPooledConnection(self):
        """ Gets a pooled database connection.
        
            @return A database connection.
        """
        # We test waitCount > 0 to make sure no other threads are
        # waiting for a connection.
        if self.__wait_count > 0 or self.__pool.empty():
            try:
                try:
                    self.__wait_count += 1
                    time.sleep(self.__wait_timeout)
                except:
                    self.log( "Connection wait timeout error: %s" % (traceBack()),
                              logging.WARNING )
                    pass
            finally:
                self.__wait_count -= 1

        pcon = self.popConnection()
        if not pcon:
            self.__over_max_connections += 1
            self.log( "OVER MAX CONNECTIONS LIMIT: %s" % \
                      (self.__over_max_connections) )
            return self.__getNewConnection()
        
        return pcon

    def popConnection(self, pcon=None):
        """ Helper function that attempts to pop a connection off the pool's stack,
            handling the case where the popped connection has become invalid by
            creating a new connection.

            @param pcon An optional pooled connection object. If given, it will be
            removed from the pool if exists in pool. Refer to PooledConnection classes.
            @return An existing or new database connection.
        """
        if pcon and pcon in self.__pool.queue:
            try:
                self.__pool.queue.remove(pcon)
            except:
                pass
            return None

        while not self.__pool.empty():

            try:
                pcon = self.__pool.get_nowait()
            except:
                pcon = None

            # It's really not safe to assume this connection is
            # valid even though it's checked before being pooled.
            if pcon:
                if self.__is_valid(pcon):
                    return pcon
                else:
                    self.__closePooledConnection(pcon)

                    # If the pool is now empty, create a new connection.  We're
                    # guaranteed not to exceed the connection limit since we
                    # just killed off one or more invalid connections, and no
                    # one else can be accessing this cache right now.
                    if self.__pool.empty():
                        return self.__getNewPooledConnection()

        self.log( "Attempted to pop connection from empty pool!",
                  logging.WARNING )

        return None

    def hasConnection(self, pcon):
        """ Check whether a connection is in the pool.
        """
        return pcon in self.__pool.queue

    def __is_valid(self, pcon):
        """ Helper method which determines whether a connection has expired.
            
            @param pcon The connection to test.
            @return False if the connection is expired, True otherwise.
        """
        birth = self.__timestamps.get(id(pcon), 0)
        age   = time.time() - birth

        return age < self.__expiry_time
        
    def finalize(self):
        """ Close any open connections when this object is garbage collected.
        """
        self.shutdown()

    def shutdown(self):
        """ Close all connections to the database.
        """

        while self.getTotalCount() > 0:
            try:
                pcon = self.__pool.get(True)
            except:
                continue

            self.__closePooledConnection(pcon)

        # shutdown the monitor thread
        self.stopMonitor()

    def getAdapter(self):
        return self.__adapter

    def getLogInterval(self):
        """ Return log_interval value.
        """
        return self.__log_interval            

    def setLogInterval(self, sec):
        """ Set log_interval value.
        """
        self.__log_interval = sec            

    def stopMonitor(self):
        """ Stop the runing monitor by setting the log_interval to 0.
        """
        while self.__monitor.isAlive():
            self.__log_interval = 0
            time.sleep(3)
        self.__monitor = None

    def getTotalCount(self):
        """ Returns the Total connections in the pool
        
            @return total connections in the pool
        """
        return self.__total_connections

    def getTotalAvailable(self):
        """ Returns the available connections in the pool
            
            @return number of available connections in the pool
        """
        return self.__pool.qsize()

    def getTotalCheckedOut(self):
        """ Returns the checked out connections in the pool
            
            @return number of checked out connections in the pool
        """
        return (self.__total_connections - self.__pool.qsize())

    def decrementConnections(self):
        """ Decreases the count of connections in the pool.
        """
        self.__total_connections -= 1

    def getPoolContext(self):
        """ Return all the attribute values in a dict with variable name
            as the key.
        """
        # an internal counter for exceeding max connection limit times
        #self.__over_max_connections = 0
        attr = {}
        attr['__host']                  = self.__host
        attr['__username']              = self.__username
        #attr['__password']              = self.__password
        attr['__dbname']                = self.__dbname
        attr['__max_connections']       = self.__max_connections
        attr['__expiry_time']           = self.__expiry_time
        attr['__log_interval']          = self.__log_interval
        attr['__wait_timeout']          = self.__wait_timeout
        attr['__total_connections']     = self.__total_connections
        attr['__available_connections'] = self.getTotalAvailable()
        attr['__over_max_connections']  = self.__over_max_connections

        return attr

    def __str__(self):
        s = "%s:\n" % (self.__class__.__name__)
        attr = self.getPoolContext()
        for k in attr.keys():
            s += "    %s => %s\n" % (k, attr[k])

        return s

    def releaseConnection(self, pcon):
        """ This method returns a connection to the pool, and <b>must</b>
            be called by the requestor when finished with the connection.
            
            @param pcon The database connection to release.
        """
        if self.__is_valid(pcon):
            try:
                self.__pool.put(pcon)
            except:
                self.log("Connection Pool is full when adding '%s': %s" % (pcon, traceBack()))
                self.__closePooledConnection(pcon)
        else:
            self.__closePooledConnection(pcon)

    def __closePooledConnection(self, pcon):
        """ Close a pooled connection.
        
            @param pcon The database connection to close.
        """
        try:
            try:
                pcon.releaseConnectionPool()
                pcon.close()
            except:
                self.log( "Exception was raised when closing a connection: %s" \
                          % ( traceBack() ) )
        finally:
            self.decrementConnections()

    def getLogger(self):
        return self.__logger
    
    def setLogger(self, logger):
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write

#============================================================================
# This inner class monitors the <code>ConnectionPool</code>.
#
# This class is capable of logging the number of connections available in
# the pool periodically. This can prove useful if you application
# frozes after certain amount of time/requests and you suspect
# that you have connection leakage problem.
#
# Set the <code>log_interval</code> property of your pool definition
# to the number of seconds you want to elapse between loging the number of
# connections.
#============================================================================

class _Monitor(threading.Thread):

    def __init__(self, monitor_pool):
        threading.Thread.__init__(self)
        self.__is_running = False
        self.__monitor_pool = monitor_pool
    
    def run(self):
        self.__is_running = True
        __log_interval = self.__monitor_pool.getLogInterval()
        while __log_interval > 0 and self.__is_running:
            self.__monitor_pool.log( str(self.__monitor_pool) )

            try:
                time.sleep(__log_interval)

                # stop monitoring in case log_interval set to 0 dynamically
                __log_interval = self.__monitor_pool.getLogInterval()
            except:
                pass

        #self.__monitor_pool = None
