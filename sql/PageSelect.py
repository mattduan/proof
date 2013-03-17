"""
This class can be used to retrieve a large result set from a database query.
The query is started and then rows are returned a page at a time.  The <code>
PageSelect</code> is meant to be placed into the Session, so that it can be
used in response to several related requests.  Note that in order to use
<code>PageSelect</code> you need to be willing to accept the fact that the result
set may become inconsistent with the database if updates are processed subsequent
to the queries being executed. Specifying a memory page limit of 1 will give you
a consistent view of the records but the totals may not be accurate and the
performance will be terrible. In most cases the potential for inconsistencies
data should not cause any serious problems and performance should be pretty
good (but read on for further warnings).

<p>The idea here is that the full query result would consume too much memory
and if displayed to a user the page would be too long to be useful.  Rather
than loading the full result set into memory, a window of data (the memory
limit) is loaded and retrieved a page at a time.  If a request occurs for
data that falls outside the currently loaded window of data then a new query
is executed to fetch the required data.  Performance is optimized by
starting a thread to execute the database query and fetch the results.  This
will perform best when paging forwards through the data, but a minor
optimization where the window is moved backwards by two rather than one page
is included for when a user pages past the beginning of the window.

<p>As the query is performed in in steps, it is often the case that the total
number of records and pages of data is unknown. <code>PageSelect</code>
provides various methods for indicating how many records and pages it is
currently aware of and for presenting this information to users.

<p><code>PageSelect</code> utilises the <code>Criteria</code> methods
<code>setOffset()</code> and <code>setLimit()</code> to limit the amount of
data retrieved from the database - these values are passed through to the DBMS
when supported (efficient with the caveat below) or handled by the Factory when
it is not (not so efficient). At time of writing <code>Criteria</code> will
only pass the offset and limit through to MySQL and PostgreSQL.

<p>As <code>PageSelect</code> must re-execute the query each time the user
pages out of the window of loaded data, you should consider the impact of
non-index sort orderings and other criteria that will require the DBMS to
execute the entire query before filtering down to the offset and limit either
internally or via Factory.

<p>The memory limit defaults to 5 times the page size you specify, but
alternative constructors and the class method <code>setMemoryPageLimit()
</code> allow you to override this for a specific instance of
<code>PageSelect</code> or future instances respectively.

<p>The constructors allow you to specify the name of the Factory instance to
use to build the returnd objects. This separates the page window management
from the DBMS query. It still uses <code>Factory.doSelect(criteria)</code>
to do the real work.

<p>Typically you will create a <code>PageSelect</code> using your <code>
Criteria</code> (perhaps created from the results of a search parameter
page), page size, memory page limit and the corresponding factory instance
and pickle this in Session.

<p>...to move through the records. <code>PageSelect</code> implements a
number of convenience methods that make it easy to add all of the necessary
bells and whistles to your template.
"""

__version__='$Revision: 3212 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import util.logger.Logger as Logger


# Constants

DEFAULT_MEMORY_LIMIT_PAGES = 5
DEFAULT_PAGE_SIZE          = 10

class PageSelect:

    def __init__( self,
                  criteria,
                  pagesize,
                  factory,
                  mem_limit_pages = DEFAULT_MEMORY_LIMIT_PAGES,
                  logger = None ):
        """ Constructor.

            @param criteria A Criteria object.
            @param pagesize The number of records in one page.
            @param factory The Factory object.
            @param mem_limit_pages The maximum number of pages worth of rows to
            be held in memory at one time.
            @param logger A logger object.
        """
        self.init( criteria,
                   pagesize,
                   factory,
                   mem_limit_pages,
                   logger )
        pass

    def init( self,
              criteria,
              pagesize,
              factory,
              mem_limit_pages = DEFAULT_MEMORY_LIMIT_PAGES,
              logger = None ):
        import proof.BaseFactory as BaseFactory
        assert issubclass(factory.__class__, BaseFactory.BaseFactory)

        if pagesize < 1:
            pagesize = DEFAULT_PAGE_SIZE

        if mem_limit_pages < 1:
            mem_limit_pages = DEFAULT_MEMORY_LIMIT_PAGES
          
        self.__criteria        = criteria
        self.__factory         = factory
        self.__page_size       = pagesize
        self.__mem_limit_pages = mem_limit_pages
        self.__mem_limit       = pagesize * mem_limit_pages
        self.__db_name         = criteria.getDbName()
        #self.__more_indicator  = more_indicator

        # The record number of the first record in memory.
        self.__block_begin = 0
        # The record number of the last record in memory.
        self.__block_end   = self.__block_begin + pagesize - 1

        # The memory store of records.
        self.__results = []

        # An indication of whether or not the totals (records and pages) are at
        # their final values.
        self.__totals_finalized = False

        # The cursor position in the result set.
        self.__position = 0
        # The total number of pages known to exist.
        self.__total_pages = -1
        # The total number of records known to exist.
        self.__total_records = 0
        # The number of the page that was last retrieved.
        self.__current_page_number = 0

        # The last page of results that were returned.
        self.__last_results = []
        
        self.__logger = Logger.makeLogger(logger)
        self.log      = self.__logger.write

        self.__startQuery(self.__page_size)
        self.__getTotal()

    def getPage(self, page_number):
        """ Retrieve a specific page, if it exists.
        
            @param page_number the number of the page to be retrieved - must be
            greater than zero. An empty <code>List</code> will be returned if
            <code>page_number</code> exceeds the total number of pages that exist.
            @return a <code>List</code> of query results containing a maximum of
            <code>page_size</code> results.
        """
        if page_number < 1:
            page_number = 1

        self.__current_page_number = page_number
        return self.getResults((page_number - 1) * self.__page_size)

    def getCurrentPageResults(self):
        """ Provide access to the results from the current page.

            @return a <code>List</code> of query results containing a maximum of
            <code>page_size</code> reslts.
        """
        return self.__last_results

    def getNextResults(self):
        """ Gets the next page.

            @return a <code>List</code> of query results containing a maximum of
            <code>page_size</code> reslts.
        """
        if not self.getNextResultsAvailable():
            return self.getCurrentPageResults()
        self.__current_page_number += 1
        return self.getResults(self.__position)
    
    def getPreviousResults(self):
        """ Gets the previous page.

            @return a <code>List</code> of query results containing a maximum of
            <code>page_size</code> reslts.
        """
        if not self.getPreviousResultsAvailable():
            return self.getCurrentPageResults()

        start = 0
        if (self.__position - 2 * self.__page_size) < 0:
            self.__current_page_number = 1
        else:
            start = self.__position - 2 * self.__page_size
            self.__current_page_number -= 1
        return self.getResults(start)
    
    def getResults(self, start, size=None):
        """ Gets a block of rows starting at a specified row and containing a
            specified number of rows.
            
            @param start the starting row.
            @param size the number of rows.
            @return a <code>List</code> of query results containing a maximum of
            <code>page_size</code> reslts.
        """
        if not size or type(size)!=type(1) or size<1:
            size = self.__page_size

        if type(start)!=type(1) or start<0:
            start = 0
        
        self.log("getResults(start: %s, size: %s) invoked." % (start, size))

        if size > self.__mem_limit:
            size = self.__mem_limit

        # Going in reverse direction, trying to limit db hits so assume user
        # might want at least 2 sets of data.
        if start < self.__block_begin:
            self.log("getResults(): Paging backwards: start(%s), block_begin(%s)" % \
                     (start, self.__block_begin))
            if self.__mem_limit >= size*2:
                self.__block_begin = start - size
                if self.__block_begin < 0:
                    self.__block_begin = 0
            else:
                self.__block_begin = start

            self.__block_end = self.__block_begin + self.__mem_limit - 1
            self.__startQuery(size)

            return self.getResults(start, size)

        # Assume we are moving on, do not retrieve any records prior to start.
        elif (start + size - 1) > self.__block_end:
            self.log("getResults(): Paging past end of loaded data as " +
                     "end (%s), block_end (%s)" % ((start + size - 1), self.__block_end) )
            self.__block_begin = start
            self.__block_end = self.__block_begin + self.__mem_limit - 1
            self.__startQuery(size)

            return self.getResults(start, size)

        from_index = start - self.__block_begin
        to_index   = from_index + min(size, len(self.__results)-from_index)

        results = self.__results[from_index:to_index]
        
        self.__position = start + size
        self.__last_results = results

        return results

    def __startQuery(self, init_size):
        """ Retrieve the result set.

            @param init_size the initial size for each block.
        """
        if not init_size or type(init_size)!=type(1) or init_size<1:
            init_size = self.__page_size
        
        # Use the criteria to limit the rows that are retrieved to the
        # block of records that fit in the predefined memoryLimit.
        self.__criteria.setOffset(self.__block_begin)
        # Add 1 to memory limit to check if the query ends on a page break.
        #self.__criteria.setLimit(self.__mem_limit + 1)
        self.__criteria.setLimit(self.__mem_limit)

        import proof.ObjectFactory as ObjectFactory
        import proof.AggregateFactory as AggregateFactory
        if isinstance(self.__factory, ObjectFactory.ObjectFactory):
            self.__results = self.__factory.doSelectObject(self.__criteria)
        elif isinstance(self.__factory, AggregateFactory.AggregateFactory):
            self.__results = self.__factory.doSelectAggregate(self.__criteria)
        else:
            self.__results = self.__factory.doSelect(self.__criteria)

    def __getTotal(self):
        """ Query the total number of records for this PageSelect.
        """
        # remove offset and limit
        self.__criteria.setOffset(0)
        self.__criteria.setLimit(-1)
        
        self.__total_records = self.__factory.doTotalSelect(self.__criteria)

        self.__totals_finalized = True
        
    def getCurrentPageNumber(self):
        return self.__current_page_number

    def getTotalRecords(self):
        if not self.__totals_finalized:
            self.__getTotal()
        
        return self.__total_records

    def getTotalPages(self):
        if self.__total_pages > -1:
            return self.__total_pages

        if not self.__totals_finalized:
            self.__getTotal()
        
        self.__total_pages = self.__total_records/self.__page_size + \
                             (self.__total_records%self.__page_size and 1 or 0)

        return self.__total_pages

    def getPageSize(self):
        return self.__page_size

    def setPageSize(self, page_size):
        self.__page_size = page_size
        self.__mem_limit = page_size * self.__mem_limit_pages

    def resetTotalsFinalized(self):
        self.__totals_finalized = False

    def setMemoryPageLimit(self, mem_limit_pages):
        self.__mem_limit_pages = mem_limit_pages
        self.__mem_limit       = self.__page_size * mem_limit_pages

    def getMemoryPageLimit(self):
        return self.__mem_limit_pages

    def getCurrentPageSize(self):
        """ Provides a count of the number of rows to be displayed on the current
            page - for the last page this may be less than the configured page size.
            
            @return the number of records that are included on the current page of
            results.
        """
        if not self.__last_results:
            return 0

        return len(self.__last_results)

    def getFirstRecordNoForPage(self):
        """ Provide the record number of the first row included on the current page.
            
            @return The record number of the first row of the current page.
        """
        if self.__current_page_number < 1:
            return 0

        return self.__current_page_number * self.__page_size - self.__page_size + 1

    def getLastRecordNoForPage(self):
        """ Provide the record number of the last row included on the current page.
        
            @return the record number of the last row of the current page.
        """
        if self.__current_page_number < 1:
            return 0

        return (self.__current_page_number-1) * self.__page_size + \
                                                self.getCurrentPageSize()

    def getNextResultsAvailable(self):
        """ Indicates if further result pages are available.
        
            @return <code>true</code> when further results are available.
        """
        if self.__current_page_number < self.getTotalPages():
            return True

        return False

    def getPreviousResultsAvailable(self):
        """ Indicates if previous results pages are available.
        
            @return <code>true</code> when previous results are available.
        """
        if self.__current_page_number <= 1:
            return False

        return True

    def hasResultsAvailable(self):
        """ Indicates if any results are available.
        
            @return <code>true</code> of any results are available.
        """
        return self.getTotalRecords() > 0

    def reset(self):
        """ Clear the query result so that the query is reexecuted when the next page
            is retrieved.
        """
        self.__block_begin = 0
        self.__block_end   = self.__block_begin + self.__page_size - 1
        self.__results = []
        self.__totals_finalized = False
        self.__position = 0
        self.__total_pages = -1
        self.__total_records = 0
        self.__current_page_number = 0
        self.__last_results = []

    def __str__(self):
        """ Provide something useful for debugging purposes.
        
            @return some basic information about this instance of PageSelect.
        """
        # remove offset and limit
        self.__criteria.setOffset(0)
        self.__criteria.setLimit(-1)

        s  = "PageSelect:\n"
        s += "    Query: %s\n" % (self.__factory.createQueryString(self.__criteria))
        s += "    PageSize: %s\n" % (self.__page_size)
        s += "    CurrentPageNumber: %s\n" % (self.__current_page_number)
        s += "    TotalRecords: %s\n" % (self.getTotalRecords())
        s += "    TotalPages: %s\n" % (self.getTotalPages())
        s += "    MemoryPageLimit: %s\n" % (self.getMemoryPageLimit())
        s += "    CurrentPageSize: %s\n" % (self.getCurrentPageSize())

        return s

