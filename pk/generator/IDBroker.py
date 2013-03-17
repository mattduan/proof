"""
This method of ID generation is used to ensure that code is
more database independent.  For example, MySQL has an auto-increment
feature while Oracle uses sequences.  It caches several ids to
avoid needing a Connection for every request.

This class uses the table ID_TABLE:

<database name="@DATABASE_DEFAULT@">
  <table name="ID_TABLE" idMethod="idbroker">
    <column name="ID_TABLE_ID" required="true" primaryKey="true" type="INTEGER"/>
    <column name="TABLE_NAME" required="true" size="255" type="VARCHAR"/>
    <column name="NEXT_ID" type="INTEGER"/>
    <column name="QUANTITY" type="INTEGER"/>
 
    <unique>
      <unique-column name="TABLE_NAME"/>
    </unique>
 
  </table>
</database>

The columns in ID_TABLE are used as follows:<br>

ID_TABLE_ID - The PK for this row (any unique int).<br>
TABLE_NAME - The name of the table you want ids for.<br>
NEXT_ID - The next id returned by IDBroker when it queries the
          database (not when it returns an id from memory).<br>
QUANTITY - The number of ids that IDBroker will cache in memory.<br>
<p>
Use this class like this:
<pre>
id = dbMap.getIDBroker().getNextIdAsInt(null, "TABLE_NAME")
 - or -
ids = dbMap.getIDBroker().getNextIds("TABLE_NAME", numOfIdsToReturn)
</pre>

NOTE: When the ID_TABLE must be updated we must ensure that
IDBroker objects running in different PROOFs do not overwrite each
other.  This is accomplished using using the transactional support
occuring in some databases.  Using this class with a database that
does not support transactions should be limited to a single PROOF.

####################################################################

This class is not implemented yet until there is a need for the
ID_TABLE.

####################################################################
"""

__version__='$Revision: 3194 $'[11:-2]


import proof.pk.generator.IDGenerator as IDGenerator


class IDBroker(IDGenerator.IDGenerator):

    __implements__ = 'IDGenerator'

    def __init__(self, table_name, logger=None):
        """ Constructor.
        """
        IDGenerator.IDGenerator.__init__(self, logger=logger)

        self.__table_name = table_name

    
