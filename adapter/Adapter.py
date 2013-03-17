"""
<code>Adapter</code> defines the interface for a Proof database
adapter.  Support for new databases is added by subclassing
<code>Adapter</code> and implementing its abstract interface, and by
registering the new database adapter and its corresponding
driver in the service configuration.

<p>The Proof database adapters exist to present a uniform
interface to database access across all available databases. Once
the necessary adapters have been written and configured,
transparent swapping of databases is theoretically supported with
<i>zero code changes</i> and minimal configuration file
modifications.

<p>Proof uses the driver class name to find the right adapter.
A driver corresponding to your adapter must be added to the properties
file, using the fully-qualified class name of the driver. If no driver is
specified for your database, <code>driver.default</code> is used.

<pre>
#### MySQL MM Driver
database.default.driver=org.gjt.mm.mysql.Driver
database.default.url=jdbc:mysql://localhost/DATABASENAME
</pre>
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import string
import datetime

import proof.ProofException as ProofException
import proof.sql.SQLConstants as SQLConstants
import proof.pk.IDMethod as IDMethod

class Adapter:

    def __init__(self):
        """ Constructor.
        """
        pass

    def toUpperCase(self, s):
        """ This method is used to ignore case.
        
            @param s The string to transform to upper case.
            @return The upper case string.
        """
        return string.upper(s)

    def getStringDelimiter(self):
        """ Returns the character used to indicate the beginning and end of
            a piece of text used in a SQL statement (generally a single
            quote).
            
            @return The text delimeter.
        """
        return '\''

    def getResourceType(self):
        raise ProofException.ProofNotImplementedException( \
            "Adapter.getResourceType: need to be overrided." )

    def getIDMethodType(self):
        """ Returns the constant from the interface denoting which
            type of primary key generation method this type of RDBMS uses.
            
            @return IDMethod constant
        """
        return IDMethod.NO_ID_METHOD

    def getIDMethodSQL(self, obj):
        """ Returns SQL used to get the most recently inserted primary key.
            Databases which have no support for this return
            <code>null</code>.
            
            @param obj Information used for key generation.
            @return The most recently inserted database key.
        """
        raise ProofException.ProofNotImplementedException( \
            "Adapter.getIDMethodSQL: need to be overrided." )

    def lockTable(self, con, table):
        """ Locks the specified table.
            
            @param con The connection to use.
            @param table The name of the table to lock.
        """
        raise ProofException.ProofNotImplementedException( \
            "Adapter.lockTable: need to be overrided." )

    def unlockTable(self, con, table):
        """ Unlocks the specified table.
        
            @param con The connection to use.
            @param table The name of the table to unlock.
        """
        raise ProofException.ProofNotImplementedException( \
            "Adapter.unlockTable: need to be overrided." )

    def ignoreCase(self, s):
        """ This method is used to ignore case.
        
            @param s The string whose case to ignore.
            @return The string in a case that can be ignored.
        """
        return self.toUpperCase(s)

    def ignoreCaseInOrderBy(self, s):
        """ This method is used to ignore case in an ORDER BY clause.
            Usually it is the same as ignoreCase, but some databases
            (Interbase for example) does not use the same SQL in ORDER BY
            and other clauses.
            
            @param s The string whose case to ignore.
            @return The string in a case that can be ignored.
        """
        return self.ignoreCase(s)

    def supportsNativeLimit(self):
        """ This method is used to check whether the database natively
            supports limiting the size of the resultset.
            
            @return True if the database natively supports limiting the
            size of the resultset.
        """
        return False

    def supportsNativeOffset(self):
        """ This method is used to check whether the database natively
            supports returning results starting at an offset position other
            than 0.

            @return True if the database natively supports returning
            results starting at an offset position other than 0.
        """
        return False

    def escapeText(self):
        """ This method is for the SQLExpression.quoteAndEscape rules.  The rule is,
            any string in a SqlExpression with a BACKSLASH will either be changed to
            '\\' or left as '\'.  SapDB does not need the escape character.
            
            @return true if the database needs to escape text in SqlExpressions.
        """
        return True

    def getLimitStyle(self):
        """ This method is used to check whether the database supports
            limiting the size of the resultset.
            
            @return The limit style for the database.
        """
        return SQLConstants.LIMIT_STYLE_NONE

    def getLongString(self, number):
        """ This method is used to convert long to int.
            
            @param number Long value
            @return The int String.
        """
        return str(number)

    def getDateString(self, date):
        """ This method is used to format any date string.
            Database can use different default date formats.
            
            @param date the Date to format
            @return The proper date formatted String.
        """
        assert isinstance( date, datetime.date )

        return "'%s'" % (date.isoformat())

    def getDateTimeString(self, date):
        """ This method is used to format any datetime string.
            Database can use different default date formats.
            
            @param date the DateTime to format
            @return The proper date formatted String.
        """
        assert isinstance( date, datetime.datetime )

        return "'%s'" % (date.strftime("%Y-%m-%d %H:%M:%S"))

    def getBooleanString(self, b):
        """ This method is used to format a boolean string.
            
            @param b the Boolean to format
            @return The proper boolean String.
        """
        if b:
            return "1"
        else:
            return "0"
    
