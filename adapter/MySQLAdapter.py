"""
This is used in order to connect to a MySQL database using the MySQLdb
drivers.  Simply comment the above and uncomment this code below and
fill in the appropriate values for DB_NAME, DB_HOST, DB_USER,
DB_PASS.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import proof.ProofConstants as ProofConstants
import proof.sql.SQLExpression as SQLExpression
import proof.pk.IDMethod as IDMethod
import proof.sql.SQLConstants as SQLConstants
import proof.adapter.Adapter as Adapter

class MySQLAdapter(Adapter.Adapter):

    def __init__(self):
        Adapter.Adapter.__init__(self)

    def toUpperCase(self, s):
        """ This method is used to ignore case.
        
            @param s The string to transform to upper case.
            @return The upper case string.
        """
        return s

    def getResourceType(self):
        """ Return the resource type constant.
        """
        return ProofConstants.MYSQL 

    def getIDMethodType(self):
        """ Returns the constant from the interface denoting which
            type of primary key generation method this type of RDBMS uses.
            
            @return IDMethod constant
        """
        return IDMethod.AUTO_INCREMENT

    def getIDMethodSQL(self, obj):
        """ Returns SQL used to get the most recently inserted primary key.
            Databases which have no support for this return
            <code>null</code>.
            
            @param obj Information used for key generation.
            @return The most recently inserted database key.
        """
        return "SELECT LAST_INSERT_ID()"

    def lockTable(self, con, table):
        """ Locks the specified table.
            
            @param con The connection to use.
            @param table The name of the table to lock.
        """
        assert table.find(";")==-1 and table.find("'")==-1
        cursor = con.cursor()
        cursor.execute("LOCK TABLE %s WRITE" % (table))

    def unlockTable(self, con, table):
        """ Unlocks the specified table.
        
            @param con The connection to use.
            @param table The name of the table to unlock.
        """
        assert table.find(";")==-1 and table.find("'")==-1
        #con.update("UNLOCK TABLES")
        cursor = con.cursor()
        cursor.execute("UNLOCK TABLE %s" % (table))

    def ignoreCase(self, s):
        """ This method is used to ignore case.
        
            @param s The string whose case to ignore.
            @return The string in a case that can be ignored.
        """
        return self.toUpperCase(s)

    def supportsNativeLimit(self):
        """ This method is used to check whether the database natively
            supports limiting the size of the resultset.
            
            @return True if the database natively supports limiting the
            size of the resultset.
        """
        return True

    def supportsNativeOffset(self):
        """ This method is used to check whether the database natively
            supports returning results starting at an offset position other
            than 0.

            @return True if the database natively supports returning
            results starting at an offset position other than 0.
        """
        return True

    def getLimitStyle(self):
        """ This method is used to check whether the database supports
            limiting the size of the resultset.
            
            @return The limit style for the database.
        """
        return SQLConstants.LIMIT_STYLE_MYSQL
