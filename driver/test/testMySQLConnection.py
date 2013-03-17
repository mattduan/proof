"""
A testcase for MySQLConnection.
"""


import proof.driver.MySQLConnection as MySQLConnection
import proof.driver.MySQLCursor as MySQLCursor
import proof.driver.MySQLDictCursor as MySQLDictCursor


import unittest


class testMySQLConnection(unittest.TestCase):

    def setUp(self):
        self.__host     = 'localhost'
        self.__username = 'test'
        self.__password = 'test'
        self.__db       = 'test'

        self.con = MySQLConnection.MySQLConnection( host   = self.__host,
                                                    user   = self.__username,
                                                    passwd = self.__password,
                                                    db     = self.__db )
        
    def tearDown(self):
        self.con.close()
        self.con = None

    def testCommit(self):
        self.con.commit()
        self.assert_(True)

    def testRollback(self):
        self.con.rollback()
        self.assert_(True)

    def testAutoCommit(self):
        auto_commit = self.con.getAutoCommit()
        self.assert_( auto_commit )

        self.con.setAutoCommit(False)
        auto_commit = self.con.getAutoCommit()
        self.assert_( not auto_commit )

    def testCursor(self):
        cursor = self.con.getCursor()
        self.assert_(cursor.__class__.__name__ == 'MySQLCursor')
        dictcursor = self.con.getCursor(ret_dict=1)
        self.assert_(dictcursor.__class__.__name__ == 'MySQLDictCursor')
