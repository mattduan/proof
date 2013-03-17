"""
This module is used to setup the test environment.
"""

import os, os.path
import fnmatch
import MySQLdb


class TestEnvironment(object):

    def __init__(self,
                 db_host         = 'localhost',
                 db_name         = 'test_proof',
                 db_user         = 'xxxx',
                 db_pass         = '', 
                 schema_basepath = '',
                 schema_pattern  = ''):
        self._db_host = db_host
        self._db_name = db_name
        self._db_user = db_user
        self._db_pass = db_pass
        self._schema_basepath = schema_basepath
        self._schema_pattern  = schema_pattern
        
        self._conn = MySQLdb.connect(host   = self._db_host,
                                     user   = self._db_user,
                                     passwd = self._db_pass,
                                     db     = self._db_name)
        self._cursor = self._conn.cursor()
    
    def setUpEnv(self):
        self.create_database()
        self.init_database()
    
    def resetEnv(self):
        self.clear_data()
    
    def tearDownEnv(self):
        self.drop_database()
   
    # create database
    def create_database(self):
        #self.execute("create database %s"%(self._db_name))
        pass
    
    # drop database
    def drop_database(self):
        #self.execute("drop database %s"%(self._db_name))
        self.clear_table()
    
    # init schema
    def init_database(self):
        sql_file = self._sql_filename()
        os.system("mysql -u %s %s < %s"%(self._db_user, self._db_name, sql_file))
    
    # drop all tables from database
    def clear_table(self):
        tables = self.query("show tables")
        for table in tables:
            self.execute("drop table %s"%(table[0]))
    
    # clear all data from database
    def clear_data(self):
        tables = self.query("show tables")
        for table in tables:
            self.execute("delete from %s"%(table[0]))
    
    # sql query
    def execute(self, sql, args=None):
        self._cursor.execute(sql, args)
    
    def query(self, sql, args=None):
        self._cursor.execute(sql, args)
        result = self._cursor.fetchall()
        return result
    
    def _sql_filename(self):
        """
        Return the last version of sql filename.
        """
        filenames = os.listdir(self._schema_basepath)
        sql_files = []
        for filename in filenames:
            if fnmatch.fnmatch(filename, self._schema_pattern):
                sql_files.append(filename)
        sql_files.sort()
        filename = sql_files[-1]
        return os.path.join(self._schema_basepath, filename)

    def __del__(self):
        self._cursor.close()
        self._conn.close()
