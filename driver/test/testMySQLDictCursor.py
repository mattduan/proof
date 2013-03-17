"""
A testcase for MySQLDictCursor.
"""


import datetime
import unittest

import proof.driver.MySQLConnection as MySQLConnection
import proof.driver.MySQLDictCursor as MySQLDictCursor


class testMySQLDictCursor(unittest.TestCase):

    def setUp(self):
        self.__host     = 'localhost'
        self.__username = 'test'
        self.__password = 'test'
        self.__db       = 'test'

        self.con = MySQLConnection.MySQLConnection( host   = self.__host,
                                                    user   = self.__username,
                                                    passwd = self.__password,
                                                    db     = self.__db )
        self.cursor = self.con.cursor(ret_dict=1)

    def tearDown(self):
        self.con.close()
        self.con = None

    def testExecute(self):
        self.cursor.execute("DROP table IF EXISTS Customer")
        # create a table
        sql = """\
CREATE TABLE Customer (ID int(5) NOT NULL auto_increment PRIMARY KEY,
  FirstName varchar(50) NOT NULL,
  MiddleName varchar(50),
  LastName varchar(50) NOT NULL,
  Email varchar(128) NOT NULL,
  Company varchar(150),
  Phone varchar(25) NOT NULL,
  Phone2 varchar(25),
  Fax varchar(25),
  Status enum('A', 'I') NOT NULL default 'A',
  CreateTime DATETIME,
  UpdateTime timestamp(14))"""
        self.cursor.execute(sql)

        self.cursor.execute("DROP table IF EXISTS Address")
        sql = """\
CREATE TABLE Address (
  ID int(10) unsigned NOT NULL auto_increment PRIMARY KEY,
  CustomerId int(5) unsigned NOT NULL REFERENCES Customer,
  AddressType enum ('contact', 'billing') not null default 'contact',
  Street varchar(50) NOT NULL,
  Street2 varchar(50),
  Street3 varchar(50),
  Street4 varchar(50),
  Street5 varchar(50),
  City varchar(30) NOT NULL,
  State varchar(50) NOT NULL,
  PostalCode varchar(20) NOT NULL,
  Country char(2) NOT NULL,
  KEY CustomerID (CustomerID),
  KEY AddressType (AddressType))"""
        self.cursor.execute(sql)

        # insert a row
        sql = """\
insert into Customer ( FirstName,
                       MiddleName,
                       LastName,
                       Email,
                       Company,
                       Phone,
                       Phone2,
                       Fax,
                       Status,
                       CreateTime )
              values ( %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s )"""
        self.cursor.execute(sql, ('Guoqiang', '', 'Duan', 'duan@duan.com', '', '312-321-1234', '', '312-321-1234', 'A', datetime.datetime.now().__str__()))

        id1 = self.cursor.insert_id()

        # insert another row
        sql = """\
insert into Customer ( FirstName,
                       MiddleName,
                       LastName,
                       Email,
                       Company,
                       Phone,
                       Phone2,
                       Fax,
                       Status,
                       CreateTime )
              values ( %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s,
                       %s )"""
        self.cursor.execute(sql, ('Mike', '', 'Layer', 'mike@gmail.com', '', '312-321-1234', '', '312-321-1234', 'A', datetime.datetime.now().__str__()))

        id2 = self.cursor.insert_id()

        # select 2 rows
        sql = """\
select Customer.FirstName as 'Customer.FirstName',
       Customer.LastName as 'Customer.LastName',
       Customer.Email as 'Customer.Email'
from Customer
where ID in (%s, %s)""" % (id1, id2)
        result = self.cursor.query(sql)
        print
        print "SELECT Result:\n>>>%s" % (`result`)
        self.assert_( result == 2 )

        # fetch one dict
        result = self.cursor.fetchone()
        print "FETCH OneDict Result:\n>>>%s" % (`result`)
        self.assert_( result['Customer.FirstName'] == 'Guoqiang' )
        self.assert_( result['Customer.LastName'] == 'Duan' )
        self.assert_( result['Customer.Email'] == 'duan@duan.com' )

        result = self.cursor.fetchone()
        print "FETCH OneDict Result:\n>>>%s" % (`result`)
        self.assert_( result['Customer.FirstName'] == 'Mike' )
        self.assert_( result['Customer.LastName'] == 'Layer' )
        self.assert_( result['Customer.Email'] == 'mike@gmail.com' )

        # reset cursor
        self.cursor.seek(0, mode="absolute")

        # fetch many dict
        result = self.cursor.fetchmany()
        print "FETCH ManyDict None Result:\n>>>%s" % (`result`)
        self.assert_( result[0]['Customer.FirstName'] == 'Guoqiang' )
        self.assert_( result[0]['Customer.LastName'] == 'Duan' )
        self.assert_( result[0]['Customer.Email'] == 'duan@duan.com' )
        #self.assert_( result[1]['Customer.FirstName'] == 'Mike' )
        #self.assert_( result[1]['Customer.LastName'] == 'Layer' )
        #self.assert_( result[1]['Customer.Email'] == 'mike@gmail.com' )

        # reset cursor
        self.cursor.seek(0, mode="absolute")

        # fetch many dict
        result = self.cursor.fetchmany(size=3)
        print "FETCH ManyDict 3 Result:\n>>>%s" % (`result`)
        self.assert_( result[0]['Customer.FirstName'] == 'Guoqiang' )
        self.assert_( result[0]['Customer.LastName'] == 'Duan' )
        self.assert_( result[0]['Customer.Email'] == 'duan@duan.com' )
        self.assert_( result[1]['Customer.FirstName'] == 'Mike' )
        self.assert_( result[1]['Customer.LastName'] == 'Layer' )
        self.assert_( result[1]['Customer.Email'] == 'mike@gmail.com' )

        # reset cursor
        self.cursor.seek(0, mode="absolute")

        # fetch all dict
        result = self.cursor.fetchall()
        print "FETCH AllDict Result:\n>>>%s" % (`result`)
        self.assert_( result[0]['Customer.FirstName'] == 'Guoqiang' )
        self.assert_( result[0]['Customer.LastName'] == 'Duan' )
        self.assert_( result[0]['Customer.Email'] == 'duan@duan.com' )
        self.assert_( result[1]['Customer.FirstName'] == 'Mike' )
        self.assert_( result[1]['Customer.LastName'] == 'Layer' )
        self.assert_( result[1]['Customer.Email'] == 'mike@gmail.com' )

        # update a row
        sql = """\
update Customer set Customer.Email = 'duan@domainect.com'
where ID = %s""" % (id1)
        self.cursor.update(sql)

        sql = """\
select Customer.Email as 'Customer.Email' from Customer
where ID = %s""" % (id1)
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        print "FETCH One Result:\n>>>%s" % (`result`)
        self.assert_( result['Customer.Email'] == 'duan@domainect.com' )
        
        # datetime
        sql = "select Customer.CreateTime as 'Customer.CreateTime' from Customer"
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        print "FETCH datetime Result:\n>>>%s" % (`result`)

        # timestamp
        sql = "select Customer.UpdateTime as 'Customer.UpdateTime' from Customer"
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        print "FETCH timestamp Result:\n>>>%s" % (`result`)

        # delete a row
        sql = "delete from Customer where ID=%s" % (id2)
        result = self.cursor.execute(sql)
        print "DELETE Result:\n>>>%s" % (`result`)
        
        sql = "select * from Customer"
        result = self.cursor.execute(sql)
        self.assert_( result == 1 )

        # show tables
        sql = "show tables"
        result = self.cursor.execute(sql)
        print "SHOW Table Result:\n>>>%s" % (`result`)
        result = self.cursor.fetchall()
        print "SHOW Table Result:\n>>>%s" % (`result`)

        # show create
        sql = "show create table Customer"
        result = self.cursor.execute(sql)
        print "SHOW CREATE Result:\n>>>%s" % (`result`)
        result = self.cursor.fetchall()
        print "SHOW CREATE Result:\n>>>%s" % (`result`)

        # desc table
        sql = "desc `Customer`"
        result = self.cursor.execute(sql)
        print "DESC Table Result:\n>>>%s" % (`result`)
        result = self.cursor.fetchall()
        print "DESC Table Result:\n>>>%s" % (`result`)

        # delete table
        sql = "drop table Customer"
        self.cursor.execute(sql)
        sql = "drop table Address"
        self.cursor.execute(sql)
        self.assert_( True )
