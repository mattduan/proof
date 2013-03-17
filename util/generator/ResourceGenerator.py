"""
A PROOF resource generator utility script.
"""

import string
import os, os.path
import re, sys
import datetime
import getopt
import ConfigParser

# required path
#import PyStartup

from util.Trace import traceBack

import proof.driver.MySQLConnection as MySQLConnection

from proof.ProofResource import STRATEGY_STATIC, STRATEGY_DYNAMIC


# mysql is the only supported adapter at this time
DEFAULT_ADAPTER = 'mysql'


class ResourceGenerator:

    def __init__( self,
                  name,
                  version,
                  proof_path,
                  schemas,
                  adapter            = DEFAULT_ADAPTER,
                  python_path        = '/path/to/python/modules',
                  default_schema     = '',
                  default_namespace  = '',
                  id                 = None,
                  date               = None
                  ):
        """ Constructor.
        """
        #######################
        # Resource Attributes #
        #######################
        # name
        self.__name               = name
        # version 
        self.__version            = version
        # id
        if not id:
            self.__id             = string.replace(string.lower(name), ' ', '_')
        else:
            self.__id             = id

        # proof path
        self.__proof_path         = proof_path

        if self.__proof_path[0] == '/':
            raise Exception("proof_path has to be a relative path")
        
        # adapter used for the resource
        self.__adapter            = adapter

        # path to services base
        self.__python_path        = python_path
        
        self.__root_path = os.path.join(self.__python_path, self.__proof_path)
        
        # default schema [[ FILL_IN_NEEDED ]]
        self.__default_schema     = default_schema
        # default namespace [[ FILL_IN_NEEDED ]]
        self.__default_namespace  = default_namespace
        # date created
        self.__date               = date or datetime.date.today().__str__()

        self.schemas = schemas

        self.xml = ''

    def process(self):
        self.xml = """\
<?xml version="1.0" encoding="UTF-8"?>
 
<resource>
%(info)s

%(strategies)s

%(schemas)s

</resource>""" % { 'info'       : self.generate_info(),
                   'strategies' : self.generate_strategies(),
                   'schemas'    : self.generate_schemas() }
        
        # save to _gen
        res_path = self.__gen_path()
        filename = "%s.xml" % (string.replace(string.lower(self.__name), ' ', '_'))
        open( os.path.join(res_path, filename), "w" ).write(self.xml)

    def __gen_path(self):
        """ Return gen path.
        """
        # make sure the basic path exists
        gen_path = os.path.join( self.__root_path,
                                 'resource',
                                 '_gen' )
        if not os.access(gen_path, os.F_OK):
            os.makedirs(gen_path)

        date = datetime.date.today().strftime('%Y%m%d')

        i = 0
        full_path = None
        while not full_path or os.access(full_path, os.F_OK):
            i += 1
            full_path = os.path.join( gen_path,
                                      "%s_%s" % (date, string.zfill(`i`, 3)) )
        os.mkdir(full_path)

        return full_path

    def generate_info(self):
        """ Return resource info header xml.
        """
        return """\
  <name>%(name)s</name>
  <id>%(id)s</id>
  <version>%(version)s</version>
  <date>%(date)s</date>
 
  <proof_path>%(path)s</proof_path>
  <default_schema>%(default_schema)s</default_schema>
  <default_namespace>%(default_namespace)s</default_namespace>""" % {
            'name'              : self.__name,
            'id'                : self.__id,
            'version'           : self.__version,
            'date'              : self.__date,
            'path'              : self.__python_path,
            'default_schema'    : self.__default_schema,
            'default_namespace' : self.__default_namespace
            }

    def generate_strategies(self):
        """ Return resource strategies xml block.
        """
        return """\
  <!-- strategy for different resource types -->
  <strategies>
    <adapter>%(adapter)s</adapter>
    <databasemap>%(dbmap)s</databasemap>
    <namespace>%(namespace)s</namespace>
    <object>%(object)s</object>
    <aggregate>%(aggregate)s</aggregate>
  </strategies>""" % {
            'adapter'   : STRATEGY_STATIC,
            'dbmap'     : STRATEGY_STATIC,
            'namespace' : STRATEGY_DYNAMIC,
            'object'    : STRATEGY_STATIC,
            'aggregate' : STRATEGY_STATIC,
            }

    def generate_schemas(self):
        """ Return all schema xml blocks.
        """
        xml_list = []
        for schema in self.schemas:
            db_host      = schema['db_host']
            db_name      = schema['db_name']
            db_user      = schema['db_user']
            db_pass      = schema['db_pass']
            aggregates   = schema['aggregates']
            schema_name  = string.upper(db_name)
            sg = SchemaGenerator( schema_name,
                                  self.__adapter,
                                  self.__proof_path,
                                  db_host,
                                  db_name,
                                  db_user,
                                  db_pass,
                                  aggregates = aggregates )
            sg.process()
            xml_list.append(sg.xml)

        return string.join(xml_list, "\n\n")
        

class SchemaGenerator:
    """ generate one schema using one database.
    """

    def __init__( self,
                  schema_name,
                  adapter,
                  proof_path,
                  db_host,
                  db_name,
                  db_user,
                  db_pass,
                  aggregates = {}
                  ):
        self.__schema_name = schema_name
        self.__adapter     = adapter
        self.__proof_path  = proof_path
        self.__aggregates  = aggregates

        self.__module_path = proof_path.replace('/', '.')

        # parse tables first
        self.table_gen = TableGenerator( db_host,
                                         db_name,
                                         db_user,
                                         db_pass )

        self.table_gen.process()

        # do we missing anything
        self.__check_mistakes()
        
        self.xml = ''

    def __check_mistakes(self):
        # check all tables are included in aggregate defines
        aggr_tables = []
        for aggregate in self.__aggregates.keys():
            aggr_tables.append(aggregate)
            for child in self.__aggregates[aggregate].keys():
                aggr_tables.append(child)
        db_tables = self.table_gen.table_names

        if len(aggr_tables) != len(db_tables):
            aggr_tables.sort()
            db_tables.sort()
            missing_tables = []
            for table in db_tables:
                if table not in aggr_tables:
                    missing_tables.append(table)
                
            raise Exception( "Not all tables are included in aggregates:\n" + \
                             "Aggregated tables: \n%s\n" % (aggr_tables) + \
                             "Database tables: \n%s\n" % (db_tables) + \
                             "Missing tables: \n%s\n" % (missing_tables) )
        

    def process(self):
        """ Create the xml config resource for one schema.
        """
        self.xml = """\
  <schema name="%(schema_name)s">
%(adapter)s

%(namespaces)s

%(tables)s

%(objects)s

%(aggregates)s

  </schema>""" % { 'schema_name' : self.__schema_name,
                   'adapter'     : self.generate_adapter(),
                   'namespaces'  : self.generate_namespaces(),
                   'tables'      : self.generate_tables(),
                   'objects'     : self.generate_objects(),
                   'aggregates'  : self.generate_aggregates() }
  
    def generate_adapter(self):
        return """\
    <adapter>%s</adapter>""" % (self.__adapter)

    def generate_namespaces(self):
        """ Generated resource always be dynamic.
        """
        return """\
    <!-- start namespace -->
    <namespaces>
    </namespaces>
    <!-- end namespace -->"""

    def generate_tables(self):
        return self.table_gen.xml
    
    def generate_objects(self):
        """ Generate objects section in a schema based available tables.

            @return generated xml.
        """
        xml = """\
    <!-- start objects -->
    <objects>"""

        for table_name in self.table_gen.table_names:
            xml += """
      <object name="%(table_name)s">
        <module>%(module_path)s.%(schema_name)s.%(table_name)s</module>
        <class>%(table_name)s</class>
        <factorymodule>%(module_path)s.%(schema_name)s.%(table_name)sFactory</factorymodule>
        <factoryclass>%(table_name)sFactory</factoryclass>
      </object>""" % { 'table_name'  : table_name,
                       'module_path' : self.__module_path,
                       'schema_name' : string.lower(self.__schema_name) }

        xml += """
    </objects>
    <!-- end objects -->"""

        return xml

    def generate_aggregates(self):
        """ Generate aggregates section in a schema based available tables.
            All tables are considered as aggregates, which need to be defined
            further when defining specific domains.

            @return generated xml.
        """
        xml = """\
    <!-- start aggregates -->
    <aggregates>"""

        for table_name in self.table_gen.table_names:
            if self.__aggregates and table_name in self.__aggregates.keys():
                xml += """
      <aggregate name="%(table_name)s">
        <module>%(module_path)s.%(schema_name)s.%(table_name)sAggregate</module>
        <class>%(table_name)sAggregate</class>
        <factorymodule>%(module_path)s.%(schema_name)s.%(table_name)sAggregateFactory</factorymodule>
        <factoryclass>%(table_name)sAggregateFactory</factoryclass>
        <repositorymodule>%(module_path)s.%(schema_name)s.%(table_name)sAggregateRepository</repositorymodule>
        <repositoryclass>%(table_name)sAggregateRepository</repositoryclass>
      </aggregate>""" % { 'table_name'  : table_name,
                          'module_path' : self.__module_path,
                          'schema_name' : string.lower(self.__schema_name) }

        xml += """
    </aggregates>
    <!-- end aggregates -->"""

        return xml


class TableGenerator:
    """ table generator for one database schema.
    """

    def __init__( self,
                  db_host,
                  db_name,
                  db_user,
                  db_pass,
                  ):
        self.__db_host     = db_host
        self.__db_name     = db_name
        self.__db_user     = db_user
        self.__db_pass     = db_pass

        self.con = MySQLConnection.MySQLConnection( host   = self.__db_host,
                                                    user   = self.__db_user,
                                                    passwd = self.__db_pass,
                                                    db     = self.__db_name )

        self.table_names   = []
        self.tables        = []
        self.xml           = ''

    def process(self):
        self.collect_tables()
        self.parse_tables()
        self.py2xml()

    def collect_tables(self):
        """ Collect all tables in the database.
        """
        sql = "show tables"
        cursor = self.con.cursor()
        cursor.query(sql)
        result = cursor.fetchall()
        for row in result:
            self.table_names.append(row[0])

    def parse_tables(self):
        """ Parse all tables.
        """
        cursor = self.con.cursor()
        for table in self.table_names:
            sql = "desc `%s`" % (table)
            cursor.query(sql)
            result = cursor.fetchall()

            #sys.stderr.write(`result`)
            self.tables.append( [table, self.parse_columns(result)] )

    def parse_columns(self, column_list):
        """ Parse columns in the table.
        
            Required fields for each column are:
            <type>varchar</type>
            <size>25</size>
            <pk>false</pk>
            <notnull>true</notnull>
            <fktable>none</fktable>
            <fkcolumn>none</fkcolumn>

            @param column_list A list of columns with these attributes:
                   Field, Type, Null, Key, Default, Extra
        """
        columns = []

        for column in column_list:
            name    = string.strip(column[0])
            typestr = string.strip(column[1])
            isnull  = string.strip(column[2])
            key     = string.strip(column[3])

            type_name = typestr
            size = 0

            m = re.match("^([^(]+)\((.*?)\)$", typestr)
            if m:
                type_name = m.group(1)
                # only varchar/char size make sense
                if type_name in ( 'varchar', 'char' ):
                    size = m.group(2)

            if isnull == 'YES':
                notnull = 'false'
            else:
                notnull = 'true'

            if key == 'PRI':
                pk = 'true'
            else:
                pk = 'false'

            # guess foreign key
            # assume all primary keys are 'Id'
            # all foreign keys should follow the pattern: <table_name>Id
            fktable  = 'none'
            fkcolumn = 'none'
            if re.match(".+Id$", name):
                fkt = name[:-2]
                if fkt in self.table_names:
                    fktable  = fkt
                    fkcolumn = 'Id'

            columns.append( [ name, { 'type'     : type_name,
                                      'size'     : size,
                                      'pk'       : pk,
                                      'notnull'  : notnull,
                                      'fktable'  : fktable,
                                      'fkcolumn' : fkcolumn
                                      } ] )
            
        return columns
            
    def py2xml(self):
        """ Convert parsed python list to an xml string.
        """
        
        xml = """\
    <!-- start tables -->
    <tables>"""
        for table_name, columns in self.tables:
            xml += """
      <table name="%s">
        <columns>""" % (table_name)
            for column_name, fields in columns:
                fields['name'] = column_name
                xml += """
          <column name="%(name)s">
            <type>%(type)s</type>
            <size>%(size)s</size>
            <pk>%(pk)s</pk>
            <notnull>%(notnull)s</notnull>
            <fktable>%(fktable)s</fktable>
            <fkcolumn>%(fkcolumn)s</fkcolumn>
          </column>""" % fields
            xml += """
        </columns>
      </table>"""
        xml += """
    </tables>
    <!-- end tables -->"""

        self.xml = xml


def usage(msg=''):

    print """USAGE: %s options [-c filename]
                                                                                                                                                
Description
===========
Generate PROOF XML Resource for database schemas configured in the configuration
file.

Parameters
==========
options:
    h/help            -- print this message
c(cfg):
    c/cfg             -- specify the configuration filename
                         default 'resource.cfg'
"""%( sys.argv[0] )
    if msg:
        print >> sys.stderr, msg

    sys.exit(1)
    
    

if __name__ == '__main__':

    # options:
    # for simplity, we use config file for now
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hc:', ['help', 'cfg='])
    except getopt.error, msg:
        usage()

    cfg_filename = 'resource.cfg'

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage()

        if opt in ('-c', '--cfg'):
            cfg_filename = arg

    config = ConfigParser.ConfigParser()

    try:
        config.readfp(open(cfg_filename))
    except:
        usage(msg=traceBack())

    # required parameters
    name       = config.get('INFO', 'name')
    version    = config.get('INFO', 'version')
    proof_path = config.get('INFO', 'proof_path')
    
    # optional
    kwargs = {}
    for opt in ['id', 'adapter', 'python_path', 'default_schema', 'default_namespace', 'date']:
        if config.has_option('INFO', opt):
            kwargs[opt] = config.get('INFO', opt)

    # schemas
    schemas = []
    sections = config.sections()
    for section in sections:
        if section != 'INFO':
            # schema
            aggregate_dict = {}
            if config.has_option(section, 'aggregates'):
                aggregates = config.get(section, 'aggregates')
                aggregate_list = string.split(aggregates, ",")
                for aggregate in aggregate_list:
                    aggregate_dict[aggregate] = {}
                    if config.has_option(section, aggregate):
                        child_list = string.split(config.get(section, aggregate), "|")
                        for child in child_list:
                            child_spec_list = string.split(child, ",")
                            aggregate_dict[aggregate][child_spec_list[0]] = child_spec_list

            schemas.append( { 'db_host' : config.get(section, 'db_host'),
                              'db_name' : config.get(section, 'db_name'),
                              'db_user' : config.get(section, 'db_user'),
                              'db_pass' : config.get(section, 'db_pass'),
                              'aggregates' : aggregate_dict,
                              } )

    res_gen = ResourceGenerator(name, version, proof_path, schemas, **kwargs)

    res_gen.process()

    print res_gen.xml
