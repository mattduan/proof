"""
Test PyXML lib.
"""

import string
import copy

from _xmlplus.sax import saxutils
from _xmlplus.sax import make_parser
from _xmlplus.sax.handler import feature_namespaces


KNOWN_SECTIONS = [ 'resource', 'strategies', 'schema', 'namespaces',
                   'tables', 'columns', 'objects', 'aggregates' ]


def print_dict(d, level=0):
    for k, v in d.items():
        print "%s'%s' : " % ('\t'*level, k),
        if type(v) == type({}):
            print "{"
            print_dict(v, level+1)
            print "%s}" % ('\t'*(level+1))
        else:
            print "'%s'," % (v)


class ResourceHandler(saxutils.DefaultHandler):
    def __init__(self):
        # printing ident level
        #self.__level = 0

        # position indicator
        #self.__start_char = 0

        # section indicators
        self.__section = []

        # temp value used for parsing values
        self.__value = ''
        self.__current_schema = ''
        self.__current_namespace = ''
        self.__current_table = ''
        self.__current_column = ''
        self.__current_object = ''
        self.__current_aggregate = ''
        self.__current_dict = {}

        # real values parsed from XML
        self.__resource = {}
        self.__strategies = {}
        self.__adapters = {}
        self.__database_map = {}
        self.__namespaces = {}
        self.__objects = {}
        self.__aggregates = {}
    
    def startElement(self, name, attrs):
        self.__value = ''
        if name in KNOWN_SECTIONS:
            self.__section.append( name )
            if name == 'schema':
                self.__current_schema = attrs['name']
                self.__database_map[self.__current_schema] = {}
                self.__namespaces[self.__current_schema] = {}
                self.__objects[self.__current_schema] = {}
                self.__aggregates[self.__current_schema] = {}
        else:
            section = string.join(self.__section)
            if section == 'resource schema namespaces':
                if name == 'namespace':
                    self.__current_namespace = attrs['name']
                    self.__current_dict = {}
            elif section == 'resource schema tables':
                if name == 'table':
                    self.__current_table = attrs['name']
                    self.__database_map[self.__current_schema][self.__current_table] = {}
            elif section == 'resource schema tables columns':
                if name == 'column':
                    self.__current_column = attrs['name']
                    self.__current_dict = {}
            elif section == 'resource schema objects':
                if name == 'object':
                    self.__current_object = attrs['name']
                    self.__current_dict = {}
            elif section == 'resource schema aggregates':
                if name == 'aggregate':
                    self.__current_aggregate = attrs['name']
                    self.__current_dict = {}
                   
        # print it
        #print "%sstart %s: %s" % ('\t'*self.__level, name, attrs.items())
        #self.__level += 1
        #self.__start_char = 1

    def characters(self, ch):
        if string.strip(ch) == '':
            return

        #if self.__start_char:
        #    print '\t'*self.__level,

        self.__value += ch
        
        #print ch,

    def endElement(self, name):
        #self.__start_char = 0

        if name in KNOWN_SECTIONS:
            self.__section.pop()
        else:
            section = string.join(self.__section)
            if section == 'resource':
                self.__resource[name] = self.__value
            elif section == 'resource strategies':
                self.__strategies[name] = self.__value
            elif section == 'resource schema':
                if name == 'adapter':
                    self.__adapters[self.__current_schema] = self.__value
            elif section == 'resource schema namespaces':
                if name == 'namespace':
                    self.__namespaces[self.__current_schema][self.__current_namespace] = \
                                            copy.copy(self.__current_dict)
                    self.__current_dict = {}
                else:
                    self.__current_dict[name] = self.__value
            elif section == 'resource schema tables columns':
                if name == 'column':
                    self.__database_map[self.__current_schema][self.__current_table][self.__current_column] = \
                                            copy.copy(self.__current_dict)
                    self.__current_dict = {}
                else:
                    self.__current_dict[name] = self.__value
            elif section == 'resource schema objects':
                if name == 'object':
                    self.__objects[self.__current_schema][self.__current_object] = \
                                            copy.copy(self.__current_dict)
                    self.__current_dict = {}
                else:
                    self.__current_dict[name] = self.__value
            elif section == 'resource schema aggregates':
                if name == 'aggregate':
                    self.__aggregates[self.__current_schema][self.__current_aggregate] = \
                                            copy.copy(self.__current_dict)
                    self.__current_dict = {}
                else:
                    self.__current_dict[name] = self.__value

        if self.__value:
            #print
            self.__value = ''

        #self.__level -= 1
        #print "%send %s" % ('\t'*self.__level, name)

    def getResource(self):
        print "Resource:"
        print_dict(self.__resource, 1)
        return self.__resource
    
    def getAdapters(self):
        print "Adapters:"
        print_dict(self.__adapters, 1)
        return self.__adapters

    def getStrategies(self):
        print "Strategies:"
        print_dict(self.__strategies, 1)
        return self.__strategies

    def getDatabaseMap(self):
        print "Database Map:"
        print_dict(self.__database_map, 1)
        return self.__database_map

    def getNamespaces(self):
        print "Namespaces:"
        print_dict(self.__namespaces, 1)
        return self.__namespaces

    def getObjects(self):
        print "Objects:"
        print_dict(self.__objects, 1)
        return self.__objects

    def getAggregates(self):
        print "Aggregates:"
        print_dict(self.__aggregates, 1)
        return self.__aggregates
        

if __name__ == '__main__':
    # Create a parser
    parser = make_parser()
    
    # Tell the parser we are not interested in XML namespaces
    parser.setFeature(feature_namespaces, 0)
    
    # Create the handler
    dh = ResourceHandler()
    
    # Tell the parser to use our handler
    parser.setContentHandler(dh)
    
    # Parse the input
    parser.parse('resource.xml')
    
    dh.getResource()
    dh.getAdapters()
    dh.getStrategies()
    dh.getNamespaces()
    dh.getDatabaseMap()
    dh.getObjects()
    dh.getAggregates()
