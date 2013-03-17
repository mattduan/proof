"""
Test PyXML lib.
"""

import string

from _xmlplus.sax import saxutils
from _xmlplus.sax import make_parser
from _xmlplus.sax.handler import feature_namespaces


class ResourceHandler(saxutils.DefaultHandler):
    def __init__(self):
        self.__level = 0
        self.__start_char = 0
        self.__value = ''
        pass
    
    def startElement(self, name, attrs):
        # print it
        self.__value = ''
        print "%sstart %s: %s" % ('\t'*self.__level, name, attrs.items())
        self.__level += 1
        self.__start_char = 1

    def characters(self, ch):
        if string.strip(ch) == '':
            return

        if self.__start_char:
            print '\t'*self.__level,

        self.__value += ch
        
        print ch,

    def endElement(self, name):
        self.__start_char = 0
        if self.__value:
            print
            self.__value = ''
        self.__level -= 1
        print "%send %s" % ('\t'*self.__level, name)


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
    
