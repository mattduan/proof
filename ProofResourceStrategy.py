"""
ProofResourceStrategy is the real implementation of initializing the resource
data for ProofResource. There can be multiple strategies for one ProofResource.
Main considerations are configure file, database (may use a ProofInstance),
XML file.

This is the base class with all interfaces needed by ProofResource. Each different
strategies can futher extend from this class to include strategic specific
functions.

By separating the strategy process from resource interfaces, we can make
ProofInstance work both as stand-alone and shared persistent data layer.
It also make testing easier by creating some dummy strategy implementations.
"""

__version__='$Revision: 117 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import logging


class ProofResourceStrategy:

    def __init__(self, logger=None):
        pass

    def __parseXMLDBMap(self, schema, xml):
        pass

    def getDatabaseMap(self, schema):
        pass

