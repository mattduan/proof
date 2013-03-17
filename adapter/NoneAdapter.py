"""
This DatabaseHandler is used when you do not have a database installed.
"""

import proof.ProofConstants as ProofConstants
import proof.adapter.Adapter as Adapter

class NoneAdapter(Adapter.Adapter):

    def __init__(self):
        pass

    def getResourceType(self):
        return ProofConstants.NONE

    def getConnection(self):
        return None
    
    def toUpperCase(self, s):
        return s    
    
    def ignoreCase(self, s):
        return self.toUpperCase(s)

    def getIDMethodSQL(self, obj):
        return None

    def lockTable(self, con, table):
        pass
    
    def unlockTable(self, con, table):
        pass
    
