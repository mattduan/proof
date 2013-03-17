"""
MapBuilders are wrappers around DatabaseMaps.  You use a MapBuilder
to populate a DatabaseMap.  You should implement this interface to create
your own MapBuilders.  The MapBuilder interface exists to support ease of
casting.
"""

__version__= '$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import proof.ProofException as ProofException


class MapBuilder:

    def doBuild(self):
        """ Build up the database mapping.
        """
        raise ProofException.ProofNotImplementedException( \
            "MapBuilder.doBuild: need to be overrided." )
 
    def isBuilt(self):
        """ Tells us if the database mapping is built so that we can avoid
            re-building it repeatedly.
            
            @return Whether the DatabaseMap is built.
        """
        raise ProofException.ProofNotImplementedException( \
            "MapBuilder.isBuilt: need to be overrided." )
 
    def getDatabaseMap(self):
        """ Gets the database mapping this map builder built.
        
        @return A DatabaseMap.
        """
        raise ProofException.ProofNotImplementedException( \
            "MapBuilder.getDatabaseMap: need to be overrided." )
