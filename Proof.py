"""
The core base class for Proof. It is a container class for a ProofResource and
all ProofInstances.

In service-based application, the shared ProofResource will be used by all
ProofInstances. This class is responsible to initialize the ProofResource and
maintain a list of ProofInstance in different namespaces.

Another Facade class should be created as a Singleton in Apache environment
each time a new Proof class is created. The facade class will encapsulate the
Proof class and have same interfaces as the Proof class. (If subclass can be
a Singleton in Python, this Proof subclass should implemented as a Singleton.)
All application frameworks should only instantiate and access the Facade
Singleton class.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"

import logging
import thread

import util.logger.Logger as Logger
from util.Import import my_import

import proof.ProofInstance as ProofInstance
import proof.ProofResource as ProofResource


class Proof(object):

    def __new__( cls,
                 config_filename,
                 reload = False,
                 logger = None ):
        """ Initialize as a Singleton.

            @param config_filename The proof resource config filename.
            @param reload A flag if True, config file will be reparsed. 
            @logger The logger object.
        """
        it = cls.__dict__.get('__it__')
        if it is not None and not reload:
            return it
        cls.__it__ = it = object.__new__(cls)
        it.init(config_filename, logger=logger)
        return it

    def init(self, config_filename, logger=None):
        self.__config_filename = config_filename
        self.__proof_resource  = None
        self.__proof_instances = {}
        
        # acquire a thread lock for serializing access to its data.
        self.lock = thread.allocate_lock()

        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write
        
    def makeProofResource(self):
        return ProofResource.ProofResource(self.__config_filename, 
                                           logger=self.__logger)

    def makeProofInstance(self, namespace):
        return ProofInstance.ProofInstance(self.getProofResource(),
                                           namespace,
                                           logger=self.__logger)

    def getProofResource(self):
        if not self.__proof_resource:
            self.__proof_resource = self.makeProofResource()
        
        return self.__proof_resource

    def getProofInstance(self, namespace=None):
        if not namespace:
            namespace = self.getProofResource().getDefaultNameSpace()
        
        # make this thread safe
        self.lock.acquire()
        try:
            if self.__proof_instances.has_key(namespace):
                #self.log( "Return ProofInstance for '%s' from cache." % (namespace) )
                return self.__proof_instances[namespace]
            elif self.getProofResource().hasNameSpace(namespace):
                self.__proof_instances[namespace] = self.makeProofInstance(namespace)
        finally:
            self.lock.release()
                
        return self.__proof_instances.get(namespace, None)

    def getConfigFilename(self):
        return self.__config_filename
    
    def getLogger(self):
        return self.__logger
    
