"""
An aggregate is an entity object with a concrete meaning in certain business
logic. It is identified by the root object and may contain several other objects.
Except the root object, all other objects in an aggregate should not have direct
relationship with other objects outside this aggregate. It's largely controlled
by database design and denormalization. The Domain/Business layer should only
directly access Aggregates from PROOF.

Each aggregate has its own lifetime - from its creation to its removal from the
Repository. In its lifetime, the aggregate will keep different states and ages.
There are four states to divide the entire lifetime period -- NEW, LOADED, DIRTY,
and UNLOAD. These states are defined below:

NEW: The aggregate has just be created. It only contains the root object pk. All
included objects are not created.
LOADED: All objects and attributes of the aggregate have been initialized.
DIRTY: Any of attributes of its objects is modified or become dirty. When the
aggregate makes commit or cancel, the state will change to LOADED. 
UNLOAD: Any object in the aggregate is unloaded, that is, only pks are kept in
all objects.

STATES
======
           root pk            init attributes               modified
Aggregate --------->  NEW   ------------------->         --------------->
                                                  LOADED                  DIRTY
                     UNLOAD <-------------------         <---------------
                              remove attributes            commit/cancel

In each state, there is an age associated with the aggregate. They are NEW_AGE for
NEW/UNLOAD, LOADED_AGE for LOADED, and DIRTY_AGE for DIRTY. The age is the time
period the aggregate can keep in that state. Each Repository has a backend monitor
thread to check each aggregate's access time and change its state and remove it from
repository.

LIFETIME AGE
============

Aggregate

   CreateTime           Last AccessTime         Last AccessTime    Last AccessTime
      |-----------------------|-----------------------|----------------------| 
      |     NEW / UNLOAD      |         LOADED        |         DIRTY        |
      |                       |                       |                      |
      |<------ NEW_AGE ------>|<---- LOADED_AGE ----->|<---- DIRTY_AGE ----->|


This Aggregate class is the base class for all aggregates in PROOF. It only gives the
abstraction and some basic operations. Most implementation details and relationship
should be implemented in the extended classes.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


import time
import logging
import copy

import util.logger.Logger as Logger
from util.Import import my_import

import proof.ProofInstance as ProofInstance
import proof.ProofConstants as ProofConstants
import proof.sql.Criteria as Criteria


class Aggregate:

    def __init__( self,
                  proof_instance,
                  pk,
                  db_schema         = "DB_SCHEMA",
                  auto_commit       = True,
                  cascade_on_delete = False,
                  logger            = None ):
        """ Constructor.

            @param proof_instance A ProofInstance object.
            @param pk The root object ObjectKey object.
            @param db_schema The schema name for the aggregate.
            @param auto_commit A flag to indicate whether to commit changes when update.
            @param cascade_on_delete A flag to indicate whether to delete all included objects when
                   deleting the aggregate.
            @param logger A Logger object.
        """
        # proof instance 
        self.__proof = proof_instance

        # root pk object
        self.__pk = pk

        # database schema name
        self.__db_schema = db_schema

        # database
        self.__db_name = self.__proof.getDBName(self.__db_schema)
        
        # root object table name
        self.__root_name = self.__pk.getTableName()

        # root object
        # NOTE: it has to be consistent with pk
        self.__root = None

        # auto commit flag
        self.__auto_commit = auto_commit

        # whether cascade when delete
        self.__cascade_on_delete = cascade_on_delete
        
        # init state
        self.__state = ProofConstants.AGGR_NEW

        # creation time
        self.__create_time = time.time()

        # access time
        self.__access_time = time.time()
        
        # the logger
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write

        # initialize relation map
        repository = self.__proof.getInstanceForRepository( self.__root_name,
                                                            schema = self.__db_schema )
        self.__relation_map = repository.getRelationMap()

        # contained objects
        self.__objects = {}
        for obj_name in self.__relation_map.keys():
            self.__objects[obj_name] = {}

    def getProofInstance(self):
        return self.__proof
    
    def getDBSchema(self):
        return self.__db_schema
    
    def getDBName(self):
        return self.__db_name
    
    def getPK(self):
        return self.__pk
    
    def getId(self):
        return self.__pk.getValue()
    
    def getRootObjectName(self):
        return self.__root_name

    getRootName = getRootObjectName

    def getRootObject(self):
        return self.__root

    getRoot = getRootObject
  
    def setRootObject(self, root):
        if root.getTableName() == self.__root_name and \
               root.getPK() == self.__pk:
            self.__root = root
        else:
            self.log( "%s.setRootObject: not a root '%s'" % \
                      (self.__class__.__name__, root), logging.WARNING )

    setRoot = setRootObject
    
    def isRootObjectName(self, name):
        return name == self.__root_name

    #def getObjects(self):
    #    return self.__objects

    def hasObject(self, obj_name):
        if self.__root_name == obj_name or \
               self.__objects.has_key(obj_name):
            return True
        else:
            return False

    def getObjects(self, obj_name):
        """ Return a list of objects with obj_name.
        """
        return self.__objects.get(obj_name, {}).values()
    
    def getObjectNames(self):
        return self.__objects.keys()

    def getObject(self, obj_name, pk):
        """ This method is used when the aggregate is constructed. It should not
            be called directly from application layers. Use getAttribute or any
            convenient methods provided by the extended classes. 
        """
        obj = self.__objects.get(obj_name, {}).get(str(pk), None)
        if not obj:
            self.log( "%s.getObject '%s' with pk '%s' return None." % ( self.__class__.__name__,
                                                                        obj_name,
                                                                        str(pk) ) )
        return obj
    
    def removeObject(self, obj_name, pk):
        """
        Remove an object from this aggregate.
        """
        obj = self.__objects.get(obj_name, {}).get(str(pk), None)
        if obj:
            del self.__objects[obj_name][str(pk)]
            self.log("removed object %s: %s"%(obj_name, str(pk)))

    def addObject(self, obj_name, obj):
        """ This method is used when the aggregate is constructed. It should not
            be called directly from application layers. Use getAttribute or any
            convenient methods provided by the extended classes. 
        """
        ret_obj = None

        if self.__objects.has_key(obj_name):
            pk = obj.getPK()
            self.__objects[obj_name][str(pk)] = ret_obj = obj
            self.log("added object %s: %s"%(obj_name, str(pk)))
        else:
            self.log("Trying to add an object '%s' that doesn't belong to %s." % \
                     (obj_name, self.__class__.__name__), logging.WARN)

        return ret_obj

    def load(self):
        self.touch()
        self.__load_root_object()
        for obj_name in self.__objects.keys():
            self.__load(obj_name)
        self.__state = ProofConstants.AGGR_LOADED
        #self.updateState()

    def load_objects(self):
        self.touch()
        for obj_name in self.__objects.keys():
            self.__load(obj_name)
        self.__state = ProofConstants.AGGR_LOADED
        #self.updateState()

    def unload(self):
        self.touch()
        self.__root.unload()
        for obj_name in self.__objects.keys():
            self.__objects[obj_name] = {}
        self.__state = ProofConstants.AGGR_UNLOADED
        #self.updateState()

    reload = load

    def getState(self):
        return self.__state

    def updateState(self):
        if self.isDirty():
            self.__state = ProofConstants.AGGR_DIRTY

        elif self.__root:
            # default to AGGR_UNLOADED
            self.__state = ProofConstants.AGGR_UNLOADED
            for obj_dict in self.__objects.values():
                if obj_dict != {}:
                    self.__state = ProofConstants.AGGR_LOADED
                    break
        else:
            # AGGR_NEW if root object is None
            self.__state = ProofConstants.AGGR_NEW

    def isDirty(self):
        """ Check whether root or objects have been changed.
        """
        if self.__root and self.__root.isDirty():
            return True
        
        for obj_dict in self.__objects.values():
            for obj in obj_dict.values():
                if obj and obj.isDirty():
                    return True

        return False

    def setAutoCommit(self, auto_commit):
        self.__auto_commit = auto_commit

    def getAutoCommit(self):
        return self.__auto_commit

    isAutoCommit = getAutoCommit

    def commit(self):
        """ Commit all the changes and return to loaded state.
        """
        if self.__state == ProofConstants.AGGR_DIRTY:
            self.__root.update(force=True)
            for obj_dict in self.__objects.values():
                for obj in obj_dict.values():
                    obj.update()
            self.touch()
            self.__state = ProofConstants.AGGR_LOADED

    def cancel(self):
        """ Cancel all the changes and return to loaded state.
        """
        if self.__state == ProofConstants.AGGR_DIRTY:
            self.__root.cancel()
            for obj_dict in self.__objects.values():
                for obj in obj_dict.values():
                    obj.cancel()
            self.touch()
            self.__state = ProofConstants.AGGR_LOADED

    #def getAttributes(self, obj_name=None):
    #    """ Return the entire attributes.
    #    """
    #    # return only required object if obj_name is specified
    #    if obj_name and self.__objects.has_key(obj_name) \
    #           and self.__objects[obj_name]:
    #        return { obj_name : self.__objects[obj_name].getAttributes() }
    #    
    #    # otherwise, return all
    #    attr_dict = {}
    #    for obj_name, obj in self.__objects.items():
    #        attr_dict[obj_name] = obj.getAttributes()
    #
    #    return attr_dict
    
    def getDirtyAttributes(self, obj_name=None):
        """ Return the entire dirty attributes.
        """
        attr_dict = {}

        # return only required object if obj_name is specified
        if obj_name and self.__objects.has_key(obj_name):
            attr_dict[obj_name] = {}
            for pk, obj in self.__objects[obj_name].items():
                attr_dict[obj_name][pk] = obj.getDirtyAttributes()

        # otherwise, return all
        else:
            for obj_name, obj_dict in self.__objects.items():
                attr_dict[obj_name] = {}
                for pk, obj in obj_dict.items():
                    if obj.isDirty():
                        attr_dict[obj_name][pk] = obj.getDirtyAttributes()
    
        return attr_dict
    
    #def getAttribute(self, obj_name, attr):
    #    if self.__state == ProofConstants.AGGR_UNLOADED or \
    #           self.__state == ProofConstants.AGGR_NEW:
    #        self.load()
    #    if self.__objects.has_key(obj_name):
    #        obj = self.__objects[obj_name]
    #        if obj and obj.has_key(attr):
    #            return obj[attr]
    #        else:
    #            self.log( "%s.getAttribute: can't find attribute '%s' in %s." % \
    #                      (self.__class__.__name__, attr, obj_name) )
    #    else:
    #        self.log( "%s.getAttribute: can't find object %s." % \
    #                  (self.__class__.__name__, obj_name) )
    #
    #def setAttribute(self, obj_name, attr, value):
    #    if self.__state == ProofConstants.AGGR_UNLOADED or \
    #           self.__state == ProofConstants.AGGR_NEW:
    #        self.load()
    #    if self.__objects.has_key(obj_name):
    #        obj = self.__objects[obj_name]
    #        if obj and obj.has_key(attr):
    #            obj[attr] = value
    #            if self.isAutoCommit():
    #                self.commit()
    #        else:
    #            self.log( "%s.setAttribute: can't find attribute '%s' in %s." % \
    #                      (self.__class__.__name__, attr, obj_name) )
    #    else:
    #        self.log( "%s.setAttribute: can't find object %s." % \
    #                  (self.__class__.__name__, obj_name) )

    def setCascadeOnDelete(self, cascade_on_delete):
        self.__cascade_on_delete = cascade_on_delete

    def getCascadeOnDelete(self):
        return self.__cascade_on_delete

    def delete(self):
        if self.__cascade_on_delete:
            # delete all objects in this aggregate
            for obj_dict in self.__objects.values():
                for obj in obj_dict.values():
                    obj.delete()
            # delete the root object
            self.__root.delete()
        else:
            # only delete the root object
            self.__root.delete()

        # delete the aggregate from repository
        repository = self.__proof.getInstanceForRepository( self.__root_name,
                                                            schema = self.__db_schema )
        repository.remove(self)
        self = None

    def touch(self):
        """ Update the access time.
        """
        self.__access_time = time.time()

    def _access_time(self):
        return self.__access_time

    def checkRelationMap(self):
        if not self.__relation_map:
            return False
        else:
            return True

    def __eq__(self, aggr):
        if aggr is self:
            return True

        if isinstance(aggr, self.__class__):
            aggr_pk = aggr.getPK()
            if aggr_pk == self.__pk: # and self.__objects == aggr.getObjects():
                return True

        return False

    def __load(self, obj_name):
        """ Load the object based on object name, which should be the table
            name.
        """
        repository = self.__proof.getInstanceForRepository( self.__root_name,
                                                            schema = self.__db_schema )
        relation = repository.getRelationList(obj_name)

        if not relation:
            self.log( "Abort load: no relationship is defined between '%s' and '%s'" % \
                      (self.__root_name, obj_name), logging.WARNING )
            return

        # construct criteria
        crit = Criteria.Criteria( self.__proof,
                                  db_name = self.__db_schema,
                                  logger  = self.__logger )
        
        pk_value = self.__pk.getValue()
        if type(pk_value) == type([]):
            for pk in pk_value:
                crit[pk.getFullyQualifiedName()] = pk.getValue()
        else:
            crit[self.__pk.getFullyQualifiedName()] = pk_value

        for left, right in zip(relation[0], relation[1]):
            crit.addJoin(left, right)

        # reset objects
        self.__objects[obj_name] = {}

        # init ObjectFactory
        module_name = self.__proof.getModuleForFactory(obj_name, schema=self.__db_schema)
        class_name  = self.__proof.getClassForFactory(obj_name, schema=self.__db_schema)
        object_module = my_import(module_name)
        obj = getattr( object_module, class_name )
        factory = obj(self, logger=self.__logger)
        
        # init objects
        objects = factory.doSelectObject(crit)

        for object in objects:
            self.addObject(obj_name, object)
    
    def __load_root_object(self):
        """ Load the root object based on pk.
        """
        if not self.__root:
            module_name = self.__proof.getModuleForObject(self.__root_name, schema=self.__db_schema)
            class_name  = self.__proof.getClassForObject(self.__root_name, schema=self.__db_schema)
            object_module = my_import(module_name)
            obj = getattr( object_module, class_name )
            self.__root = obj(self, self.__pk, logger=self.__logger)

        self.__root.load()

    #===========================================================================

    def setProofInstance(self, inst):
        """ Associate this factory to a PROOF instance.
        """
        assert issubclass(inst.__class__, ProofInstance.ProofInstance)

        self.__proof = inst

    def getLogger(self):
        return self.__logger
 
    def setLogger(self, logger):
        self.__logger = Logger.makeLogger(logger)
        self.log = self.__logger.write
 
    def __getstate__(self):
        """ Used by pickle when the class is serialized.
            Remove the 'proof', 'logger' attributes before serialization.
            @hidden
        """
        self.log = None
        self.__logger = None
        d = copy.copy(self.__dict__)
        del d["log"]
        del d["_Aggregate__logger"]
        del d["_Aggregate__proof"]
        return d
    
    def __setstate__(self, d):
        """ Used by pickle when the class is unserialized.
            Add the 'proof', 'logger' attributes.
            @hidden
        """
        logger = Logger.makeLogger(None)
        d["_Aggregate__logger"] = logger
        d["log"] = logger.write
        d["_Aggregate__proof"] = None
        self.__dict__ = d
        
    
    
    
