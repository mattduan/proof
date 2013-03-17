"""
A factory which instantiates IDGenerator.
"""

__version__='$Revision: 3194 $'[11:-2]


import proof.pk.IDMethod as IDMethod
import proof.pk.generator.AutoIncrementIDGenerator as AutoIncrementIDGenerator
import proof.pk.generator.SequenceIDGenerator as SequenceIDGenerator

class IDGeneratorFactory:

    def __init__(self):
        pass

    def create(self, adapter):
        """ Factory method which instantiates IDGenerator based on the
            return value of the provided adapter's getIDMethodType method.
            Returns <code>None</code> for unknown types.

            @param adapter The type of adapter to create an ID generator for.
            @return The appropriate ID generator (possibly <code>None</code>).
        """
        id_method = adapter.getIDMethodType()

        if id_method == IDMethod.AUTO_INCREMENT:
            return AutoIncrementIDGenerator.AutoIncrementIDGenerator(adapter)
        elif id_method == IDMethod.SEQUENCE:
            return SequenceIDGenerator.SequenceIDGenerator(adapter)
        else:
            return None
