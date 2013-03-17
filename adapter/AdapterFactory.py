"""
A factory which instantiates Adapter.
"""

__version__='$Revision: 3194 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"
 

import proof.ProofConstants as ProofConstants
import proof.adapter.MySQLAdapter as MySQLAdapter
import proof.adapter.NoneAdapter as NoneAdapter

class AdapterFactory:

    def __init__(self):
        pass

    def create(self, resource_type):
        """ Factory method which instantiates Adapter based on the
            resource type value and that defined in ProofConstants.
            Returns <code>None</code> for unknown types.

            @param resource_type The resource type.
            @return The appropriate Adapter object (possibly <code>None</code>).
        """
        if resource_type == ProofConstants.MYSQL:
            return MySQLAdapter.MySQLAdapter()
        elif resource_type == ProofConstants.NONE:
            return NoneAdapter.NoneAdapter()
        else:
            return None
