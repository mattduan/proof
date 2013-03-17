""" Constants used in PROOF.
"""

__version__='$Revision: 117 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


# resource type used by PROOF
NONE  = 'none'
MYSQL = 'mysql'

RESOURCE_TYPE_LIST = [ NONE, MYSQL ]


# States for Aggregates
AGGR_NEW      = "STATE_NEW"
AGGR_LOADED   = "STATE_LOADED"
AGGR_DIRTY    = "STATE_DIRTY"
AGGR_UNLOADED = "STATE_UNLOADED"

AGGR_STATE_LIST = [ AGGR_NEW,
                    AGGR_LOADED,
                    AGGR_DIRTY,
                    AGGR_UNLOADED ]

# Ages for Aggregates in seconds
DEFAULT_AGGR_NEW_AGE    = 3600  # 1 hour
DEFAULT_AGGR_LOADED_AGE = 600   # 10 minutes
DEFAULT_AGGR_DIRTY_AGE  = 600   # 10 minutes

# ProofInstance Memory Threshold in Bytes
DEFAULT_MEMORY_THRESHOLD = 150000000

# repository monitor thread run interval
MIN_REPOSITORY_GC_INTERVAL     = 60   # 1 minute
DEFAULT_REPOSITORY_GC_INTERVAL = 180  # 3 minutes

# repository thread session lifetime in seconds
THREAD_SESSION_LIFETIME = 18  # httpd.conf KeepAliveTimeout 15

# A maximum limit to prevent selectAll on a big table
DEFAULT_SELECTALL_LIMIT = 100
