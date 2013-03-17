
# Query constants
SELECT            = "SELECT "
INSERT            = "INSERT "
UPDATE            = "UPDATE "
DELETE            = "DELETE "
REPLACE           = "REPLACE "
FROM              = " FROM "
INTO              = " INTO "
VALUES            = " VALUES"
WHERE             = " WHERE "
AND               = " AND "
OR                = " OR "
ORDER_BY          = " ORDER BY "
GROUP_BY          = " GROUP BY "
HAVING            = " HAVING "
LIMIT             = " LIMIT "
ROWCOUNT          = " SET ROWCOUNT "
ROWNUM            = " ROWNUM "

# Comparison types
EQUAL             = "="
NOT_EQUAL         = "<>"
ALT_NOT_EQUAL     = "!="
GREATER_THAN      = ">"
LESS_THAN         = "<"
GREATER_EQUAL     = ">="
LESS_EQUAL        = "<="
LIKE              = " LIKE "
NOT_LIKE          = " NOT LIKE "
IN                = " IN "
NOT_IN            = " NOT IN "

COMPARISON_LIST = [ EQUAL,
                    NOT_EQUAL,
                    ALT_NOT_EQUAL,
                    GREATER_THAN,
                    LESS_THAN,
                    GREATER_EQUAL,
                    LESS_EQUAL,
                    LIKE,
                    NOT_LIKE,
                    IN,
                    NOT_IN ]

# Other ANSI SQL keywords
DISTINCT          = "DISTINCT "
JOIN              = "JOIN"
ALL               = "ALL "
CUSTOM            = "CUSTOM"

# "Order by" qualifier - ascending
ASC               = "ASC"

# "Order by" qualifier - descending
DESC              = "DESC"

# "IS NULL" null comparison
ISNULL            = " IS NULL "

# "IS NOT NULL" null comparison
ISNOTNULL         = " IS NOT NULL "

# ANSI SQL functions
CURRENT_DATE      = "CURRENT_DATE"
CURRENT_TIME      = "CURRENT_TIME"
NOW               = "NOW()"

# escaped single quote
SINGLE_QUOTE      = "'"

# escaped backslash
BACKSLASH         = "\\"


# Database Specific Styles

# database does not support limiting result sets.
LIMIT_STYLE_NONE     = 0

# <code>SELECT ... LIMIT <limit>, [<offset>]</code>
LIMIT_STYLE_POSTGRES = 1

# <code>SELECT ... LIMIT [<offset>, ] <limit></code>
LIMIT_STYLE_MYSQL    = 2

# <code>SET ROWCOUNT &lt;offset&gt; SELECT ... SET ROWCOUNT 0</code>
LIMIT_STYLE_SYBASE   = 3

# <code><pre>SELECT ... WHERE ... AND ROWNUM < <limit></pre></code>
LIMIT_STYLE_ORACLE   = 4

LIMIT_STYLES = [ LIMIT_STYLE_NONE, LIMIT_STYLE_POSTGRES,
                 LIMIT_STYLE_MYSQL, LIMIT_STYLE_SYBASE,
                 LIMIT_STYLE_ORACLE ]
