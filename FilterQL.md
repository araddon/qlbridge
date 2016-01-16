
FilterQL 
=============================

FilterQL is a filter (Where clause) expression syntax that varies
from traditional SQL in it allows a DSL'esque expression of AND/OR nesting
and reference to other named Filters.

```
Filter     = "FILTER" Phrase [FROM] [ALIAS]
Phrase     = AND | OR | Expression
AND        = "AND" (Phrase1, Phrase2, ...) # Nested ANDs may be folded into the toplevel AND
OR         = "OR" (Phrase1, Phrase2, ...)  # Nested ORs may be folded into the toplevel OR
Expression = NOT
           | Comparison
           | EXISTS
           | IN
           | INTERSECTS
           | CONTAINS
           | LIKE
           | FilterPointer
NOT           = "NOT" Phrase # Multiple NOTs may be folded into a single NOT or removed
Comparison    = Identifier ComparisonOp Literal
ComparisonOp  = ">" | ">=" | "<" | "<=" | "=="
EXISTS        = "EXISTS" Identifier
IN            = Identifier "IN" (Literal1, Literal2, ...)
INTERSECTS    = Identifier "INTERSECTS" (Literal1, Literal2, ...)
CONTAINS      = Identifier "CONTAINS" Literal
LIKE          = Identifier "LIKE" String # uses * for wildcards
FilterPointer = "INCLUDE" String
FROM          = "FROM" Identifier
ALIAS         = "ALIAS" Identifier

Literal = String | Int | Float | Bool | Timestamp
Identifier = [a-zA-Z][a-zA-Z0-9_]+
```


**Examples**

```
# Simple single expression filter
FILTER "abc" IN some_identifier

FILTER NOT foo

# Filters have an optional "Alias" used for 
# referencing 

FILTER AND ( channelsct > 1 AND scores.quantity > 20 ) ALIAS multi_channel_active


# Filters can "include" (make references to other ALIASed filters)
FILTER AND (
   EXISTS email,
   NOT INCLUDE multi_channel_active
)


FILTER AND (
    visits > 5,
    NOT INCLUDE someotherfilter,
)

# negation
FILTER NOT AND ( ... )

# Compound filter
FILTER AND (
    visits > 5,
    last_visit >= "2015-04-01 00:00:00Z",
    last_visit <  "2015-04-02 00:00:00Z",
)

# Like wildcard match
FILTER url LIKE "/blog/"

# date math
# Operator is either + or -. Units supported are y (year), M (month), 
#   w (week), d (date), h (hour), m (minute), and s (second)

FILTER last_visit > "now-24h"

# IN operator
city IN ("Portland, OR", "Seattle, WA", "Newark, NJ")

# Nested Logical expressions
AND (
    OR (
        foo == true
        bar != 5
    )
    EXISTS signup_date
    OR (
        NOT bar IN (1, 2, 4, 5)
        INCLUDE SomeOtherFilter
    )
)

# functions
FILTER domain(email) == "gmail.com"


```
