
FilterQL
=============================

FilterQL is a record filtering language for finding/search.
It is analogus to the SQL WHERE expression or the _filter/query_ part of the Elasticsearch dsl.
* Used For filtering/finding/searching
* Departs from traditional SQL by allowing a DSL'esque expression
  * `AND ( expression, expression2)` instead of `expression AND expression2`
  * allows referrencing named embedded Filters (other named *FilterQL* statements)


```
# Select Filter clauses allow projection
SelectFilter     = "SELECT" [Columns] Filter

# Filter clauses are Filter only, no projection
Filter         = "FILTER" Phrase [FROM] [LIMIT] [WITH] [ALIAS]

Phrase         = AND | OR | Expression
AND            = "AND" (Phrase1, Phrase2, ...) # Nested ANDs may be folded into the toplevel AND
OR             = "OR" (Phrase1, Phrase2, ...)  # Nested ORs may be folded into the toplevel OR
Expression     = NOT
               | Comparison
               | EXISTS
               | IN
               | INTERSECTS
               | CONTAINS
               | BETWEEN
               | LIKE
               | FilterPointer
NOT           = "NOT" Phrase # Multiple NOTs may be folded into a single NOT or removed
Comparison    = Identifier ComparisonOp Literal
ComparisonOp  = ">" | ">=" | "<" | "<=" | "=="
EXISTS        = "EXISTS" Identifier
IN            = Identifier "IN" ArrayValue
INTERSECTS    = Identifier "INTERSECTS" ArrayValue
CONTAINS      = Identifier "CONTAINS" Literal
LIKE          = Identifier "LIKE" String # uses * for wildcards
BETWEEN       = Identifier "BETWEEN" Literal "AND" Literal
FilterPointer = "INCLUDE" Identifier
FROM          = "FROM" Identifier
ALIAS         = "ALIAS" Identifier
LIMIT         = "LIMIT" Literal

WITH          = WITHVAL [, WITHVAL]
WITHVAL       = Identifier "=" Literal



# arrays can be literal's, or identifiers
ArrayValue    = (Literal1, Literal2, ...) | Identifier

# Raw strings are any string character

# strings must be double-quoted
String  = '"' RawString '"'

# DateMath:   "now-3d" "now-2h" "now-2w" "now+2d"
DateMath = '"now' (+|-) [0-9]+ [smdwMY] '"'

Literal = String | Int | Float | Bool | DateMath

# Identities start with alpha (then can contain numbers) but no spaces, quotes etc
# - escape with back-ticks
# - Identifiers get resolved at runtime to their underlying value
Identifier = [a-zA-Z][a-zA-Z0-9_.]+  | "`"  RawString "`"
```


**Examples**

```
# Simple single expression filter
FILTER "abc" IN some_identifier


FILTER EXISTS email

# the identity `identity with spaces` is a field name
FILTER "value" IN `identity with spaces`

# Filters have an optional "Alias" For Saving it as a Stored Filter

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
FILTER NOT AND ( EXISTS visitct )

# Compound filter
FILTER AND (
    visits > 5,
    last_visit >= "2015-04-01 00:00:00Z",
    last_visit <  "2015-04-02 00:00:00Z",
)

# compound filter showing optional commas
# new-lines serve as expression breaks
FILTER AND (
    visits > 5
    NOT INCLUDE someotherfilter
)

# Like wildcard match
FILTER url LIKE "/blog/"

# Date Math
# Operator is either + or -. Units supported are y (year), M (month), 
#   w (week), d (date), h (hour), m (minute), and s (second)

FILTER last_visit > "now-24h"

# IN operator with Literal values
city IN ("Portland, OR", "Seattle, WA", "Newark, NJ")
# In operator with Right Side Identifier
city IN all_cities

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
    -- IN for identitier resolves to array
    bar IN `user_interests`
)

# functions
FILTER domain(email) == "gmail.com"


# Between
FILTER AND (
    modified BETWEEN "2015-07-01" AND "2016-08-01"
)

```
