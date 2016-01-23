
## v 0.12  January 2016

* https://github.com/araddon/qlbridge/pull/66
  * add support for **WITH** key=value pairs in sql dialect, ie `SELECT title FROM article WITH distributed=true, node_ct=20`
  * Support planner/visitors to be swapped out with custom implementations, so upstream can implemented distributed planners.
    * `TaskMaker` interfaces
    * implement more Visitor interfaces (Source, etc)

## v 0.11  December 2015

* https://github.com/araddon/qlbridge/pull/61
  * convert `IN` statement from MultiArg -> BinaryNode w ArrayNode type
  * cleanup remove un-used interface methods (NodeType())
  * fingerprint() for filterql
  * fix negateable string for BinaryNode  (LIKE)

* https://github.com/araddon/qlbridge/pull/56
  * GroupBy implementation in execution engine
  * count, avg, sum functions for aggs
  * recognize aggregate functions (count, sum) without group-by in parser

## v 0.10  December 2015

* https://github.com/araddon/qlbridge/pull/54
  - implmennt `FilterQL` in vm.  filterQL is a `WHERE` filter language with dsl nesting
  - context-wrapper, allow go structs to be used natively in vm including functions

* https://github.com/araddon/qlbridge/pull/52
  - move datasource schema structures into *schema* pkg
  - new plan.context simplify interfaces and pass through ctx to runtime queries
  - implement literal query `select 1;`
  - implement @@session.max_allowed_packets type variables in both Global, Session contexts
  - new staticsource data type for simpler static-data returns
  - better support for `SHOW FULL COLUMNS`, `SHOW CREATE TABLE` 
  - new `Wrap` interface on job builders allowing frontends to override query behavior

