
Pull Requests:  https://github.com/araddon/qlbridge/pulls?q=is%3Apr+is%3Aclosed

## 2017 Updates
* Calculate Time Boundarys for Datemath expressions https://github.com/araddon/qlbridge/pull/183
* Expression inliner for `INCLUDE` (referenced expressions) https://github.com/araddon/qlbridge/pull/182
* Code Coverage improvements in `value` pkg https://github.com/araddon/qlbridge/pull/178
* Cleanup the `CREATE ...` Statement https://github.com/araddon/qlbridge/pull/175
* Exists() as part of column expression lex bug https://github.com/araddon/qlbridge/pull/167
* Base64 Builtin functions https://github.com/araddon/qlbridge/pull/166
* File based datasources support iterator instead of in-mem file list https://github.com/araddon/qlbridge/pull/165
* Elasticsearch Sql -> ES search DSL generator https://github.com/araddon/qlbridge/pull/160
* Completely revamp/improve performance of VM functions by removing reflect usage https://github.com/araddon/qlbridge/pull/148


## Sept 2016
* Expression Massive Revamp https://github.com/araddon/qlbridge/pull/125
  * `Include` and `FilterQL` moved into native Expresson parser/vm not separate dialect
  * Implement generic `Expr` type for usage as json representation of Node/Expressions

## Aug 2016
* Schema Improvements for internal schema mgmt https://github.com/araddon/qlbridge/pull/124
* Column existence check https://github.com/araddon/qlbridge/pull/121
* Filter QL improvements https://github.com/araddon/qlbridge/pull/114
* Improve SQL Projections https://github.com/araddon/qlbridge/pull/123


## July 2016
* Handle Identity Quotes/Escaping and Left/Right better.
  * https://github.com/araddon/qlbridge/pull/116
  * https://github.com/araddon/qlbridge/pull/112
  * https://github.com/araddon/qlbridge/pull/117
* Support Dates in Between https://github.com/araddon/qlbridge/pull/109
* New `HasSuffix` and `HasPrefix` functions https://github.com/araddon/qlbridge/pull/107
* Dialect specific quote marks.  Includes new SQL Rewriter https://github.com/araddon/qlbridge/pull/103
* Function Registry https://github.com/araddon/qlbridge/pull/101
* https://github.com/araddon/qlbridge/pull/100
  * SQL OrderBy
  * cleaner lex quoting
  * FilterQL Doc

## June 1016

* Schema Improvements https://github.com/araddon/qlbridge/pull/97
* FilterQL Enhancments https://github.com/araddon/qlbridge/pull/96
* Information Schema https://github.com/araddon/qlbridge/pull/91

## May 2016
* Filter Function https://github.com/araddon/qlbridge/pull/86


## April 2016
* sql `SET @var = "stuff"` https://github.com/araddon/qlbridge/pull/79
* Better runtime close channel mgmt https://github.com/araddon/qlbridge/pull/80

## March 27th 2016

* Better internal Schema Query planning, system (SHOW, DESCRIBE) with schemadb https://github.com/araddon/qlbridge/pull/68
  * introspect csv files for types
  * convert `SHOW`, `DESCRIBE` into `SELECT` statements
  * better internal data-source registry


## v 0.12  February 2016

* Enable Distributed runtime by `Executor` interface https://github.com/araddon/qlbridge/pull/66
  * add support for **WITH** key=value pairs in sql dialect, ie `SELECT title FROM article WITH distributed=true, node_ct=20`
  * Support planner/executors to be swapped out with custom implementations, so upstream can implemented distributed planners.
    * `expr`, `sql`, `plan` support protobuf serialization
    * separate out the planner, executor so planning can occur on one master node, and send request (dag of plan tasks) to slave executor nodes.
    * support partitionable sources (run partion 1 on node 1, partition 2 on node 2, etc....)

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

