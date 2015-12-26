

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

