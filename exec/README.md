

Runtime
-----------------------------

Execution consists of a DAG of Tasks called a Job

* *ExecMaster* each DAG has a single master of that job
* *Planner* creates a dag of tasks
* *TaskRunner* runs a single node of a set of tasks, communicates between
    child tasks
* *Datasource* supplies data to a task

Coercion
------------------------------------

| Go Types         |  Value types        |
------------------------------------------
| int(8,16,32,64)  |  IntValue           |
| float(32,64)     |  NumberValue        |
| string           |  StringValue        |
| []string         |  StringsValue       |
| boolean          |  BoolValue          |
| map[string]int   |  MapStringIntValue  |


From               |   ToInt   | ToString   | ToBool    |  ToNumber  |  MapInt | MapString
------------------------------------------------------------------------------------------
| int(8,16,32,64)  |    y      |   y        |  y        |   y        |  N
| uint(8,16,32,64) |    y      |   y        |  y        |   y        |  N

