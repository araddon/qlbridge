



Coercion
------------------------------------

| Go Types         |  vm.Value types     |
------------------------------------------
| int(8,16,32,64)  |  IntValue           |
| float(32,64)     |  NumberValue        |
| string           |  StringValue        |
| []string         |  StringsValue       |
| map[string]int   |  MapStringIntValue  |


From               |   ToInt   | ToString   | ToBool    |  ToNumber  |
----------------------------------------------------------------------
| int(8,16,32,64)  |    y      |   y        |  y        |   y        |
| uint(8,16,32,64) |    y      |   y        |  y        |   y        |