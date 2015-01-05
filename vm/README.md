



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

