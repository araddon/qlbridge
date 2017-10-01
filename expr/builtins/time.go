package builtins

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/leekchan/timeutil"
	"github.com/lytics/datemath"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

// Now Get current time of Message (message time stamp) or else choose current
// server time if none is available in message context
//
type Now struct{}

// Type time
func (m *Now) Type() value.ValueType { return value.TimeType }

func (m *Now) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 0 {
		return nil, fmt.Errorf("Expected 0 args for Now() but got %s", n)
	}
	return nowEval, nil
}
func nowEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	if ctx != nil && !ctx.Ts().IsZero() {
		t := ctx.Ts()
		return value.NewTimeValue(t), true
	}
	return value.NewTimeValue(time.Now().In(time.UTC)), true
}

// Yy Get year in integer from field, must be able to convert to date
//
//    yy()                 =>  15, true    // assuming it is 2015
//    yy("2014-03-01")     =>  14, true
//
type Yy struct{}

// Type integer
func (m *Yy) Type() value.ValueType { return value.IntType }
func (m *Yy) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 or 1 args for Yy() or yy(date_field) but got %s", n)
	}
	return yearEval, nil
}
func yearEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	yy := 0
	if len(vals) == 0 {
		if ctx != nil && !ctx.Ts().IsZero() {
			yy = ctx.Ts().Year()
		} else {
			yy = time.Now().Year()
		}
	} else if len(vals) == 1 {
		dateStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.NewIntValue(0), false
		}
		if t, err := dateparse.ParseAny(dateStr); err != nil {
			return value.NewIntValue(0), false
		} else {
			yy = t.Year()
		}
	}

	if yy >= 2000 {
		yy = yy - 2000
	} else if yy >= 1900 {
		yy = yy - 1900
	}
	return value.NewIntValue(int64(yy)), true
}

// mm Get month as integer from date
//
// @optional timestamp (if not, gets from context reader)
//
//  mm()                =>  01, true  /// assuming message ts = jan 1
//  mm("2014-03-17")    =>  03, true
//
type Mm struct{}

// Type integer
func (m *Mm) Type() value.ValueType { return value.IntType }
func (m *Mm) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 args for mm(), or 1 arg for mm(date_field) but got %s", n)
	}
	return monthEval, nil
}

func monthEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) == 0 {
		if ctx != nil && !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewIntValue(int64(t.Month())), true
		}
		return value.NewIntValue(int64(time.Now().Month())), true
	}

	dateStr, ok := value.ValueToString(vals[0])
	if !ok {
		return value.NewIntValue(0), false
	}
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		return value.NewIntValue(int64(t.Month())), true
	}

	return value.NewIntValue(0), false
}

// yymm convert date to 4 digit string from argument if supplied, else uses message context ts
//
//   yymm() => "1707", true
//   yymm("2016/07/04") => "1607", true
//
type YyMm struct{}

// Type string
func (m *YyMm) Type() value.ValueType { return value.StringType }
func (m *YyMm) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 or 1 args for YyMm() but got %s", n)
	}
	return yymmEval, nil
}
func yymmEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) == 0 {
		if ctx != nil && !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewStringValue(t.Format(yymmTimeLayout)), true
		}
		return value.NewStringValue(time.Now().Format(yymmTimeLayout)), true
	}

	dateStr, ok := value.ValueToString(vals[0])
	if !ok {
		return value.EmptyStringValue, false
	}
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		return value.NewStringValue(t.Format(yymmTimeLayout)), true
	}

	return value.EmptyStringValue, false
}

// DayOfWeek day of week [0-6]
//
//   dayofweek() => 3, true
//   dayofweek("2016/07/04") => 5, true
//
type DayOfWeek struct{}

// Type int
func (m *DayOfWeek) Type() value.ValueType { return value.IntType }
func (m *DayOfWeek) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 or 1 args for DayOfWeek() but got %s", n)
	}
	return dayOfWeekEval, nil
}

func dayOfWeekEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	var t time.Time
	if len(vals) == 0 {
		if ctx != nil && !ctx.Ts().IsZero() {
			t = ctx.Ts()
		} else {
			t = time.Now()
		}
	} else {
		dateStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.NewIntNil(), false
		}
		var err error
		t, err = dateparse.ParseAny(dateStr)
		if err != nil {
			return value.NewIntNil(), false
		}
	}

	return value.NewIntValue(int64(t.Weekday())), true
}

// hour of week [0-167]
type HourOfWeek struct{}

// Type int
func (m *HourOfWeek) Type() value.ValueType { return value.IntType }
func (m *HourOfWeek) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 or 1 args for hourofweek() but got %s", n)
	}
	return hourOfWeekEval, nil
}
func hourOfWeekEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	var t time.Time
	if len(vals) == 0 {
		if ctx != nil && !ctx.Ts().IsZero() {
			t = ctx.Ts()
		} else {
			t = time.Now()
		}
	} else {
		dateStr, ok := value.ValueToString(vals[0])
		if !ok {
			return value.NewIntValue(0), false
		}
		if t2, err := dateparse.ParseAny(dateStr); err == nil {
			t = t2
		}
	}
	if !t.IsZero() {
		return value.NewIntValue(int64(t.Weekday()*24) + int64(t.Hour())), true
	}

	return value.NewIntValue(0), false
}

// hourofday hour of day [0-23]
//
//    hourofday(field)
//    hourofday()  // Uses message time
//
type HourOfDay struct{}

// Type integer
func (m *HourOfDay) Type() value.ValueType { return value.IntType }
func (m *HourOfDay) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) > 1 {
		return nil, fmt.Errorf("Expected 0 or 1 args for HourOfDay(val) but got %s", n)
	}
	return hourOfDayEval, nil
}
func hourOfDayEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if len(vals) == 0 {
		if ctx != nil && !ctx.Ts().IsZero() {
			return value.NewIntValue(int64(ctx.Ts().Hour())), true
		}
		return value.NewIntValue(int64(time.Now().Hour())), true
	}
	dateStr, ok := value.ValueToString(vals[0])
	if !ok {
		return value.NewIntValue(0), false
	}
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		return value.NewIntValue(int64(t.Hour())), true
	}

	return value.NewIntValue(0), false
}

// totimestamp:   convert to date, then to unix Seconds
//
//    totimestamp() => int, true
//    totimestamp("Apr 7, 2014 4:58:55 PM") => 1396889935
//
type ToTimestamp struct{}

// Type integer
func (m *ToTimestamp) Type() value.ValueType { return value.IntType }
func (m *ToTimestamp) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for ToTimestamp(field) but got %s", n)
	}
	return toTimestampEval, nil
}
func toTimestampEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	dateStr, ok := value.ValueToString(args[0])
	if !ok {
		return value.NewIntValue(0), false
	}
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		return value.NewIntValue(int64(t.Unix())), true
	}
	return value.NewIntValue(0), false
}

// todate:   convert to Date
//
//    // uses lytics/datemath
//    todate("now-3m")
//
//    // uses araddon/dateparse util to recognize formats
//    todate(field)
//
//    // first parameter is the layout/format
//    todate("01/02/2006", field )
//
type ToDate struct{}

// Type time
func (m *ToDate) Type() value.ValueType { return value.TimeType }
func (m *ToDate) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 || len(n.Args) > 2 {
		return nil, fmt.Errorf(`Expected 1 or 2 args for ToDate([format] , field) but got %s`, n)
	}
	return toDateEval, nil
}
func toDateEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if len(args) == 1 {
		dateStr, ok := value.ValueToString(args[0])
		if !ok {
			return value.TimeZeroValue, false
		}

		if len(dateStr) > 3 && strings.ToLower(dateStr[:3]) == "now" {
			// Is date math
			if t, err := datemath.Eval(dateStr[3:]); err == nil {
				return value.NewTimeValue(t), true
			}
		} else {
			if t, err := dateparse.ParseAny(dateStr); err == nil {
				return value.NewTimeValue(t), true
			}
		}

	} else if len(args) == 2 {

		formatStr, ok := value.ValueToString(args[0])
		if !ok {
			return value.TimeZeroValue, false
		}

		dateStr, ok := value.ValueToString(args[1])
		if !ok {
			return value.TimeZeroValue, false
		}

		//u.Infof("hello  layout=%v  time=%v", formatStr, dateStr)
		if t, err := time.Parse(formatStr, dateStr); err == nil {
			return value.NewTimeValue(t), true
		}
	}

	return value.TimeZeroValue, false
}

// todatein:   convert to Date with timezon
//
//    // uses lytics/datemath
//    todate("now-3m", "America/Los_Angeles")
//
//    // uses araddon/dateparse util to recognize formats
//    todate(field, "America/Los_Angeles")
//
type ToDateIn struct{}

// Type time
func (m *ToDateIn) Type() value.ValueType { return value.TimeType }
func (m *ToDateIn) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf(`Expected args for todatein( (field | "now-3h" ), location) but got %s`, n)
	}

	sn, ok := n.Args[1].(*expr.StringNode)
	if !ok {
		return nil, fmt.Errorf("Expected a string literal value for location like America/Los_Angeles")
	}

	loc, err := time.LoadLocation(sn.Text)
	if err != nil {
		return nil, err
	}

	if sn, ok = n.Args[0].(*expr.StringNode); ok {
		dateStr := sn.Text
		if ok && len(dateStr) > 3 && strings.ToLower(dateStr[:3]) == "now" {
			_, err = datemath.Eval(dateStr)
			if err != nil {
				return nil, fmt.Errorf("%q was not able to be parsed %v", dateStr, err)
			}
			// Is date math
			return func(_ expr.EvalContext, _ []value.Value) (value.Value, bool) {
				t, _ := datemath.Eval(dateStr)
				return value.NewTimeValue(t), true
			}, nil
		}
	}

	// Return the Evaluator
	return func(_ expr.EvalContext, args []value.Value) (value.Value, bool) {
		valueDateStr, ok := value.ValueToString(args[0])
		if !ok {
			return value.TimeZeroValue, false
		}
		if t, err := dateparse.ParseIn(valueDateStr, loc); err == nil {
			// We are going to correct back to UTC. so all fields are in UTC.
			return value.NewTimeValue(t.In(time.UTC)), true
		}
		return value.TimeZeroValue, false
	}, nil
}

// TimeSeconds time in Seconds, parses a variety of formats looking for seconds
// See github.com/araddon/dateparse for formats supported on date parsing
//
//    seconds("M10:30")      =>  630
//    seconds("M100:30")     =>  6030
//    seconds("00:30")       =>  30
//    seconds("30")          =>  30
//    seconds(30)            =>  30
//    seconds("2015/07/04")  =>  1435968000
//
type TimeSeconds struct{}

// Type number
func (m *TimeSeconds) Type() value.ValueType { return value.NumberType }
func (m *TimeSeconds) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for TimeSeconds(field) but got %s", n)
	}
	return timeSecondsEval, nil
}

func timeSecondsEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	switch vt := args[0].(type) {
	case value.StringValue:
		ts := vt.ToString()
		// First, lets try to treat it as a time/date and
		// then extract unix seconds
		if tv, err := dateparse.ParseAny(ts); err == nil {
			return value.NewNumberValue(float64(tv.In(time.UTC).Unix())), true
		}

		// Since that didn't work, lets look for a variety of seconds/minutes type
		// pseudo standards
		//    M10:30
		//     10:30
		//    100:30
		//
		if strings.HasPrefix(ts, "M") {
			ts = ts[1:]
		}

		if strings.Contains(ts, ":") {
			parts := strings.Split(ts, ":")
			if len(parts) == 2 {
				min, sec := float64(0), float64(0)
				if iv, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
					min = float64(iv)
				} else if fv, err := strconv.ParseFloat(parts[0], 64); err == nil {
					min = fv
				}
				if iv, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
					sec = float64(iv)
				} else if fv, err := strconv.ParseFloat(parts[1], 64); err == nil {
					sec = fv
				}
				if min > 0 || sec > 0 {
					return value.NewNumberValue(60*min + sec), true
				}
			}
		} else {
			if iv, err := strconv.ParseInt(ts, 10, 64); err == nil {
				return value.NewNumberValue(float64(iv)), true
			}
			if fv, err := strconv.ParseFloat(ts, 64); err == nil {
				return value.NewNumberValue(fv), true
			}
		}
	case value.NumberValue:
		return vt, true
	case value.IntValue:
		return vt.NumberValue(), true
	}

	return value.NewNumberValue(0), false
}

// UnixDateTruncFunc converts a value.Value to a unix timestamp string. This is used for the BigQuery export
// since value.TimeValue returns a unix timestamp with milliseconds (ie "1438445529707") by default.
// This gets displayed in BigQuery as "47547-01-24 10:49:05 UTC", because they expect seconds instead of milliseconds.
// This function "truncates" the unix timestamp to seconds to the form "1438445529.707"
// i.e the milliseconds are placed after the decimal place.
// Inspired by the "DATE_TRUNC" function used in PostgresQL and RedShift:
// http://www.postgresql.org/docs/8.1/static/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC
//
//    unixtrunc("1438445529707") --> "1438445529"
//    unixtrunc("1438445529707", "seconds") --> "1438445529.707"
//
type TimeTrunc struct{ precision string }

// Type string
func (m *TimeTrunc) Type() value.ValueType { return value.StringType }
func (m *TimeTrunc) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 1 {
		return timeTruncEvalOne, nil
	} else if len(n.Args) == 2 {
		return timeTruncEvalTwo, nil
	}
	return nil, fmt.Errorf("Expected 1 or 2 args for unixtrunc(field) but got %s", n)
}

func timeTruncEvalOne(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	switch v := args[0].(type) {
	case value.TimeValue:
		// If the value is of type "TimeValue", return the Unix representation.
		return value.NewStringValue(fmt.Sprintf("%d", v.Time().Unix())), true
	default:
		// Otherwise use date parse any
		t, err := dateparse.ParseAny(v.ToString())
		if err != nil {
			return value.NewStringValue(""), false
		}
		return value.NewStringValue(fmt.Sprintf("%d", t.Unix())), true
	}
}
func timeTruncEvalTwo(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	valTs := int64(0)
	switch itemT := args[0].(type) {
	case value.TimeValue:
		// Get the full Unix timestamp w/ milliseconds.
		valTs = itemT.Int()
	default:
		// If not a TimeValue, convert to a TimeValue and get Unix w/ milliseconds.
		t, ok := value.ValueToTime(args[0])
		if !ok || t.IsZero() {
			return value.NewStringValue(""), false
		}
		valTs = t.UnixNano() / 1e6
	}

	// Look at the seconds argument to determine the truncation.
	precision, ok := value.ValueToString(args[1])
	if !ok {
		return value.NewStringValue(""), false
	}
	format := strings.ToLower(precision)

	switch format {
	case "s", "seconds":
		// If seconds: add milliseconds after the decimal place.
		return value.NewStringValue(fmt.Sprintf("%d.%d", valTs/1000, valTs%1000)), true
	case "ms", "milliseconds":
		// Otherwise return the Unix ts w/ milliseconds.
		return value.NewStringValue(fmt.Sprintf("%d", valTs)), true
	default:
		return value.EmptyStringValue, false
	}
}

// StrFromTime extraces certain parts from a time, similar to Python's StrfTime
// See http://strftime.org/ for Strftime directives.
//
//    strftime("2015/07/04", "%B")      => "July"
//    strftime("2015/07/04", "%B:%d")   => "July:4"
//    strftime("1257894000", "%p")      => "PM"
type StrFromTime struct{}

// Type string
func (m *StrFromTime) Type() value.ValueType { return value.StringType }
func (m *StrFromTime) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for strftime(field, format_pattern) but got %s", n)
	}
	return strFromTimeEval, nil
}

func strFromTimeEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	// if we have 2 items, the first is the time string
	// and the second is the format string.
	// Use leekchan/timeutil package
	dateStr, ok := value.ValueToString(args[0])
	if !ok {
		return value.EmptyStringValue, false
	}

	formatStr, ok := value.ValueToString(args[1])
	if !ok {
		return value.EmptyStringValue, false
	}

	t, err := dateparse.ParseAny(dateStr)
	if err != nil {
		return value.EmptyStringValue, false
	}

	formatted := timeutil.Strftime(&t, formatStr)
	return value.NewStringValue(formatted), true
}
