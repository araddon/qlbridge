package builtins

import (
	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/vm"
	"math"
	"net/mail"
	"net/url"
	"reflect"
	"strings"
)

var _ = u.EMPTY

const yymmTimeLayout = "0601"

func LoadAllBuiltins() {
	vm.FuncAdd("gt", Gt)
	vm.FuncAdd("ge", Ge)
	vm.FuncAdd("ne", Ne)
	vm.FuncAdd("le", LeFunc)
	vm.FuncAdd("lt", LtFunc)
	vm.FuncAdd("not", NotFunc)
	vm.FuncAdd("eq", Eq)
	vm.FuncAdd("exists", Exists)
	vm.FuncAdd("yy", Yy)
	vm.FuncAdd("yymm", YyMm)
	vm.FuncAdd("mm", Mm)
	vm.FuncAdd("monthofyear", Mm)
	vm.FuncAdd("dayofweek", DayOfWeek)
	//vm.FuncAdd("hod", HourOfDay)
	vm.FuncAdd("hourofday", HourOfDay)
	vm.FuncAdd("hourofweek", HourOfWeek)
	vm.FuncAdd("totimestamp", ToTimestamp)
	vm.FuncAdd("todate", ToDate)

	vm.FuncAdd("contains", ContainsFunc)
	vm.FuncAdd("tolower", Lower)
	vm.FuncAdd("toint", ToInt)
	vm.FuncAdd("split", SplitFunc)
	vm.FuncAdd("join", JoinFunc)
	vm.FuncAdd("oneof", OneOfFunc)
	vm.FuncAdd("email", EmailFunc)
	vm.FuncAdd("emaildomain", EmailDomainFunc)
	vm.FuncAdd("emailname", EmailNameFunc)
	vm.FuncAdd("host", HostFunc)
	vm.FuncAdd("path", UrlPath)
	vm.FuncAdd("qs", Qs)

	vm.FuncAdd("count", CountFunc)

}

// Count
func CountFunc(s *vm.State, val vm.Value) (vm.IntValue, bool) {
	if val.Err() || val.Nil() {
		return vm.NewIntValue(0), false
	}
	//u.Infof("???   vals=[%v]", val.Value())
	return vm.NewIntValue(1), true
}

//  Equal function?  returns true if items are equal
//
//      eq(item,5)
func Eq(e *vm.State, itemA, itemB vm.Value) (vm.BoolValue, bool) {

	eq, err := vm.Equal(itemA, itemB)
	//u.Infof("EQ:  %v  %v  ==? %v", itemA, itemB, eq)
	if err == nil {
		return vm.NewBoolValue(eq), true
	}
	return vm.BoolValueFalse, false
}

//  Not Equal function?  returns true if items are equal
//
//      ne(item,5)
func Ne(e *vm.State, itemA, itemB vm.Value) (vm.BoolValue, bool) {
	eq, err := vm.Equal(itemA, itemB)
	if err == nil {
		return vm.NewBoolValue(eq), true
	}
	return vm.BoolValueFalse, false
}

//  Not
//
//      eq(item,5)
func NotFunc(e *vm.State, item vm.Value) (vm.BoolValue, bool) {
	boolVal, ok := vm.ToBool(item.Rv())
	if ok {
		return vm.NewBoolValue(!boolVal), true
	}
	return vm.BoolValueFalse, false
}

// > GreaterThan
//  Must be able to convert items to Floats or else not ok
//
func Gt(s *vm.State, lv, rv vm.Value) (vm.BoolValue, bool) {
	left := vm.ToFloat64(lv.Rv())
	right := vm.ToFloat64(rv.Rv())
	if left == math.NaN() || right == math.NaN() {
		return vm.BoolValueFalse, false
	}

	return vm.NewBoolValue(left > right), true
}

// >= GreaterThan or Equal
//  Must be able to convert items to Floats or else not ok
//
func Ge(s *vm.State, lv, rv vm.Value) (vm.BoolValue, bool) {
	left := vm.ToFloat64(lv.Rv())
	right := vm.ToFloat64(rv.Rv())
	if left == math.NaN() || right == math.NaN() {
		return vm.BoolValueFalse, false
	}

	return vm.NewBoolValue(left >= right), true
}

// <= Less Than or Equal
//  Must be able to convert items to Floats or else not ok
//
func LeFunc(s *vm.State, lv, rv vm.Value) (vm.BoolValue, bool) {
	left := vm.ToFloat64(lv.Rv())
	right := vm.ToFloat64(rv.Rv())
	if left == math.NaN() || right == math.NaN() {
		return vm.BoolValueFalse, false
	}

	return vm.NewBoolValue(left <= right), true
}

// < Less Than
//  Must be able to convert items to Floats or else not ok
//
func LtFunc(s *vm.State, lv, rv vm.Value) (vm.BoolValue, bool) {
	left := vm.ToFloat64(lv.Rv())
	right := vm.ToFloat64(rv.Rv())
	if left == math.NaN() || right == math.NaN() {
		return vm.BoolValueFalse, false
	}

	return vm.NewBoolValue(left < right), true
}

//  Exists
func Exists(e *vm.State, item interface{}) (vm.BoolValue, bool) {

	u.Infof("Exists():  %T  %v", item, item)
	switch node := item.(type) {
	case vm.IdentityNode:
		_, ok := e.Reader.Get(node.Text)
		if ok {
			return vm.BoolValueTrue, true
		}
		return vm.BoolValueFalse, false
	case vm.StringNode:
		_, ok := e.Reader.Get(node.Text)
		if ok {
			return vm.BoolValueTrue, true
		}
		return vm.BoolValueFalse, false
	}
	return vm.BoolValueFalse, false
}

// String contains
//   Will first convert to string, so may get unexpected results
//
func ContainsFunc(s *vm.State, lv, rv vm.Value) (vm.BoolValue, bool) {
	left, leftOk := vm.ToString(lv.Rv())
	right, rightOk := vm.ToString(rv.Rv())
	if !leftOk || !rightOk {
		return vm.BoolValueFalse, false
	}
	//u.Infof("Contains(%v, %v)", left, right)
	if left == "" || right == "" {
		return vm.BoolValueFalse, false
	}
	if strings.Contains(left, right) {
		return vm.BoolValueTrue, true
	}
	return vm.BoolValueFalse, true
}

// String lower function
//   must be able to conver to string
//
func Lower(s *vm.State, item vm.Value) (vm.StringValue, bool) {
	val, ok := vm.ToString(item.Rv())
	if !ok {
		return vm.EmptyStringValue, false
	}
	return vm.NewStringValue(strings.ToLower(val)), true
}

// choose OneOf these fields, first non-null
func OneOfFunc(s *vm.State, vals ...vm.Value) (vm.Value, bool) {
	for _, v := range vals {
		if v.Err() || v.Nil() {
			// continue
		} else if !vm.IsNilIsh(v.Rv()) {
			return v, true
		}
	}
	return vm.EmptyStringValue, false
}

// Split a string, accepts an optional with parameter
//
//     split(item, ",")
//
func SplitFunc(s *vm.State, input vm.Value, splitByV vm.StringValue) (vm.StringsValue, bool) {

	sv, ok := vm.ToString(input.Rv())
	splitBy, splitByOk := vm.ToString(splitByV.Rv())
	if !ok || !splitByOk {
		return vm.NewStringsValue(make([]string, 0)), false
	}
	if sv == "" {
		return vm.NewStringsValue(make([]string, 0)), false
	}
	if splitBy == "" {
		return vm.NewStringsValue(make([]string, 0)), false
	}
	vals := strings.Split(sv, splitBy)
	return vm.NewStringsValue(vals), true
}

// Join items
//
//   join("applies","oranges",",") => "apples,oranges"
//
func JoinFunc(s *vm.State, items ...vm.Value) (vm.StringValue, bool) {
	if len(items) <= 1 {
		return vm.EmptyStringValue, false
	}
	sep, ok := vm.ToString(items[len(items)-1].Rv())
	if !ok {
		return vm.EmptyStringValue, false
	}
	args := make([]string, 0)
	for i := 0; i < len(items)-1; i++ {
		val, ok := vm.ToString(items[i].Rv())
		if !ok {
			return vm.EmptyStringValue, false
		}
		if val == "" {
			return vm.EmptyStringValue, false
		}
		args = append(args, val)
	}
	return vm.NewStringValue(strings.Join(args, sep)), true
}

func ToInt(e *vm.State, item vm.Value) (vm.IntValue, bool) {
	iv, ok := vm.ToInt64(reflect.ValueOf(item.Value()))
	if !ok {
		return vm.NewIntValue(0), false
	}
	return vm.NewIntValue(iv), true
}

// Get year in integer from date
func Yy(e *vm.State, items ...vm.Value) (vm.IntValue, bool) {

	yy := 0
	if len(items) == 0 {
		if !e.Reader.Ts().IsZero() {
			yy = e.Reader.Ts().Year()
		}
	} else if len(items) == 1 {
		dateStr, ok := vm.ToString(items[0].Rv())
		if !ok {
			return vm.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, item.Rv())
		if t, err := dateparse.ParseAny(dateStr); err != nil {
			return vm.NewIntValue(0), false
		} else {
			yy = t.Year()
		}
	} else {
		return vm.NewIntValue(0), false
	}

	if yy >= 2000 {
		yy = yy - 2000
	} else if yy >= 1900 {
		yy = yy - 1900
	}
	//u.Infof("%v   yy = %v", item, yy)
	return vm.NewIntValue(int64(yy)), true
}

// Get month as integer from date
//   @optional timestamp (if not, gets from context reader)
//
//  mm()
//  mm(date_identity)
//
func Mm(e *vm.State, items ...vm.Value) (vm.IntValue, bool) {

	if len(items) == 0 {
		if !e.Reader.Ts().IsZero() {
			t := e.Reader.Ts()
			return vm.NewIntValue(int64(t.Month())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := vm.ToString(items[0].Rv())
		if !ok {
			return vm.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return vm.NewIntValue(int64(t.Month())), true
		}
	}

	return vm.NewIntValue(0), false
}

// Get yymm in 4 digits from date  as string
//
func YyMm(e *vm.State, items ...vm.Value) (vm.StringValue, bool) {

	if len(items) == 0 {
		if !e.Reader.Ts().IsZero() {
			t := e.Reader.Ts()
			return vm.NewStringValue(t.Format(yymmTimeLayout)), true
		}
	} else if len(items) == 1 {
		dateStr, ok := vm.ToString(items[0].Rv())
		if !ok {
			return vm.EmptyStringValue, false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return vm.NewStringValue(t.Format(yymmTimeLayout)), true
		}
	}

	return vm.EmptyStringValue, false
}

// day of week [0-6]
func DayOfWeek(e *vm.State, items ...vm.Value) (vm.IntValue, bool) {

	if len(items) == 0 {
		if !e.Reader.Ts().IsZero() {
			t := e.Reader.Ts()
			return vm.NewIntValue(int64(t.Weekday())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := vm.ToString(items[0].Rv())
		if !ok {
			return vm.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return vm.NewIntValue(int64(t.Weekday())), true
		}
	}

	return vm.NewIntValue(0), false
}

// hour of week [0-167]
func HourOfWeek(e *vm.State, items ...vm.Value) (vm.IntValue, bool) {

	if len(items) == 0 {
		if !e.Reader.Ts().IsZero() {
			t := e.Reader.Ts()
			return vm.NewIntValue(int64(t.Weekday()*24) + int64(t.Hour())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := vm.ToString(items[0].Rv())
		if !ok {
			return vm.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return vm.NewIntValue(int64(t.Weekday()*24) + int64(t.Hour())), true
		}
	}

	return vm.NewIntValue(0), false
}

// hour of day [0-23]
func HourOfDay(e *vm.State, items ...vm.Value) (vm.IntValue, bool) {

	if len(items) == 0 {
		if !e.Reader.Ts().IsZero() {
			return vm.NewIntValue(int64(e.Reader.Ts().Hour())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := vm.ToString(items[0].Rv())
		if !ok {
			return vm.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return vm.NewIntValue(int64(t.Hour())), true
		}
	}

	return vm.NewIntValue(0), false
}

// totimestamp
func ToTimestamp(e *vm.State, item vm.Value) (vm.IntValue, bool) {

	dateStr, ok := vm.ToString(item.Rv())
	if !ok {
		return vm.NewIntValue(0), false
	}
	//u.Infof("v=%v   %v  ", v, item.Rv())
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		return vm.NewIntValue(int64(t.Unix())), true
	}

	return vm.NewIntValue(0), false
}

// todate
func ToDate(e *vm.State, item vm.Value) (vm.TimeValue, bool) {

	dateStr, ok := vm.ToString(item.Rv())
	if !ok {
		return vm.TimeZeroValue, false
	}
	//u.Infof("v=%v   %v  ", v, item.Rv())
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		return vm.NewTimeValue(t), true
	}

	return vm.TimeZeroValue, false
}

// email a string, parses email
//
//     email("Bob <bob@bob.com>")  =>  bob@bob.com, true
//     email("Bob <bob>")          =>  "", false
//
func EmailFunc(s *vm.State, item vm.Value) (vm.StringValue, bool) {
	val, ok := vm.ToString(item.Rv())
	if !ok {
		return vm.EmptyStringValue, false
	}
	if val == "" {
		return vm.EmptyStringValue, false
	}
	if len(val) < 6 {
		return vm.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(val); err == nil {
		u.Infof("found email?  '%v'", em.Address)
		return vm.NewStringValue(strings.ToLower(em.Address)), true
	}

	return vm.EmptyStringValue, false
}

// emailname a string, parses email
//
//     emailname("Bob <bob@bob.com>") =>  Bob
//
func EmailNameFunc(s *vm.State, item vm.Value) (vm.StringValue, bool) {
	val, ok := vm.ToString(item.Rv())
	if !ok {
		return vm.EmptyStringValue, false
	}
	if val == "" {
		return vm.EmptyStringValue, false
	}
	if len(val) < 6 {
		return vm.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(val); err == nil {
		return vm.NewStringValue(em.Name), true
	}

	return vm.EmptyStringValue, false
}

// email a string, parses email
//
//     email("Bob <bob@bob.com>") =>  bob@bob.com
//
func EmailDomainFunc(s *vm.State, item vm.Value) (vm.StringValue, bool) {
	val, ok := vm.ToString(item.Rv())
	if !ok {
		return vm.EmptyStringValue, false
	}
	if val == "" {
		return vm.EmptyStringValue, false
	}
	if len(val) < 6 {
		return vm.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(strings.ToLower(val)); err == nil {
		parts := strings.SplitN(strings.ToLower(em.Address), "@", 2)
		if len(parts) == 2 {
			return vm.NewStringValue(parts[1]), true
		}
	}

	return vm.EmptyStringValue, false
}

// Extract host from a String (must be urlish), doesn't do much/any validation
func HostFunc(s *vm.State, item vm.Value) (vm.StringValue, bool) {
	val, ok := vm.ToString(item.Rv())
	if !ok {
		return vm.EmptyStringValue, false
	}
	if val == "" {
		return vm.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) < 8 {
		return vm.EmptyStringValue, false
	}
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		//u.Infof("url.parse: %#v", urlParsed)
		return vm.NewStringValue(urlParsed.Host), true
	}

	return vm.EmptyStringValue, false
}

// Extract url path from a String (must be urlish), doesn't do much/any validation
func UrlPath(s *vm.State, item vm.Value) (vm.StringValue, bool) {
	val, ok := vm.ToString(item.Rv())
	if !ok {
		return vm.EmptyStringValue, false
	}
	if val == "" {
		return vm.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) < 8 {
		return vm.EmptyStringValue, false
	}
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		//u.Infof("url.parse: %#v", urlParsed)
		return vm.NewStringValue(urlParsed.Path), true
	}

	return vm.EmptyStringValue, false
}

// Extract host from a String (must be urlish), doesn't do much/any validation
func Qs(s *vm.State, urlItem, keyItem vm.Value) (vm.StringValue, bool) {
	val, ok := vm.ToString(urlItem.Rv())
	if !ok {
		return vm.EmptyStringValue, false
	}
	if val == "" {
		return vm.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) < 8 {
		return vm.EmptyStringValue, false
	}
	keyVal, ok := vm.ToString(keyItem.Rv())
	if !ok {
		return vm.EmptyStringValue, false
	}
	if keyVal == "" {
		return vm.EmptyStringValue, false
	}
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		//u.Infof("url.parse: %#v", urlParsed)
		qsval, ok := urlParsed.Query()[keyVal]
		if !ok {
			return vm.EmptyStringValue, false
		}
		if len(qsval) > 0 {
			return vm.NewStringValue(qsval[0]), true
		}
	}

	return vm.EmptyStringValue, false
}
