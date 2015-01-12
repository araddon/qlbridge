package builtins

import (
	"math"
	"net/mail"
	"net/url"
	"reflect"
	"strings"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var _ = u.EMPTY

const yymmTimeLayout = "0601"

func LoadAllBuiltins() {
	expr.FuncAdd("gt", Gt)
	expr.FuncAdd("ge", Ge)
	expr.FuncAdd("ne", Ne)
	expr.FuncAdd("le", LeFunc)
	expr.FuncAdd("lt", LtFunc)
	expr.FuncAdd("not", NotFunc)
	expr.FuncAdd("eq", Eq)
	expr.FuncAdd("exists", Exists)
	expr.FuncAdd("yy", Yy)
	expr.FuncAdd("yymm", YyMm)
	expr.FuncAdd("mm", Mm)
	expr.FuncAdd("monthofyear", Mm)
	expr.FuncAdd("dayofweek", DayOfWeek)
	//expr.FuncAdd("hod", HourOfDay)
	expr.FuncAdd("hourofday", HourOfDay)
	expr.FuncAdd("hourofweek", HourOfWeek)
	expr.FuncAdd("totimestamp", ToTimestamp)
	expr.FuncAdd("todate", ToDate)

	expr.FuncAdd("contains", ContainsFunc)
	expr.FuncAdd("tolower", Lower)
	expr.FuncAdd("toint", ToInt)
	expr.FuncAdd("split", SplitFunc)
	expr.FuncAdd("join", JoinFunc)
	expr.FuncAdd("oneof", OneOfFunc)
	expr.FuncAdd("email", EmailFunc)
	expr.FuncAdd("emaildomain", EmailDomainFunc)
	expr.FuncAdd("emailname", EmailNameFunc)
	expr.FuncAdd("host", HostFunc)
	expr.FuncAdd("path", UrlPath)
	expr.FuncAdd("qs", Qs)

	expr.FuncAdd("count", CountFunc)

}

// Count
func CountFunc(ctx vm.EvalContext, val value.Value) (value.IntValue, bool) {
	if val.Err() || val.Nil() {
		return value.NewIntValue(0), false
	}
	//u.Infof("???   vals=[%v]", val.Value())
	return value.NewIntValue(1), true
}

//  Equal function?  returns true if items are equal
//
//      eq(item,5)
func Eq(ctx vm.EvalContext, itemA, itemB value.Value) (value.BoolValue, bool) {

	eq, err := value.Equal(itemA, itemB)
	//u.Infof("EQ:  %v  %v  ==? %v", itemA, itemB, eq)
	if err == nil {
		return value.NewBoolValue(eq), true
	}
	return value.BoolValueFalse, false
}

//  Not Equal function?  returns true if items are equal
//
//      ne(item,5)
func Ne(ctx vm.EvalContext, itemA, itemB value.Value) (value.BoolValue, bool) {
	eq, err := value.Equal(itemA, itemB)
	if err == nil {
		return value.NewBoolValue(eq), true
	}
	return value.BoolValueFalse, false
}

//  Not
//
//      eq(item,5)
func NotFunc(ctx vm.EvalContext, item value.Value) (value.BoolValue, bool) {
	boolVal, ok := value.ToBool(item.Rv())
	if ok {
		return value.NewBoolValue(!boolVal), true
	}
	return value.BoolValueFalse, false
}

// > GreaterThan
//  Must be able to convert items to Floats or else not ok
//
func Gt(ctx vm.EvalContext, lv, rv value.Value) (value.BoolValue, bool) {
	left := value.ToFloat64(lv.Rv())
	right := value.ToFloat64(rv.Rv())
	if left == math.NaN() || right == math.NaN() {
		return value.BoolValueFalse, false
	}

	return value.NewBoolValue(left > right), true
}

// >= GreaterThan or Equal
//  Must be able to convert items to Floats or else not ok
//
func Ge(ctx vm.EvalContext, lv, rv value.Value) (value.BoolValue, bool) {
	left := value.ToFloat64(lv.Rv())
	right := value.ToFloat64(rv.Rv())
	if left == math.NaN() || right == math.NaN() {
		return value.BoolValueFalse, false
	}

	return value.NewBoolValue(left >= right), true
}

// <= Less Than or Equal
//  Must be able to convert items to Floats or else not ok
//
func LeFunc(ctx vm.EvalContext, lv, rv value.Value) (value.BoolValue, bool) {
	left := value.ToFloat64(lv.Rv())
	right := value.ToFloat64(rv.Rv())
	if left == math.NaN() || right == math.NaN() {
		return value.BoolValueFalse, false
	}

	return value.NewBoolValue(left <= right), true
}

// < Less Than
//  Must be able to convert items to Floats or else not ok
//
func LtFunc(ctx vm.EvalContext, lv, rv value.Value) (value.BoolValue, bool) {
	left := value.ToFloat64(lv.Rv())
	right := value.ToFloat64(rv.Rv())
	if left == math.NaN() || right == math.NaN() {
		return value.BoolValueFalse, false
	}

	return value.NewBoolValue(left < right), true
}

//  Exists
func Exists(ctx vm.EvalContext, item interface{}) (value.BoolValue, bool) {

	u.Infof("Exists():  %T  %v", item, item)
	switch node := item.(type) {
	case expr.IdentityNode:
		_, ok := ctx.Get(node.Text)
		if ok {
			return value.BoolValueTrue, true
		}
		return value.BoolValueFalse, false
	case expr.StringNode:
		_, ok := ctx.Get(node.Text)
		if ok {
			return value.BoolValueTrue, true
		}
		return value.BoolValueFalse, false
	}
	return value.BoolValueFalse, false
}

// String contains
//   Will first convert to string, so may get unexpected results
//
func ContainsFunc(ctx vm.EvalContext, lv, rv value.Value) (value.BoolValue, bool) {
	left, leftOk := value.ToString(lv.Rv())
	right, rightOk := value.ToString(rv.Rv())
	if !leftOk || !rightOk {
		return value.BoolValueFalse, false
	}
	//u.Infof("Contains(%v, %v)", left, right)
	if left == "" || right == "" {
		return value.BoolValueFalse, false
	}
	if strings.Contains(left, right) {
		return value.BoolValueTrue, true
	}
	return value.BoolValueFalse, true
}

// String lower function
//   must be able to conver to string
//
func Lower(ctx vm.EvalContext, item value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(item.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.ToLower(val)), true
}

// choose OneOf these fields, first non-null
func OneOfFunc(ctx vm.EvalContext, vals ...value.Value) (value.Value, bool) {
	for _, v := range vals {
		if v.Err() || v.Nil() {
			// continue
		} else if !value.IsNilIsh(v.Rv()) {
			return v, true
		}
	}
	return value.EmptyStringValue, false
}

// Split a string, accepts an optional with parameter
//
//     split(item, ",")
//
func SplitFunc(ctx vm.EvalContext, input value.Value, splitByV value.StringValue) (value.StringsValue, bool) {

	sv, ok := value.ToString(input.Rv())
	splitBy, splitByOk := value.ToString(splitByV.Rv())
	if !ok || !splitByOk {
		return value.NewStringsValue(make([]string, 0)), false
	}
	if sv == "" {
		return value.NewStringsValue(make([]string, 0)), false
	}
	if splitBy == "" {
		return value.NewStringsValue(make([]string, 0)), false
	}
	vals := strings.Split(sv, splitBy)
	return value.NewStringsValue(vals), true
}

// Join items
//
//   join("applies","oranges",",") => "apples,oranges"
//
func JoinFunc(ctx vm.EvalContext, items ...value.Value) (value.StringValue, bool) {
	if len(items) <= 1 {
		return value.EmptyStringValue, false
	}
	sep, ok := value.ToString(items[len(items)-1].Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	args := make([]string, 0)
	for i := 0; i < len(items)-1; i++ {
		val, ok := value.ToString(items[i].Rv())
		if !ok {
			return value.EmptyStringValue, false
		}
		if val == "" {
			return value.EmptyStringValue, false
		}
		args = append(args, val)
	}
	return value.NewStringValue(strings.Join(args, sep)), true
}

func ToInt(ctx vm.EvalContext, item value.Value) (value.IntValue, bool) {
	iv, ok := value.ToInt64(reflect.ValueOf(item.Value()))
	if !ok {
		return value.NewIntValue(0), false
	}
	return value.NewIntValue(iv), true
}

// Get year in integer from date
func Yy(ctx vm.EvalContext, items ...value.Value) (value.IntValue, bool) {

	yy := 0
	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			yy = ctx.Ts().Year()
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, item.Rv())
		if t, err := dateparse.ParseAny(dateStr); err != nil {
			return value.NewIntValue(0), false
		} else {
			yy = t.Year()
		}
	} else {
		return value.NewIntValue(0), false
	}

	if yy >= 2000 {
		yy = yy - 2000
	} else if yy >= 1900 {
		yy = yy - 1900
	}
	//u.Infof("%v   yy = %v", item, yy)
	return value.NewIntValue(int64(yy)), true
}

// Get month as integer from date
//   @optional timestamp (if not, gets from context reader)
//
//  mm()
//  mm(date_identity)
//
func Mm(ctx vm.EvalContext, items ...value.Value) (value.IntValue, bool) {

	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewIntValue(int64(t.Month())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Month())), true
		}
	}

	return value.NewIntValue(0), false
}

// Get yymm in 4 digits from date  as string
//
func YyMm(ctx vm.EvalContext, items ...value.Value) (value.StringValue, bool) {

	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewStringValue(t.Format(yymmTimeLayout)), true
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.EmptyStringValue, false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewStringValue(t.Format(yymmTimeLayout)), true
		}
	}

	return value.EmptyStringValue, false
}

// day of week [0-6]
func DayOfWeek(ctx vm.EvalContext, items ...value.Value) (value.IntValue, bool) {

	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewIntValue(int64(t.Weekday())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Weekday())), true
		}
	}

	return value.NewIntValue(0), false
}

// hour of week [0-167]
func HourOfWeek(ctx vm.EvalContext, items ...value.Value) (value.IntValue, bool) {

	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			t := ctx.Ts()
			return value.NewIntValue(int64(t.Weekday()*24) + int64(t.Hour())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Weekday()*24) + int64(t.Hour())), true
		}
	}

	return value.NewIntValue(0), false
}

// hour of day [0-23]
func HourOfDay(ctx vm.EvalContext, items ...value.Value) (value.IntValue, bool) {

	if len(items) == 0 {
		if !ctx.Ts().IsZero() {
			return value.NewIntValue(int64(ctx.Ts().Hour())), true
		}
	} else if len(items) == 1 {
		dateStr, ok := value.ToString(items[0].Rv())
		if !ok {
			return value.NewIntValue(0), false
		}
		//u.Infof("v=%v   %v  ", v, items[0].Rv())
		if t, err := dateparse.ParseAny(dateStr); err == nil {
			return value.NewIntValue(int64(t.Hour())), true
		}
	}

	return value.NewIntValue(0), false
}

// totimestamp
func ToTimestamp(ctx vm.EvalContext, item value.Value) (value.IntValue, bool) {

	dateStr, ok := value.ToString(item.Rv())
	if !ok {
		return value.NewIntValue(0), false
	}
	//u.Infof("v=%v   %v  ", v, item.Rv())
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		return value.NewIntValue(int64(t.Unix())), true
	}

	return value.NewIntValue(0), false
}

// todate
func ToDate(ctx vm.EvalContext, item value.Value) (value.TimeValue, bool) {

	dateStr, ok := value.ToString(item.Rv())
	if !ok {
		return value.TimeZeroValue, false
	}
	//u.Infof("v=%v   %v  ", v, item.Rv())
	if t, err := dateparse.ParseAny(dateStr); err == nil {
		return value.NewTimeValue(t), true
	}

	return value.TimeZeroValue, false
}

// email a string, parses email
//
//     email("Bob <bob@bob.com>")  =>  bob@bob.com, true
//     email("Bob <bob>")          =>  "", false
//
func EmailFunc(ctx vm.EvalContext, item value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(item.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	if len(val) < 6 {
		return value.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(val); err == nil {
		u.Infof("found email?  '%v'", em.Address)
		return value.NewStringValue(strings.ToLower(em.Address)), true
	}

	return value.EmptyStringValue, false
}

// emailname a string, parses email
//
//     emailname("Bob <bob@bob.com>") =>  Bob
//
func EmailNameFunc(ctx vm.EvalContext, item value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(item.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	if len(val) < 6 {
		return value.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(val); err == nil {
		return value.NewStringValue(em.Name), true
	}

	return value.EmptyStringValue, false
}

// email a string, parses email
//
//     email("Bob <bob@bob.com>") =>  bob@bob.com
//
func EmailDomainFunc(ctx vm.EvalContext, item value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(item.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	if len(val) < 6 {
		return value.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(strings.ToLower(val)); err == nil {
		parts := strings.SplitN(strings.ToLower(em.Address), "@", 2)
		if len(parts) == 2 {
			return value.NewStringValue(parts[1]), true
		}
	}

	return value.EmptyStringValue, false
}

// Extract host from a String (must be urlish), doesn't do much/any validation
func HostFunc(ctx vm.EvalContext, item value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(item.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) < 8 {
		return value.EmptyStringValue, false
	}
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		//u.Infof("url.parse: %#v", urlParsed)
		return value.NewStringValue(urlParsed.Host), true
	}

	return value.EmptyStringValue, false
}

// Extract url path from a String (must be urlish), doesn't do much/any validation
func UrlPath(ctx vm.EvalContext, item value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(item.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) < 8 {
		return value.EmptyStringValue, false
	}
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		//u.Infof("url.parse: %#v", urlParsed)
		return value.NewStringValue(urlParsed.Path), true
	}

	return value.EmptyStringValue, false
}

// Extract host from a String (must be urlish), doesn't do much/any validation
func Qs(ctx vm.EvalContext, urlItem, keyItem value.Value) (value.StringValue, bool) {
	val, ok := value.ToString(urlItem.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) < 8 {
		return value.EmptyStringValue, false
	}
	keyVal, ok := value.ToString(keyItem.Rv())
	if !ok {
		return value.EmptyStringValue, false
	}
	if keyVal == "" {
		return value.EmptyStringValue, false
	}
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		//u.Infof("url.parse: %#v", urlParsed)
		qsval, ok := urlParsed.Query()[keyVal]
		if !ok {
			return value.EmptyStringValue, false
		}
		if len(qsval) > 0 {
			return value.NewStringValue(qsval[0]), true
		}
	}

	return value.EmptyStringValue, false
}
