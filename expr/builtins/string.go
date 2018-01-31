package builtins

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

// Contains does first arg string contain 2nd arg?
//
//     contain("alabama","red") => false
//
type Contains struct{}

// Type is Bool
func (m *Contains) Type() value.ValueType { return value.BoolType }
func (m *Contains) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for contains(str_value, contains_this) but got %s", n)
	}
	return containsEval, nil
}

func containsEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	left, leftOk := value.ValueToString(args[0])
	right, rightOk := value.ValueToString(args[1])

	if !leftOk {
		// TODO:  this should be false, false?
		//        need to ensure doesn't break downstream
		return value.BoolValueFalse, true
	}
	if !rightOk {
		return value.BoolValueFalse, true
	}
	if left == "" || right == "" {
		return value.BoolValueFalse, false
	}
	if strings.Contains(left, right) {
		return value.BoolValueTrue, true
	}
	return value.BoolValueFalse, true
}

// LowerCase take a string and lowercase it. must be able to convert to string.
//
//    string.lowercase("HELLO") => "hello", true
type LowerCase struct{}

// Type string
func (m *LowerCase) Type() value.ValueType { return value.StringType }

func (m *LowerCase) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for string.lowercase(arg) but got %s", n)
	}
	return lowerCaseEval, nil
}
func lowerCaseEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	val, ok := value.ValueToString(args[0])
	if !ok {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.ToLower(val)), true
}

// UpperCase take a string and uppercase it. must be able to convert to string.
//
//    string.uppercase("hello") => "HELLO", true
type UpperCase struct{}

// Type string
func (m *UpperCase) Type() value.ValueType { return value.StringType }

func (m *UpperCase) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for string.uppercase(arg) but got %s", n)
	}
	return upperCaseEval, nil
}
func upperCaseEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	val, ok := value.ValueToString(args[0])
	if !ok {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.ToUpper(val)), true
}

// TitleCase take a string and uppercase it. must be able to convert to string.
//
//    string.uppercase("hello") => "HELLO", true
type TitleCase struct{}

// Type string
func (m *TitleCase) Type() value.ValueType { return value.StringType }

func (m *TitleCase) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for string.titlecase(arg) but got %s", n)
	}
	return titleCaseEval, nil
}
func titleCaseEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	val, ok := value.ValueToString(args[0])
	if !ok {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.Title(val)), true
}

// Split a string with given separator
//
//     split("apples,oranges", ",") => []string{"apples","oranges"}
//
type Split struct{}

// Type is Strings
func (m *Split) Type() value.ValueType { return value.StringsType }
func (m *Split) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf(`Expected 2 args for split("apples,oranges",",") but got %s`, n)
	}
	return splitEval, nil
}

func splitEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	sv, ok := value.ValueToString(vals[0])
	splitBy, splitByOk := value.ValueToString(vals[1])
	if !ok || !splitByOk {
		return value.NewStringsValue(make([]string, 0)), false
	}
	if sv == "" {
		return value.NewStringsValue(make([]string, 0)), false
	}
	if splitBy == "" {
		return value.NewStringsValue(make([]string, 0)), false
	}
	return value.NewStringsValue(strings.Split(sv, splitBy)), true
}

// Strip a string, removing leading/trailing whitespace
//
//    strip(split("apples, oranges ",",")) => {"apples", "oranges"}
//    strip("apples ")                     => "apples"
//
type Strip struct{}

// type is Unknown (string, or []string)
func (m *Strip) Type() value.ValueType { return value.UnknownType }
func (m *Strip) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf(`Expected 1 args for Strip(arg) but got %s`, n)
	}
	return stripEval, nil
}
func stripEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	switch val := vals[0].(type) {
	case value.StringValue:
		sv := strings.Trim(val.ToString(), " \n\t\r")
		return value.NewStringValue(sv), true
	case value.StringsValue:
		svs := make([]string, val.Len())
		for i, sv := range val.Val() {
			svs[i] = strings.Trim(sv, " \n\t\r")
		}
		return value.NewStringsValue(svs), true
	}
	return nil, false
}

// Replace a string(s).  Replace occurences of 2nd arg In first with 3rd.
// 3rd arg "what to replace with" is optional
//
//     replace("/blog/index.html", "/blog","")  =>  /index.html
//     replace("/blog/index.html", "/blog")  =>  /index.html
//     replace("/blog/index.html", "/blog/archive/","/blog")  =>  /blog/index.html
//     replace(item, "M")
//
type Replace struct{}

func (m *Replace) Type() value.ValueType { return value.StringType }
func (m *Replace) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 || len(n.Args) > 3 {
		return nil, fmt.Errorf(`Expected 2 or 3 args for Replace("apples","ap") but got %s`, n)
	}
	return replaceEval, nil
}

func replaceEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	val1 := vals[0].ToString()
	arg := vals[1]
	replaceWith := ""
	if len(vals) == 3 {
		replaceWith = vals[2].ToString()
	}
	val1 = strings.Replace(val1, arg.ToString(), replaceWith, -1)
	return value.NewStringValue(val1), true
}

// Join items together (string concatenation)
//
//   join("apples","oranges",",")   => "apples,oranges"
//   join(["apples","oranges"],",") => "apples,oranges"
//   join("apples","oranges","")    => "applesoranges"
//
type Join struct{}

// Type is string
func (m *Join) Type() value.ValueType { return value.StringType }
func (m *Join) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf(`Expected 2 or more args for Join("apples","ap") but got %s`, n)
	}
	return joinEval, nil
}

func joinEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	sep, ok := value.ValueToString(vals[len(vals)-1])
	if !ok {
		return value.EmptyStringValue, false
	}
	args := make([]string, 0)
	for i := 0; i < len(vals)-1; i++ {
		switch valTyped := vals[i].(type) {
		case value.SliceValue:
			svals := make([]string, len(valTyped.Val()))
			for i, sv := range valTyped.Val() {
				svals[i] = sv.ToString()
			}
			args = append(args, svals...)
		case value.StringsValue:
			svals := make([]string, len(valTyped.Val()))
			for i, sv := range valTyped.Val() {
				svals[i] = sv
			}
			args = append(args, svals...)
		case value.StringValue, value.NumberValue, value.IntValue:
			val := valTyped.ToString()
			if val == "" {
				continue
			}
			args = append(args, val)
		}
	}
	if len(args) == 0 {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(strings.Join(args, sep)), true
}

// HasPrefix string evaluation to see if string begins with
//
//   hasprefix("apples","ap")   => true
//   hasprefix("apples","o")   => false
//
type HasPrefix struct{}

// Type bool
func (m *HasPrefix) Type() value.ValueType { return value.BoolType }
func (m *HasPrefix) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf(`Expected 2 args for HasPrefix("apples","ap") but got %s`, n)
	}
	return hasPrefixEval, nil
}
func hasPrefixEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	prefixStr := vals[1].ToString()
	if len(prefixStr) == 0 {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(strings.HasPrefix(vals[0].ToString(), prefixStr)), true
}

// HasSuffix string evaluation to see if string ends with
//
//   hassuffix("apples","es")   => true
//   hassuffix("apples","e")   => false
//
type HasSuffix struct{}

// Type bool
func (m *HasSuffix) Type() value.ValueType { return value.BoolType }
func (m *HasSuffix) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf(`Expected 2 args for HasSuffix("apples","es") but got %s`, n)
	}
	return hasSuffixEval, nil
}
func hasSuffixEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {
	suffixStr := vals[1].ToString()
	if suffixStr == "" {
		return value.BoolValueFalse, false
	}
	return value.NewBoolValue(strings.HasSuffix(vals[0].ToString(), suffixStr)), true
}
