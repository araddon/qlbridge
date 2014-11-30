package vm

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	u "github.com/araddon/gou"
	ql "github.com/araddon/qlparser"
)

var _ = u.EMPTY

var NilValue = reflect.ValueOf((*interface{})(nil))
var TrueValue = reflect.ValueOf(true)
var FalseValue = reflect.ValueOf(false)

// Error provides a convenient interface for handling runtime error.
// It can be Error interface with type cast which can call Pos().
type Error struct {
	Message string
	Pos     Position
}

var BreakError = errors.New("Unexpected break statement")
var ContinueError = errors.New("Unexpected continue statement")
var ReturnError = errors.New("Unexpected return statement")

// NewStringError makes error interface with message.
func NewStringError(pos Pos, err string) error {
	return &Error{Message: err, Pos: pos.Position()}
}

// NewStringError makes error interface with message.
func NewErrorf(pos Pos, format string, args ...interface{}) error {
	return &Error{Message: fmt.Sprintf(format, args...), Pos: pos.Position()}
}

// NewError makes error interface with message. This doesn't overwrite last error.
func NewError(pos Pos, err error) error {
	if err == nil {
		return nil
	}
	if err == BreakError || err == ContinueError || err == ReturnError {
		return err
	}
	// if pe, ok := err.(*parser.Error); ok {
	// 	return pe
	// }
	if ee, ok := err.(*Error); ok {
		return ee
	}
	return &Error{Message: err.Error(), Pos: pos.Position()}
}

// Error return the error message.
func (e *Error) Error() string {
	return e.Message
}

// Func is function interface to reflect functions internaly.
type Func func(args ...reflect.Value) (reflect.Value, error)

func ToFunc(f Func) reflect.Value {
	return reflect.ValueOf(f)
}

// convert ql to runtime ast
func RuntimeAst(stmt ql.QlRequest) ([]Stmt, error) {
	sel := stmt.(*ql.SqlRequest)
	u.Info(sel)
	return nil, nil
	// for _, stmt := range sel.Columns {
	// 	if _, ok := stmt.(*BreakStmt); ok {
	// 		return NilValue, BreakError
	// 	}
	// 	if _, ok := stmt.(*ContinueStmt); ok {
	// 		return NilValue, ContinueError
	// 	}
	// 	rv, err = RunSingleStmt(stmt, env)
	// 	if _, ok := stmt.(*ReturnStmt); ok {
	// 		return reflect.ValueOf(rv), ReturnError
	// 	}
	// 	if err != nil {
	// 		return rv, NewError(stmt, err)
	// 	}
	// }
}

// Run execute statements in the environment which specified.
func Run(stmts []Stmt, env *Env) (reflect.Value, error) {
	rv := NilValue
	var err error
	for _, stmt := range stmts {
		rv, err = RunSingleStmt(stmt, env)
		if err != nil {
			return rv, NewError(stmt, err)
		}
	}
	return rv, nil
}

// RunSingleStmt execute one statement in the environment which specified.
func RunSingleStmt(stmt Stmt, env *Env) (reflect.Value, error) {
	switch stmt := stmt.(type) {
	case *ExprStmt:
		rv, err := invokeExpr(stmt.Expr, env)
		if err != nil {
			return rv, NewError(stmt, err)
		}
		return rv, nil
	case *FuncExpr:
		rv, err := invokeExpr(stmt, env)
		if err != nil {
			return rv, NewError(stmt, err)
		}
		return rv, nil
	default:
		return NilValue, NewStringError(stmt, "Unknown statement")
	}
}

// toString convert all reflect.Value-s into string.
func toString(v reflect.Value) string {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	if v.Kind() == reflect.String {
		return v.String()
	}
	if !v.IsValid() {
		return "nil"
	}
	return fmt.Sprint(v.Interface())
}

// toBool convert all reflect.Value-s into bool.
func toBool(v reflect.Value) bool {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return v.Float() != 0.0
	case reflect.Int, reflect.Int32, reflect.Int64:
		return v.Int() != 0
	case reflect.Bool:
		return v.Bool()
	case reflect.String:
		if v.String() == "true" {
			return true
		}
		if toInt64(v) != 0 {
			return true
		}
	}
	return false
}

// toFloat64 convert all reflect.Value-s into float64.
func toFloat64(v reflect.Value) float64 {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.Int, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	}
	return 0.0
}

func isNil(v reflect.Value) bool {
	if !v.IsValid() || v.Kind().String() == "unsafe.Pointer" {
		return true
	}
	if (v.Kind() == reflect.Interface || v.Kind() == reflect.Ptr) && v.IsNil() {
		return true
	}
	return false
}

func isNum(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

// equal return true when lhsV and rhsV is same value.
func equal(lhsV, rhsV reflect.Value) bool {
	if isNil(lhsV) && isNil(rhsV) {
		return true
	}
	if lhsV.Kind() == reflect.Interface || lhsV.Kind() == reflect.Ptr {
		lhsV = lhsV.Elem()
	}
	if rhsV.Kind() == reflect.Interface || rhsV.Kind() == reflect.Ptr {
		rhsV = rhsV.Elem()
	}
	if !lhsV.IsValid() || !rhsV.IsValid() {
		return true
	}
	if isNum(lhsV) && isNum(rhsV) {
		if rhsV.Type().ConvertibleTo(lhsV.Type()) {
			rhsV = rhsV.Convert(lhsV.Type())
		}
	}
	if lhsV.CanInterface() && rhsV.CanInterface() {
		return reflect.DeepEqual(lhsV.Interface(), rhsV.Interface())
	}
	return reflect.DeepEqual(lhsV, rhsV)
}

// toInt64 convert all reflect.Value-s into int64.
func toInt64(v reflect.Value) int64 {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		return int64(v.Float())
	case reflect.Int, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.String:
		s := v.String()
		var i int64
		var err error
		if strings.HasPrefix(s, "0x") {
			i, err = strconv.ParseInt(s, 16, 64)
		} else {
			i, err = strconv.ParseInt(s, 10, 64)
		}
		if err == nil {
			return int64(i)
		}
	}
	return 0
}

// invokeExpr evaluate one expression.
func invokeExpr(expr Expr, env *Env) (reflect.Value, error) {
	switch e := expr.(type) {
	case *CallExpr:
		f := NilValue

		if e.Func != nil {
			f = e.Func.(reflect.Value)
		} else {
			var err error
			ff, err := env.Get(e.Name)
			if err != nil {
				return f, err
			}
			f = ff
		}
		args := []reflect.Value{}
		_, isReflect := f.Interface().(Func)
		rets := f.Call(args)
		var ret reflect.Value
		var err error
		if isReflect {
			ev := rets[1].Interface()
			if ev != nil {
				err = ev.(error)
			}
			ret = rets[0].Interface().(reflect.Value)
		} else {
			if f.Type().NumOut() == 1 {
				ret = rets[0]
			} else {
				var result []interface{}
				for _, r := range rets {
					result = append(result, r.Interface())
				}
				ret = reflect.ValueOf(result)
			}
		}
		return ret, err
	case *FuncExpr:
		u.Infof("%#v   %T", e, e)
		f := reflect.ValueOf(func(expr *FuncExpr, env *Env) Func {
			u.Infof("in invokeExpr")
			return func(args ...reflect.Value) (reflect.Value, error) {
				u.Infof("in invokeExpr 2")
				if !expr.VarArg {
					if len(args) != len(expr.Args) {
						return NilValue, NewStringError(expr, "Arguments Number of mismatch")
					}
				}
				newenv := env.NewEnv()
				if expr.VarArg {
					newenv.Define(expr.Args[0], reflect.ValueOf(args))
				} else {
					for i, arg := range expr.Args {
						newenv.Define(arg, args[i])
					}
				}
				rr, err := Run(expr.Stmts, newenv)
				if err == ReturnError {
					err = nil
					rr = rr.Interface().(reflect.Value)
				}
				return rr, err
			}
		}(e, env))
		env.Define(e.Name, f)
		return f, nil
	default:
		return NilValue, NewStringError(expr, "Unknown expression")
	}
	//return nil, NewStringError(expr, "unknown expression")
}
