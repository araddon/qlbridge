package expr

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY

	// the func mutext
	funcMu sync.Mutex
	funcs  = make(map[string]Func)
)

func FuncAdd(name string, fn interface{}) {
	funcMu.Lock()
	defer funcMu.Unlock()
	name = strings.ToLower(name)
	funcs[name] = MakeFunc(name, fn)
}

func FuncsGet() map[string]Func {
	return funcs
}

func MakeFunc(name string, fn interface{}) Func {

	f := Func{}
	f.Name = name

	funcRv := reflect.ValueOf(fn)
	funcType := funcRv.Type()

	// Verify Return Values are appropriate
	if funcType.NumOut() != 2 {
		panic(fmt.Sprintf("%s must have 2 return values:   %s(Value, bool)", name, name))
	}

	f.ReturnValueType = value.ValueTypeFromRT(funcType.Out(0))

	if funcType.Out(1).Kind() != reflect.Bool {
		panic("Must have bool as 3rd return value (Value, bool)")
	}
	f.F = funcRv
	methodNumArgs := funcType.NumIn()

	// first arg is always state type
	//if methodNumArgs > 0 && funcType.In(0) == reflect.TypeOf((*State)(nil)) {
	if methodNumArgs > 0 {
		methodNumArgs--
	}

	f.Args = make([]reflect.Value, methodNumArgs)
	if funcType.IsVariadic() {
		f.VariadicArgs = true
	}

	return f
}
