package exprvm

import (
	u "github.com/araddon/gou"
	"reflect"
	"sync"

	//ql "github.com/araddon/qlparser"
)

var (
	_ = u.EMPTY

	// the func mutext
	funcMu sync.Mutex
	funcs  = make(map[string]Func)
)

func init() {

	AddFunc("eq", Eq)
	AddFunc("toint", ToInt)
	AddFunc("count", Count)
}

func AddFunc(name string, fn interface{}) {
	funcMu.Lock()
	defer funcMu.Unlock()
	funcs[name] = MakeFunc(name, fn)
}

// Describes a function
type Func struct {
	Name string
	// The arguments we expect
	Args   []reflect.Value
	Return reflect.Value
	// The actual Function
	F reflect.Value
}

func MakeFunc(name string, fn interface{}) Func {
	f := Func{}
	f.Name = name

	funcRv := reflect.ValueOf(fn)
	funcType := funcRv.Type()
	f.F = funcRv
	methodNumArgs := funcType.NumIn()

	// Remove the State as first arg
	if methodNumArgs > 0 && funcType.In(0) == reflect.TypeOf((*State)(nil)) {
		methodNumArgs--
	}

	f.Args = make([]reflect.Value, methodNumArgs)
	// methodParamIdx := 0

	// for paramIdx, paramSpec := range params {
	// 	methodParamType := funcType.In(methodParamIdx)

	// 	paramVal, svcErr := getParam(a.Ctx, paramIdx, paramSpec, methodParamType)
	// 	//u.Debugf("%v  %v  %v", paramIdx, paramVal, methodParamType)
	// 	if svcErr != nil {
	// 		saneLog(a.Ctx, svcErr)
	// 		a.WriteBlob(svcErr.Code.HttpCode(), svcErr.CliErrMsg, svcErr.CliBody)
	// 		return RC_TERMINATE
	// 	}

	// 	funcArgsToPass[methodParamIdx] = paramVal
	// 	methodParamIdx++
	// }

	// Actually invoke the wrapped function to do the actual work.
	//methodRetVals := funcRv.Call(funcArgsToPass)

	return f
}
