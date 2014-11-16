package exprvm

import (
	u "github.com/araddon/gou"
	"reflect"
	//ql "github.com/araddon/qlparser"
)

var (
	_ = u.EMPTY

	floatRv  = reflect.ValueOf(float64(1.2))
	int64Rv  = reflect.ValueOf(int64(1))
	stringRv = reflect.ValueOf("hello")
)

/*

TODO:
   - rename ot builtins.go, and or import from builtins?



// Describes a function
type Func struct {
	// The arguments we expect
	Args   []Value
	Return *reflect.Value
	F      interface{}
}
*/
var funcs = map[string]Func{
	"count": {
		[]Value{Value},
		TYPE_SCALAR,
		Count,
	},
	"eq": {
		[]FuncArgType{TYPE_STRING, TYPE_STRING},
		TYPE_SCALAR,
		Eq,
	},
	"toint": {
		[]FuncArgType{TYPE_STRING, TYPE_STRING, TYPE_STRING},
		TYPE_SCALAR,
		Count,
	},
}

func Count(e *state, item string) (r *Results, err error) {
	return &Results{
		Results: []*Result{
			{Value: Scalar(3)},
		},
	}, nil
}

func Eq(e *state, item string) (r *Results, err error) {
	u.Infof("in Eq:  %v")
	return &Results{
		Results: []*Result{
			{Value: Scalar(3)},
		},
	}, nil
}

// func reduce(e *state, series *Results, F func(Series, ...float64) float64, args ...float64) (*Results, error) {
// 	res := *series
// 	res.Results = nil
// 	for _, s := range series.Results {
// 		switch t := s.Value.(type) {
// 		case Series:
// 			if len(t) == 0 {
// 				continue
// 			}
// 			s.Value = Number(F(t, args...))
// 			res.Results = append(res.Results, s)
// 		default:
// 			panic(fmt.Errorf("expr: expected a series"))
// 		}
// 	}
// 	return &res, nil
// }
