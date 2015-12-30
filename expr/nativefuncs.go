package expr

import (
	"math"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

const yymmTimeLayout = "0601"

func init() {
	// agregate ops
	AggFuncAdd("count", CountFunc)

	// math
	FuncAdd("sqrt", SqrtFunc)
	FuncAdd("pow", PowFunc)
}

// Count
func CountFunc(ctx EvalContext, val value.Value) (value.IntValue, bool) {
	if val.Err() || val.Nil() {
		return value.NewIntValue(0), false
	}
	//u.Infof("???   vals=[%v]", val.Value())
	return value.NewIntValue(1), true
}

// Sqrt
func SqrtFunc(ctx EvalContext, val value.Value) (value.NumberValue, bool) {
	//func Sqrt(x float64) float64
	nv, ok := val.(value.NumericValue)
	if !ok {
		return value.NewNumberValue(math.NaN()), false
	}
	if val.Err() || val.Nil() {
		return value.NewNumberValue(0), false
	}
	fv := nv.Float()
	fv = math.Sqrt(fv)
	//u.Infof("???   vals=[%v]", val.Value())
	return value.NewNumberValue(fv), true
}

// Pow
func PowFunc(ctx EvalContext, val, toPower value.Value) (value.NumberValue, bool) {
	//Pow(x, y float64) float64
	//u.Infof("powFunc:  %T:%v %T:%v ", val, val.Value(), toPower, toPower.Value())
	if val.Err() || val.Nil() {
		return value.NewNumberValue(0), false
	}
	if toPower.Err() || toPower.Nil() {
		return value.NewNumberValue(0), false
	}
	fv, _ := value.ToFloat64(val.Rv())
	pow, _ := value.ToFloat64(toPower.Rv())
	if math.IsNaN(fv) || math.IsNaN(pow) {
		return value.NewNumberValue(0), false
	}
	fv = math.Pow(fv, pow)
	//u.Infof("pow ???   vals=[%v]", fv, pow)
	return value.NewNumberValue(fv), true
}
