package vm

import (
	u "github.com/araddon/gou"
	"reflect"
	"testing"
)

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()
}

type Value struct {
	Kind  string
	Value string
}

func Bar(name string, item int) *Value {
	u.Warnf("NICE  IN BAR1")
	return nil
}
func TestExprCall(t *testing.T) {
	env := NewEnv()
	env.Define("foo", reflect.ValueOf(Bar))
	v, err := env.Get("foo")
	if err != nil {
		t.Fatalf(`Can't Get value for "foo" %v`, err)
	}
	if v.Kind() != reflect.Func {
		t.Fatalf(`Can't Get string value for "foo", %v`, v.Kind())
	}
	barStmt := NewFuncExpr("bar")
	if err != nil {
		t.Fatalf("no error? %v", err)
	}

	v2, err := Run([]Stmt{&barStmt}, env)
	u.Infof("out from Run():  %#v", v2)
	if err != nil {
		t.Fatalf("no error? %v", err)
	}

	// if v.String() != "bar" {
	// 	t.Fatalf("Expected %v, but %v:", "bar", v.String())
	// }
}
