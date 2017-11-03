// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package datasource

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

type ContextWrapper struct {
	val reflect.Value
	s   *state
}

func NewContextWrapper(val interface{}) *ContextWrapper {
	s := state{}
	return &ContextWrapper{reflect.ValueOf(val), &s}
}
func (m *ContextWrapper) Get(key string) (value.Value, bool) {
	defer func() { recover() }()
	keyParts := strings.Split(key, ".")
	dot := m.val
	var final reflect.Value
	ident := expr.NewIdentityNodeVal(key)
	// Now if it's a method, it gets the arguments.
	final = m.s.evalFieldChain(dot, dot, ident, keyParts, nil, final)
	if final == zero {
		return nil, false
	}
	if m.s.err != nil {
		return nil, false
	}
	val := value.NewValue(final.Interface())
	if val == nil {
		return nil, false
	}
	return val, true
}
func (m *ContextWrapper) Row() map[string]value.Value { return nil }
func (m *ContextWrapper) Ts() time.Time               { return time.Time{} }

// unwind pointers, etc to find either the value or flag indicating was nil
func findValue(v reflect.Value) (reflect.Value, bool) {
	for ; v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface; v = v.Elem() {
		if v.IsNil() {
			return v, true
		}
		if v.Kind() == reflect.Interface && v.NumMethod() > 0 {
			break
		}
	}
	return v, false
}

var zero reflect.Value

type state struct {
	stack []namedvar
	err   error
}

// our stack vars that have come from strings in vm eval engine
//    such as "user.Name" will try to find struct value with .Name
type namedvar struct {
	name  string
	value reflect.Value
}

var (
	errorType       = reflect.TypeOf((*error)(nil)).Elem()
	fmtStringerType = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
)

func (s *state) errorf(format string, args ...interface{}) reflect.Value {
	s.err = fmt.Errorf(format, args...)
	return zero
}

// evalFieldChain evaluates .X.Y.Z possibly followed by arguments.
// dot is the environment in which to evaluate arguments, while
// receiver is the value being walked along the chain.
func (s *state) evalFieldChain(dot, receiver reflect.Value, node *expr.IdentityNode, ident []string, args []expr.Node, final reflect.Value) reflect.Value {
	n := len(ident)
	for i := 0; i < n-1; i++ {
		receiver = s.evalField(dot, ident[i], node, nil, zero, receiver)
	}
	// Now if it's a method, it gets the arguments.
	return s.evalField(dot, ident[n-1], node, args, final, receiver)
}

// func (s *state) evalFunction(dot reflect.Value, node *expr.IdentityNode, cmd expr.Node, args []expr.Node, final reflect.Value) reflect.Value {
// 	name := node.Text
// 	function, ok := findFunction(name, s.tmpl)
// 	if !ok {
// 		return s.errorf("%q is not a defined function", name)
// 	}
// 	return s.evalCall(dot, function, cmd, name, args, final)
// }

func lowerFieldMatch(fieldName string) func(string) bool {
	lowerField := strings.ToLower(fieldName)
	return func(field string) bool {
		//u.Debugf("check: %s == %s ?", field, lowerField)
		return strings.ToLower(field) == lowerField
	}
}

// evalField evaluates an expression like (.Field) or (.Field arg1 arg2).
// The 'final' argument represents the return value from the preceding
// value of the pipeline, if any.
func (s *state) evalField(dot reflect.Value, fieldName string, node expr.Node, args []expr.Node, final, receiver reflect.Value) reflect.Value {

	//u.Debugf("evalField: valid?%v", receiver.IsValid())
	if !receiver.IsValid() {
		//u.Warnf("bailing")
		return zero
	}
	typ := receiver.Type()
	receiver, _ = findValue(receiver)
	// Unless it's an interface, need to get to a value of type *T to guarantee
	// we see all methods of T and *T.
	ptr := receiver
	if ptr.Kind() != reflect.Interface && ptr.CanAddr() {
		ptr = ptr.Addr()
	}
	if method := ptr.MethodByName(fieldName); method.IsValid() {
		//u.Warnf("unimplemented method: %v", fieldName)
		return s.evalCall(dot, method, node, fieldName, args, final)
	}
	hasArgs := len(args) > 1 || final.IsValid()
	// It's not a method; must be a field of a struct or an element of a map. The receiver must not be nil.
	receiver, isNil := findValue(receiver)
	//u.Debugf("fld:%s  receiver kind():%v  val: %v", fieldName, receiver.Kind(), receiver)
	if isNil {
		return zero
	}
	switch receiver.Kind() {
	case reflect.Struct:
		tField, ok := receiver.Type().FieldByName(fieldName)
		if !ok {
			tField, ok = receiver.Type().FieldByNameFunc(lowerFieldMatch(fieldName))
			if !ok {
				tagName := strings.ToLower(fieldName)
				// Wow, this is pretty bruttaly expensive
				// Iterate over all available fields and read the tag value
				for i := 0; i < receiver.NumField(); i++ {
					// Get the field, returns https://golang.org/pkg/reflect/#StructField
					field := receiver.Type().Field(i)

					// Get the field tag value
					tag := field.Tag.Get("json")
					if tag == tagName {
						tField = field
						ok = true
						break
					}
				}
			}
		}
		//u.Infof("got field? %v", fieldName, tField)
		if ok {
			field := receiver.FieldByIndex(tField.Index)
			if tField.PkgPath != "" { // field is unexported
				return s.errorf("%s is an unexported field of struct type %s", fieldName, typ)
			}
			// If it's a function, we must call it.
			if hasArgs {
				return s.errorf("%s has arguments but cannot be invoked as function", fieldName)
			}
			return field
		}
		//context reader doesn't care about empty values
		return zero
	case reflect.Map:
		// If it's a map, attempt to use the field name as a key.
		nameVal := reflect.ValueOf(fieldName)
		if nameVal.Type().AssignableTo(receiver.Type().Key()) {
			if hasArgs {
				return s.errorf("%s is not a method but has arguments", fieldName)
			}
			result := receiver.MapIndex(nameVal)
			if !result.IsValid() {
				u.Errorf("could not evaluate %v", nameVal)
				// switch s.tmpl.option.missingKey {
				// case mapInvalid:
				// 	// Just use the invalid value.
				// case mapZeroValue:
				// 	result = reflect.Zero(receiver.Type().Elem())
				// case mapError:
				// 	s.errorf("map has no entry for key %q", fieldName)
				// }
			}
			return result
		}
	}
	s.errorf("can't evaluate field %s in type %s", fieldName, typ)
	panic("not reached")
}

func (s *state) evalCall(dot, fun reflect.Value, node expr.Node, name string, args []expr.Node, final reflect.Value) reflect.Value {
	typ := fun.Type()
	if !goodFunc(typ) {
		// TODO: This could still be a confusing error; maybe goodFunc should provide info.
		return s.errorf("can't call method/function %q with %d results", name, typ.NumOut())
	}
	// Build the arg list.
	argv := make([]reflect.Value, 0)
	result := fun.Call(argv)
	// If we have an error that is not nil, stop execution and return that error to the caller.
	if len(result) == 2 && !result[1].IsNil() {
		return s.errorf("error calling %s: %s", name, result[1].Interface().(error))
	}
	return result[0]
}

/*
// evalCall executes a function or method call. If it's a method, fun already has the receiver bound, so
// it looks just like a function call.  The arg list, if non-nil, includes (in the manner of the shell), arg[0]
// as the function itself.
func (s *state) evalCall(dot, fun reflect.Value, node expr.Node, name string, args []expr.Node, final reflect.Value) reflect.Value {
	if args != nil {
		args = args[1:] // Zeroth arg is function name/node; not passed to function.
	}
	typ := fun.Type()
	numIn := len(args)
	if final.IsValid() {
		numIn++
	}
	numFixed := len(args)
	if typ.IsVariadic() {
		numFixed = typ.NumIn() - 1 // last arg is the variadic one.
		if numIn < numFixed {
			s.errorf("wrong number of args for %s: want at least %d got %d", name, typ.NumIn()-1, len(args))
		}
	} else if numIn < typ.NumIn()-1 || !typ.IsVariadic() && numIn != typ.NumIn() {
		s.errorf("wrong number of args for %s: want %d got %d", name, typ.NumIn(), len(args))
	}
	if !goodFunc(typ) {
		// TODO: This could still be a confusing error; maybe goodFunc should provide info.
		s.errorf("can't call method/function %q with %d results", name, typ.NumOut())
	}
	// Build the arg list.
	argv := make([]reflect.Value, numIn)
	// Args must be evaluated. Fixed args first.
	i := 0
	for ; i < numFixed && i < len(args); i++ {
		argv[i] = s.evalArg(dot, typ.In(i), args[i])
	}
	// Now the ... args.
	if typ.IsVariadic() {
		argType := typ.In(typ.NumIn() - 1).Elem() // Argument is a slice.
		for ; i < len(args); i++ {
			argv[i] = s.evalArg(dot, argType, args[i])
		}
	}
	// Add final value if necessary.
	if final.IsValid() {
		t := typ.In(typ.NumIn() - 1)
		if typ.IsVariadic() {
			if numIn-1 < numFixed {
				// The added final argument corresponds to a fixed parameter of the function.
				// Validate against the type of the actual parameter.
				t = typ.In(numIn - 1)
			} else {
				// The added final argument corresponds to the variadic part.
				// Validate against the type of the elements of the variadic slice.
				t = t.Elem()
			}
		}
		argv[i] = s.validateType(final, t)
	}
	result := fun.Call(argv)
	// If we have an error that is not nil, stop execution and return that error to the caller.
	if len(result) == 2 && !result[1].IsNil() {
		s.at(node)
		s.errorf("error calling %s: %s", name, result[1].Interface().(error))
	}
	return result[0]
}
*/

// goodFunc checks that the function or method has the right result signature.
func goodFunc(typ reflect.Type) bool {
	// We allow functions with 1 result or 2 results where the second is an error.
	switch {
	case typ.NumOut() == 1:
		return true
	case typ.NumOut() == 2 && typ.Out(1) == errorType:
		return true
	}
	return false
}
