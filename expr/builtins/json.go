package builtins

import (
	"encoding/json"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/jmespath/go-jmespath"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

// JsonPath jmespath json parser http://jmespath.org/
//
//     json_field = `[{"name":"n1","ct":8,"b":true, "tags":["a","b"]},{"name":"n2","ct":10,"b": false, "tags":["a","b"]}]`
//
//     json.jmespath(json_field, "[?name == 'n1'].name | [0]")  =>  "n1"
//
type JsonPath struct{}

func (m *JsonPath) Type() value.ValueType { return value.UnknownType }
func (m *JsonPath) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf(`Expected 2 args for json.jmespath(field,json_val) but got %s`, n)
	}

	jsonPathExpr := ""
	switch jn := n.Args[1].(type) {
	case *expr.StringNode:
		jsonPathExpr = jn.Text
	default:
		return nil, fmt.Errorf("expected a string expression for jmespath got %T", jn)
	}

	parser := jmespath.NewParser()
	_, err := parser.Parse(jsonPathExpr)
	if err != nil {
		// if syntaxError, ok := err.(jmespath.SyntaxError); ok {
		// 	u.Warnf("%s\n%s\n", syntaxError, syntaxError.HighlightLocation())
		// }
		return nil, err
	}
	return jsonPathEval(jsonPathExpr), nil
}

func jsonPathEval(expression string) expr.EvaluatorFunc {
	return func(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
		if args[0] == nil || args[0].Err() || args[0].Nil() {
			return nil, false
		}

		val := args[0].ToString()

		// Validate that this is valid json?
		var data interface{}
		if err := json.Unmarshal([]byte(val), &data); err != nil {
			return nil, false
		}

		result, err := jmespath.Search(expression, data)
		if err != nil {
			return nil, false
		}
		return value.NewValue(result), true
	}
}
