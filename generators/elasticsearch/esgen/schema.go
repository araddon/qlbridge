package esgen

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/generators/elasticsearch/gentypes"
	"github.com/araddon/qlbridge/value"
)

func exprValueType(s gentypes.SchemaColumns, n expr.Node) value.ValueType {

	switch nt := n.(type) {
	case *expr.NumberNode:
		if !nt.IsInt {
			return value.NumberType
		}
		return value.IntType
	case *expr.StringNode:
		return value.StringType
	}
	return value.UnknownType
}

// scalar returns a JSONable representation of a scalar node type for use in ES
// filters.
//
// Does not support Null.
//
func scalar(node expr.Node) (interface{}, bool) {
	switch n := node.(type) {

	case *expr.StringNode:
		return n.Text, true

	case *expr.NumberNode:
		if n.IsInt {
			// ES supports string encoded ints
			return n.Int64, true
		}
		return n.Float64, true

	case *expr.ValueNode:
		// Make sure this is a scalar value node
		switch n.Value.Type() {
		case value.BoolType, value.IntType, value.StringType, value.TimeType:
			return n.String(), true
		case value.NumberType:
			nn, ok := n.Value.(floatval)
			if !ok {
				return nil, false
			}
			return nn.Float(), true
		}
	case *expr.IdentityNode:
		if _, err := strconv.ParseBool(n.Text); err == nil {
			return n.Text, true
		}

	}
	return "", false
}

func fieldType(s gentypes.SchemaColumns, n expr.Node) (*gentypes.FieldType, error) {

	ident, ok := n.(*expr.IdentityNode)
	if !ok {
		return nil, fmt.Errorf("expected an identity but found %T (%s)", n, n)
	}

	// TODO: This shotgun approach sucks, see https://github.com/araddon/qlbridge/issues/159
	ft, ok := s.ColumnInfo(ident.Text)
	if ok {
		return ft, nil
	}

	//left, right, _ := expr.LeftRight(ident.Text)
	//u.Debugf("left:%q right:%q isNamespaced?%v   key=%v", left, right, ident.HasLeftRight(), ident.OriginalText())
	if ident.HasLeftRight() {
		ft, ok := s.ColumnInfo(ident.OriginalText())
		if ok {
			return ft, nil
		}
	}

	// This is legacy, we stupidly used to allow this:
	//
	//   `key_name.field value` -> "key_name", "field value"
	//
	// check if key is left.right
	parts := strings.SplitN(ident.Text, ".", 2)
	if len(parts) == 2 {
		// Nested field lookup
		ft, ok = s.ColumnInfo(parts[0])
		if ok {
			return ft, nil
		}
	}

	return nil, gentypes.MissingField(ident.OriginalText())
}

func fieldValueType(s gentypes.SchemaColumns, n expr.Node) (value.ValueType, error) {

	ident, ok := n.(*expr.IdentityNode)
	if !ok {
		return value.UnknownType, fmt.Errorf("expected an identity but found %T (%s)", n, n)
	}

	// TODO: This shotgun approach sucks, see https://github.com/araddon/qlbridge/issues/159
	vt, ok := s.Column(ident.Text)
	if ok {
		return vt, nil
	}

	//left, right, _ := expr.LeftRight(ident.Text)
	//u.Debugf("left:%q right:%q isNamespaced?%v   key=%v", left, right, ident.HasLeftRight(), ident.OriginalText())
	if ident.HasLeftRight() {
		vt, ok := s.Column(ident.OriginalText())
		if ok {
			return vt, nil
		}
	}

	// This is legacy, we stupidly used to allow this:
	//
	//   `key_name.field value` -> "key_name", "field value"
	//
	// check if key is left.right
	parts := strings.SplitN(ident.Text, ".", 2)
	if len(parts) == 2 {
		// Nested field lookup
		vt, ok = s.Column(parts[0])
		if ok {
			return vt, nil
		}
	}

	return value.UnknownType, gentypes.MissingField(ident.OriginalText())
}
