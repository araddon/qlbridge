package es2gen

import (
	"fmt"
	"strings"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/generators/elasticsearch/gentypes"
)

// fieldType return the Elasticsearch field name for an identity node or an error.
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

	if ident.HasLeftRight() {
		ft, ok := s.ColumnInfo(ident.OriginalText())
		if ok {
			return ft, nil
		}
	}

	// This is legacy crap, we stupidly used to allow this:
	//  ticket to remove https://github.com/araddon/qlbridge/issues/159
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
