package es2gen

import (
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/generators/elasticsearch/gentypes"
)

/*
	Native go data types that map to the Elasticsearch
	Search DSL
*/
var _ = u.EMPTY

type BoolFilter struct {
	Occurs         BoolOccurrence `json:"bool"`
	MinShouldMatch int            `json:"minimum_should_match,omitempty"`
}

type BoolOccurrence struct {
	Filter  []interface{} `json:"filter,omitempty"`
	Should  []interface{} `json:"should,omitempty"`
	MustNot interface{}   `json:"must_not,omitempty"`
}

func AndFilter(v []interface{}) *BoolFilter { return &BoolFilter{Occurs: BoolOccurrence{Filter: v}} }
func OrFilter(v []interface{}) *BoolFilter  { return &BoolFilter{Occurs: BoolOccurrence{Should: v}} }
func NotFilter(v interface{}) *BoolFilter   { return &BoolFilter{Occurs: BoolOccurrence{MustNot: v}} }

// Filter structs

type exists struct {
	Exists map[string]string `json:"exists"`
}

// Exists creates a new Elasticsearch filter {"exists": {"field": field}}
func Exists(field *gentypes.FieldType) interface{} {
	//u.Debugf("exists?  nested?%v  for %s", field.Nested(), field.String())
	if field.Nested() {
		/*
			"nested": {
				"filter": {
				    "term": {
				        "map_actioncounts.k": "Web hit"
				    }
				},
				"path": "map_actioncounts"
			}
		*/
		return &nested{&NestedFilter{
			Filter: Term(field.Path+".k", field.Field),
			Path:   field.Path,
		}}
		//Nested(field.Path, &term{map[string][]string{"k": field.Field}})
	}
	return &exists{map[string]string{"field": field.Field}}
}

type and struct {
	Filters []interface{} `json:"and"`
}

type in struct {
	Terms map[string][]interface{} `json:"terms"`
}

// In creates a new Elasticsearch terms filter {"terms": {field: values}}
func In(field *gentypes.FieldType, values []interface{}) interface{} {
	if field.Nested() {
		return &nested{&NestedFilter{
			Filter: &and{
				Filters: []interface{}{
					&in{map[string][]interface{}{field.PathAndPrefix(""): values}},
					Term(field.Path+".k", field.Field),
				},
			},
			Path: field.Path,
		}}
	}
	return &in{map[string][]interface{}{field.Field: values}}
}

// In creates a new Elasticsearch nested filter
// { "nested": {
//      "filter": {"and":[
//               {"term": { "k":fieldName}},
//               filter,
//      ]} ,
//      "path":"path_to_obj"
//  }}
func Nested(field *gentypes.FieldType, filter interface{}) *nested {

	// Hm.  Elasticsearch doc seems to insinuate we don't need
	// this path + ".k" but unit tests say otherwise
	fl := []interface{}{
		Term(field.Path+".k", field.Field),
		filter,
	}
	return &nested{&NestedFilter{
		Filter: &and{fl},
		Path:   field.Path,
	}}
}

type nested struct {
	Nested *NestedFilter `json:"nested,omitempty"`
}

type NestedFilter struct {
	Filter interface{} `json:"filter"`
	Path   string      `json:"path"`
}

type RangeQry struct {
	GTE interface{} `json:"gte,omitempty"`
	LTE interface{} `json:"lte,omitempty"`
	GT  interface{} `json:"gt,omitempty"`
	LT  interface{} `json:"lt,omitempty"`
}

type RangeFilter struct {
	Range map[string]RangeQry `json:"range"`
}

type term struct {
	Term map[string]interface{} `json:"term"`
}

// Term creates a new Elasticsearch term filter {"term": {field: value}}
func Term(fieldName string, value interface{}) *term {
	return &term{map[string]interface{}{fieldName: value}}
}

type matchall struct {
	MatchAll *struct{} `json:"match_all"`
}

// MatchAll maps to the Elasticsearch "match_all" filter
var MatchAll = &matchall{&struct{}{}}

// MatchNone matches no documents.
var MatchNone = NotFilter(MatchAll)

type wildcard struct {
	Wildcard map[string]string `json:"wildcard"`
}

type wildcardquery struct {
	Query wildcard `json:"query"`
}

func wcFunc(val string) string {
	if len(val) < 1 {
		return val
	}
	if val[0] == '*' || val[len(val)-1] == '*' {
		return val
	}
	if !strings.HasPrefix(val, "*") {
		val = "*" + val
	}
	if !strings.HasSuffix(val, "*") {
		val = val + "*"
	}
	return val
}

// Wilcard creates a new Elasticserach wildcard query
//
//   {"query": {"wildcard": {field: value}}}
//
// nested
//  {"nested": {
//     "filter" : { "and" : [
//             {"query": {"wildcard": {"v": value}}},
//             {"term":{"k": field_key}}
//     "path": path
//    }
//  }
func Wildcard(field, value string) *wildcardquery {
	return &wildcardquery{Query: wildcard{Wildcard: map[string]string{field: wcFunc(value)}}}
}
