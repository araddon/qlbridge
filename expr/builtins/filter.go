package builtins

import (
	"fmt"
	"strings"

	"github.com/mb0/glob"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// OneOf choose the first non-nil, non-zero, non-false fields
//
//    oneof(nil, 0, "hello") => 'hello'
//
type OneOf struct{}

// Type unknown
func (m *OneOf) Type() value.ValueType { return value.UnknownType }
func (m *OneOf) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf("Expected 2 or more args for OneOf(arg, arg, ...) but got %s", n)
	}
	return oneOfEval, nil
}
func oneOfEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	for _, v := range args {
		if !v.Err() && !v.Nil() {
			return v, true
		}
	}
	return value.NilValueVal, false
}

// FilterFromArgs given set of values
func FiltersFromArgs(filterVals []value.Value) []string {
	filters := make([]string, 0, len(filterVals))
	for _, fv := range filterVals {
		switch fv := fv.(type) {
		case value.Slice:
			for _, fv := range fv.SliceValue() {
				matchKey := fv.ToString()
				if strings.Contains(matchKey, "%") {
					matchKey = strings.Replace(matchKey, "%", "*", -1)
				}
				filters = append(filters, matchKey)
			}
		case value.StringValue:
			matchKey := fv.ToString()
			if strings.Contains(matchKey, "%") {
				matchKey = strings.Replace(matchKey, "%", "*", -1)
			}
			filters = append(filters, matchKey)
		}
	}
	return filters
}

// Filter Filter OUT Values that match specified list of match filter criteria
//
// Operates on MapValue (map[string]interface{}), StringsValue ([]string), or string
// takes N Filter Criteria
// supports Matching:      "filter**" // matches  "filter_x", "filterstuff"
//
// Filter a map of values by key to remove certain keys
//
//    filter(match("topic_"),key_to_filter, key2_to_filter)  => {"goodkey": 22}, true
//
// Filter out VALUES (not keys) from a list of []string{} for a specific value
//
//    filter(split("apples,oranges",","),"ora*")  => ["apples"], true
//
// Filter out values for single strings
//
//    filter("apples","app*")      => []string{}, true
//
type Filter struct{}

// Type unknown
func (m *Filter) Type() value.ValueType { return value.UnknownType }

func (m *Filter) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf(`Expected 2 args for Filter("apples","ap") but got %s`, n)
	}
	return FilterEval, nil
}

func FilterEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if vals[0] == nil || vals[0].Nil() || vals[0].Err() {
		return nil, false
	}
	val := vals[0]
	filters := FiltersFromArgs(vals[1:])

	switch val := val.(type) {
	case value.MapValue:

		mv := make(map[string]interface{})

		for rowKey, v := range val.Val() {
			filteredOut := false
			for _, filter := range filters {
				if strings.Contains(filter, "*") {
					match, _ := glob.Match(filter, rowKey)
					if match {
						filteredOut = true
						break
					}
				} else {
					if strings.HasPrefix(rowKey, filter) && v != nil {
						filteredOut = true
						break
					}
				}
			}
			if !filteredOut {
				mv[rowKey] = v.Value()
			}
		}
		if len(mv) == 0 {
			return nil, false
		}
		return value.NewMapValue(mv), true

	case value.StringValue:
		anyMatches := false
		for _, filter := range filters {
			if strings.Contains(filter, "*") {
				match, _ := glob.Match(filter, val.Val())
				if match {
					anyMatches = true
					break
				}
			} else {
				if strings.HasPrefix(val.Val(), filter) {
					anyMatches = true
					break
				}
			}
		}
		if anyMatches {
			return value.NilValueVal, true
		}
		return val, true
	case value.Slice:

		lv := make([]string, 0, val.Len())

		for _, slv := range val.SliceValue() {
			sv := slv.ToString()
			filteredOut := false
			for _, filter := range filters {
				if strings.Contains(filter, "*") {
					match, _ := glob.Match(filter, sv)
					if match {
						filteredOut = true
						break
					}
				} else {
					if strings.HasPrefix(sv, filter) && sv != "" {
						filteredOut = true
						break
					}
				}
			}
			if !filteredOut {
				lv = append(lv, sv)
			}
		}
		if len(lv) == 0 {
			return nil, false
		}
		return value.NewStringsValue(lv), true
	}
	return nil, false
}

// FilterMatch  Filter IN Values that match specified list of match filter criteria
//
// Operates on MapValue (map[string]interface{}), StringsValue ([]string), or string
// takes N Filter Criteria
//
// Wildcard Matching:      "abcd*" // matches  "abcd_x", "abcdstuff"
//
// Filter a map of values by key to only keep certain keys
//
//    filtermatch(match("topic_"),key_to_filter, key2_to_filter)  => {"goodkey": 22}, true
//
// Filter in VALUES (not keys) from a list of []string{} for a specific value
//
//    filtermatch(split("apples,oranges",","),"ora*")  => ["oranges"], true
//
// Filter in values for single strings
//
//    filtermatch("apples","app*")      => []string{"apple"}, true
//
type FilterMatch struct{}

// Type Unknown
func (m *FilterMatch) Type() value.ValueType { return value.UnknownType }

func (m *FilterMatch) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf(`Expected 2 args for filtermatch("apples","ap") but got %s`, n)
	}
	return FilterMatchEval, nil
}

func FilterMatchEval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

	if vals[0] == nil || vals[0].Nil() || vals[0].Err() {
		return nil, false
	}
	val := vals[0]
	filters := FiltersFromArgs(vals[1:])

	//u.Debugf("FilterIn():  %T:%v   filters:%v", val, val, filters)
	switch val := val.(type) {
	case value.MapValue:

		mv := make(map[string]interface{})

		for rowKey, v := range val.Val() {
			filteredIn := false
			for _, filter := range filters {
				if strings.Contains(filter, "*") {
					match, _ := glob.Match(filter, rowKey)
					if match {
						filteredIn = true
						break
					}
				} else {
					if strings.HasPrefix(rowKey, filter) && v != nil {
						filteredIn = true
						break
					}
				}
			}
			if filteredIn {
				mv[rowKey] = v.Value()
			}
		}
		if len(mv) == 0 {
			return nil, false
		}
		return value.NewMapValue(mv), true

	case value.StringValue:
		anyMatches := false
		for _, filter := range filters {
			if strings.Contains(filter, "*") {
				match, _ := glob.Match(filter, val.Val())
				if match {
					anyMatches = true
					break
				}
			} else {
				if strings.HasPrefix(val.Val(), filter) {
					anyMatches = true
					break
				}
			}
		}
		if !anyMatches {
			return value.NilValueVal, true
		}
		return val, true
	case value.Slice:
		lv := make([]string, 0, val.Len())

		for _, slv := range val.SliceValue() {
			sv := slv.ToString()
			filteredIn := false
			for _, filter := range filters {
				if strings.Contains(filter, "*") {
					match, _ := glob.Match(filter, sv)
					if match {
						filteredIn = true
						break
					}
				} else {
					if strings.HasPrefix(sv, filter) && sv != "" {
						filteredIn = true
						break
					}
				}
			}
			if filteredIn {
				lv = append(lv, sv)
			}
		}
		if len(lv) == 0 {
			return nil, false
		}
		return value.NewStringsValue(lv), true
	}

	return nil, false
}
