// Builtin functions are a library of functions natively available in
// qlbridge expression evaluation although adding your own is easy.
package builtins

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	u "github.com/araddon/gou"
	"github.com/dchest/siphash"
	"github.com/jmespath/go-jmespath"
	"github.com/mb0/glob"
	"github.com/pborman/uuid"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY
var loadOnce sync.Once

const yymmTimeLayout = "0601"

func LoadAllBuiltins() {
	loadOnce.Do(func() {

		// math
		expr.FuncAdd("sqrt", &Sqrt{})
		expr.FuncAdd("pow", &Pow{})

		// aggregate ops
		expr.FuncAdd("count", &Count{})
		expr.FuncAdd("avg", &Avg{})
		expr.FuncAdd("sum", &Sum{})

		// logical
		expr.FuncAdd("gt", &Gt{})
		expr.FuncAdd("ge", &Ge{})
		expr.FuncAdd("ne", &Ne{})
		expr.FuncAdd("le", &Le{})
		expr.FuncAdd("lt", &Lt{})
		expr.FuncAdd("eq", &Eq{})
		expr.FuncAdd("not", &Not{})
		expr.FuncAdd("exists", &Exists{})
		expr.FuncAdd("any", &Any{})
		expr.FuncAdd("all", &All{})

		// Map
		expr.FuncAdd("map", &MapFunc{})

		// Date/Time functions
		expr.FuncAdd("todate", &ToDate{})
		expr.FuncAdd("totimestamp", &ToTimestamp{})
		expr.FuncAdd("todatein", &ToDateIn{})
		expr.FuncAdd("now", &Now{})
		expr.FuncAdd("yy", &Yy{})
		expr.FuncAdd("yymm", &YyMm{})
		expr.FuncAdd("mm", &Mm{})
		expr.FuncAdd("monthofyear", &Mm{})
		expr.FuncAdd("dayofweek", &DayOfWeek{})
		expr.FuncAdd("hourofday", &HourOfDay{})
		expr.FuncAdd("hourofweek", &HourOfWeek{})
		expr.FuncAdd("seconds", &TimeSeconds{})
		expr.FuncAdd("maptime", &MapTime{})
		expr.FuncAdd("extract", &StrFromTime{})
		expr.FuncAdd("strftime", &StrFromTime{})
		expr.FuncAdd("unixtrunc", &TimeTrunc{})

		// Casting and Type Coercion
		expr.FuncAdd("tostring", &ToString{})
		expr.FuncAdd("tobool", &ToBool{})
		expr.FuncAdd("toint", &ToInt{})
		expr.FuncAdd("tonumber", &ToNumber{})

		// String Functions
		expr.FuncAdd("contains", &Contains{})
		expr.FuncAdd("tolower", &Lower{})
		expr.FuncAdd("split", &Split{})
		expr.FuncAdd("strip", &Strip{})
		expr.FuncAdd("replace", &Replace{})
		expr.FuncAdd("join", &Join{})
		expr.FuncAdd("hassuffix", &HasSuffix{})
		expr.FuncAdd("hasprefix", &HasPrefix{})

		// array, string
		expr.FuncAdd("len", &Length{})
		expr.FuncAdd("array.index", &ArrayIndex{})
		expr.FuncAdd("array.slice", &ArraySlice{})

		// selection
		expr.FuncAdd("oneof", &OneOf{})
		expr.FuncAdd("match", &Match{})
		expr.FuncAdd("mapkeys", &MapKeys{})
		expr.FuncAdd("mapvalues", &MapValues{})
		expr.FuncAdd("mapinvert", &MapInvert{})
		expr.FuncAdd("filter", &Filter{})
		expr.FuncAdd("filtermatch", &FilterIn{})

		// special functions
		expr.FuncAdd("email", &Email{})
		expr.FuncAdd("emaildomain", &EmailDomain{})
		expr.FuncAdd("emailname", &EmailName{})
		expr.FuncAdd("domain", &Domain{})
		expr.FuncAdd("domains", &Domains{})
		expr.FuncAdd("host", &Host{})
		expr.FuncAdd("hosts", &Hosts{})
		expr.FuncAdd("path", &UrlPath{})
		expr.FuncAdd("qs", &Qs{})
		expr.FuncAdd("urlmain", &UrlMain{})
		expr.FuncAdd("urlminusqs", &UrlMinusQs{})
		expr.FuncAdd("urldecode", &UrlDecode{})
		expr.FuncAdd("url.matchqs", &UrlWithQuery{})
		expr.FuncAdd("useragent.map", &UserAgentMap{})
		expr.FuncAdd("useragent", &UserAgent{})
		expr.FuncAdd("uuid", &UuidGenerate{})

		// Hashing functions
		expr.FuncAdd("hash", &HashSip{})
		expr.FuncAdd("hash.sip", &HashSip{})
		expr.FuncAdd("hash.md5", &HashMd5{})
		expr.FuncAdd("hash.sha1", &HashSha1{})
		expr.FuncAdd("hash.sha256", &HashSha256{})
		expr.FuncAdd("hash.sha512", &HashSha512{})

		expr.FuncAdd("encoding.b64encode", &EncodeB64Encode{})
		expr.FuncAdd("encoding.b64decode", &EncodeB64Decode{})

		// json
		expr.FuncAdd("json.jmespath", &JsonPath{})

		// MySQL Builtins
		expr.FuncAdd("cast", &Cast{})
		expr.FuncAdd("char_length", &Length{})
	})
}

func emptyFunc(ctx expr.EvalContext, _ value.Value) (value.Value, bool) { return nil, true }

type UuidGenerate struct{}

// uuid generates a new uuid
//
func (m *UuidGenerate) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	return value.NewStringValue(uuid.New()), true
}
func (m *UuidGenerate) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 0 {
		return nil, fmt.Errorf("Expected 0 arg for uuid() but got %s", n)
	}
	return m.Eval, nil
}
func (m *UuidGenerate) Type() value.ValueType { return value.StringType }

type OneOf struct{}

// choose OneOf these fields, first non-null
func (m *OneOf) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	for _, v := range args {
		if v.Err() || v.Nil() {
			// continue, ignore
		} else {
			return v, true
		}
	}
	return value.NilValueVal, true
}
func (m *OneOf) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf("Expected 2 or more args for OneOf(arg, arg, ...) but got %s", n)
	}
	return m.Eval, nil
}
func (m *OneOf) Type() value.ValueType { return value.UnknownType }

func FiltersFromArgs(filterVals []value.Value) []string {
	filters := make([]string, 0, len(filterVals))
	for _, fv := range filterVals {
		switch fv := fv.(type) {
		case value.SliceValue:
			for _, fv := range fv.Val() {
				matchKey := fv.ToString()
				if strings.Contains(matchKey, "%") {
					matchKey = strings.Replace(matchKey, "%", "*", -1)
				}
				filters = append(filters, matchKey)
			}
		case value.StringsValue:
			for _, fv := range fv.Val() {
				matchKey := fv
				if strings.Contains(matchKey, "%") {
					matchKey = strings.Replace(matchKey, "%", "*", -1)
				}
				filters = append(filters, matchKey)
			}
		default:
			matchKey := fv.ToString()
			if strings.Contains(matchKey, "%") {
				matchKey = strings.Replace(matchKey, "%", "*", -1)
			}
			filters = append(filters, matchKey)
		}
	}
	return filters
}

type Filter struct{}

// Filter  Filter out Values that match specified list of match filter criteria
//
//   - Operates on MapValue (map[string]interface{}), StringsValue ([]string), or string
//   - takes N Filter Criteria
//   - supports Matching:      "filter*" // matches  "filter_x", "filterstuff"
//
//  -- Filter a map of values by key to remove certain keys
//    filter(match("topic_"),key_to_filter, key2_to_filter)  => {"goodkey": 22}, true
//
// -- Filter out VALUES (not keys) from a list of []string{} for a specific value
//    filter(split("apples,oranges",","),"ora*")  => ["apples"], true
//
// -- Filter out values for single strings
//    filter("apples","app*")      => []string{}, true
//
func (m *Filter) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
		return value.NewStringsValue(lv), true

	case nil, value.NilValue:
		// nothing we can do
		return nil, true
	default:
		u.Debugf("unsuported key type: %T %v", val, val)
	}

	//u.Warnf("could not find key: %T %v", item, item)
	return nil, false
}
func (m *Filter) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf(`Expected 2 args for Filter("apples","ap") but got %s`, n)
	}
	return m.Eval, nil
}
func (m *Filter) Type() value.ValueType { return value.UnknownType }

type FilterIn struct{}

// FilterIn  Filter IN Values that match specified list of match filter criteria
//
// Operates on MapValue (map[string]interface{}), StringsValue ([]string), or string
// takes N Filter Criteria
//
// Wildcard Matching:      "abcd*" // matches  "abcd_x", "abcdstuff"
//
// Filter a map of values by key to only keep certain keys
//
//    filterin(match("topic_"),key_to_filter, key2_to_filter)  => {"goodkey": 22}, true
//
// Filter in VALUES (not keys) from a list of []string{} for a specific value
//
//    filterin(split("apples,oranges",","),"ora*")  => ["oranges"], true
//
// Filter in values for single strings
//
//    filterin("apples","app*")      => []string{"apple"}, true
//
func (m *FilterIn) Eval(ctx expr.EvalContext, vals []value.Value) (value.Value, bool) {

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
	case value.StringsValue:
		lv := make([]string, 0, val.Len())

		for _, sv := range val.Val() {
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

		return value.NewStringsValue(lv), true

	case nil, value.NilValue:
		// nothing we can do
		return nil, true
	default:
		u.Debugf("unsuported key type: %T %v", val, val)
	}

	//u.Warnf("could not find key: %T %v", item, item)
	return nil, false
}
func (m *FilterIn) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) < 2 {
		return nil, fmt.Errorf(`Expected 2 args for FilterIn("apples","ap") but got %s`, n)
	}
	return m.Eval, nil
}
func (m *FilterIn) Type() value.ValueType { return value.UnknownType }

type HashSip struct{}

// hash.sip() hash a value to a 64 bit int
//
//     hash.sip("/blog/index.html")  =>  1234
//
func (m *HashSip) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	val := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.StringsValue:
		if len(itemT.Val()) == 0 {
			return value.NewIntValue(0), false
		}
		val = itemT.Val()[0]
	}
	if val == "" {
		return value.NewIntValue(0), false
	}

	hash := siphash.Hash(0, 1, []byte(val))

	return value.NewIntValue(int64(hash)), true
}
func (m *HashSip) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for hash.sip(field_to_hash) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HashSip) Type() value.ValueType { return value.IntType }

type HashMd5 struct{}

// HashMd5Func Hash a value to MD5 string
//
//     hash.md5("/blog/index.html")  =>  abc345xyz
//
func (m *HashMd5) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	hasher := md5.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}
func (m *HashMd5) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for hash.md5(field_to_hash) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HashMd5) Type() value.ValueType { return value.StringType }

type HashSha1 struct{}

// HashSha1Func Hash a value to SHA256 string
//
//     hash.sha1("/blog/index.html")  =>  abc345xyz
//
func (m *HashSha1) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	hasher := sha1.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}
func (m *HashSha1) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for HashSha1(field_to_hash) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HashSha1) Type() value.ValueType { return value.StringType }

type HashSha256 struct{}

// HashSha256Func Hash a value to SHA256 string
//
//     hash.sha256("/blog/index.html")  =>  abc345xyz
//
func (m *HashSha256) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	hasher := sha256.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}
func (m *HashSha256) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for HashSha256(field_to_hash) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HashSha256) Type() value.ValueType { return value.StringType }

type HashSha512 struct{}

// HashSha512Func Hash a value to SHA512 string
//
//     hash.sha512("/blog/index.html")  =>  abc345xyz
//
func (m *HashSha512) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	hasher := sha512.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}
func (m *HashSha512) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for HashSha512(field_to_hash) but got %s", n)
	}
	return m.Eval, nil
}
func (m *HashSha512) Type() value.ValueType { return value.StringType }

type EncodeB64Encode struct{}

// Base 64 encoding function
//
//     encoding.b64encode("hello world=")  =>  aGVsbG8gd29ybGQ=
//
func (m *EncodeB64Encode) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}
	encodedString := base64.StdEncoding.EncodeToString([]byte(args[0].ToString()))
	return value.NewStringValue(encodedString), true
}
func (m *EncodeB64Encode) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for encoding.b64encode(field) but got %s", n)
	}
	return m.Eval, nil
}
func (m *EncodeB64Encode) Type() value.ValueType { return value.StringType }

type EncodeB64Decode struct{}

// Base 64 encoding function
//
//     encoding.b64decode("aGVsbG8gd29ybGQ=")  =>  "hello world"
//
func (m *EncodeB64Decode) Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, true
	}

	by, err := base64.StdEncoding.DecodeString(args[0].ToString())
	if err != nil {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(string(by)), true
}
func (m *EncodeB64Decode) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for encoding.b64decode(field) but got %s", n)
	}
	return m.Eval, nil
}
func (m *EncodeB64Decode) Type() value.ValueType { return value.StringType }

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
