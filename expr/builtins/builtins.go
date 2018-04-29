// Builtin functions are a library of functions natively available in
// qlbridge expression evaluation although adding your own is easy.
package builtins

import (
	"fmt"
	"sync"

	u "github.com/araddon/gou"
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
		expr.FuncAdd("tolower", &LowerCase{})
		expr.FuncAdd("string.lowercase", &LowerCase{})
		expr.FuncAdd("string.uppercase", &UpperCase{})
		expr.FuncAdd("string.titlecase", &TitleCase{})
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
		expr.FuncAdd("filtermatch", &FilterMatch{})

		// special functions
		expr.FuncAdd("email", &Email{})
		expr.FuncAdd("emaildomain", &EmailDomain{})
		expr.FuncAdd("emailname", &EmailName{})
		expr.FuncAdd("domain", &Domain{})
		expr.FuncAdd("domains", &Domains{})
		expr.FuncAdd("host", &Host{})
		expr.FuncAdd("hosts", &Hosts{})
		expr.FuncAdd("path", &UrlPath{})
		expr.FuncAdd("qs2", &Qs{})
		expr.FuncAdd("qs", &QsDeprecate{})
		expr.FuncAdd("qsl", &QsDeprecate{})
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

// uuid generates a new uuid
//
//    uuid() =>  "...."
//
type UuidGenerate struct{}

// Type string
func (m *UuidGenerate) Type() value.ValueType { return value.StringType }
func (m *UuidGenerate) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 0 {
		return nil, fmt.Errorf("Expected 0 arg for uuid() but got %s", n)
	}
	return uuidGenerateEval, nil
}

func uuidGenerateEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	return value.NewStringValue(uuid.New()), true
}
