package builtins

import (
	"fmt"
	"net/mail"
	"net/url"
	"regexp"
	"strings"

	u "github.com/araddon/gou"
	"github.com/mssola/user_agent"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

var _ = u.EMPTY

// email a string, parses email and makes sure it is valid
//
//     email("Bob <bob@bob.com>")  =>  bob@bob.com, true
//     email("Bob <bob>")          =>  "", false
//
type Email struct{}

// Type string
func (m *Email) Type() value.ValueType { return value.StringType }
func (m *Email) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for Email(field) but got %s", n)
	}
	return emailEval, nil
}

func emailEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := args[0]
	if val == nil || val.Nil() || val.Err() {
		return nil, false
	}
	emailStr := ""
	switch v := val.(type) {
	case value.StringValue:
		emailStr = v.ToString()
	case value.Slice:
		if v.Len() > 0 {
			v1 := v.SliceValue()[0]
			emailStr = v1.ToString()
		}
	}

	if emailStr == "" {
		return value.EmptyStringValue, false
	}
	if em, err := mail.ParseAddress(emailStr); err == nil {
		return value.NewStringValue(strings.ToLower(em.Address)), true
	}
	return value.EmptyStringValue, false
}

// emailname a string, parses email
//
//     emailname("Bob <bob@bob.com>") =>  Bob
//
type EmailName struct{}

// Type string
func (m *EmailName) Type() value.ValueType { return value.StringType }
func (m *EmailName) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for EmailName(fieldname) but got %s", n)
	}
	return emailNameEval, nil
}
func emailNameEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val, ok := value.ValueToString(args[0])
	if !ok || val == "" {
		return value.EmptyStringValue, false
	}
	if em, err := mail.ParseAddress(val); err == nil {
		return value.NewStringValue(em.Name), true
	}
	return value.EmptyStringValue, false
}

// emaildomain parses email and returns domain
//
//     emaildomain("Bob <bob@bob.com>") =>  bob.com
//
type EmailDomain struct{}

// Type string
func (m *EmailDomain) Type() value.ValueType { return value.StringType }
func (m *EmailDomain) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for EmailDomain(fieldname) but got %s", n)
	}
	return emailDomainEval, nil
}
func emailDomainEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val, ok := value.ValueToString(args[0])
	if !ok || val == "" {
		return value.EmptyStringValue, false
	}

	if em, err := mail.ParseAddress(strings.ToLower(val)); err == nil {
		parts := strings.SplitN(strings.ToLower(em.Address), "@", 2)
		if len(parts) == 2 {
			return value.NewStringValue(parts[1]), true
		}
	}

	return value.EmptyStringValue, false
}

// Domains Extract Domains from a Value, or Values (must be urlish), doesn't do much/any validation
//
//     domains("http://www.lytics.io/index.html") =>  []string{"lytics.io"}
//
type Domains struct{}

// Type strings
func (m *Domains) Type() value.ValueType { return value.StringsType }
func (m *Domains) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("Expected 1 or more args for Domains(arg, ...) but got %s", n)
	}
	return domainsEval, nil
}
func domainsEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	svals := value.NewStringsValue(make([]string, 0))
	for _, val := range args {
		switch v := val.(type) {
		case value.StringValue:
			svals.Append(v.Val())
		case value.StringsValue:
			for _, sv := range v.Val() {
				svals.Append(sv)
			}
		}
	}

	// Since its empty, we will just re-use it
	if svals.Len() == 0 {
		return nil, false
	}

	// Now convert to domains
	domains := value.NewStringsValue(make([]string, 0))
	for _, val := range svals.Val() {
		urlstr := strings.ToLower(val)
		if len(urlstr) < 4 {
			continue
		}

		// May not have an http prefix, if not assume it
		if !strings.HasPrefix(urlstr, "http") {
			urlstr = "http://" + urlstr
		}
		if urlParsed, err := url.Parse(urlstr); err == nil {
			parts := strings.Split(urlParsed.Host, ".")
			if len(parts) > 2 {
				parts = parts[len(parts)-2:]
			}
			if len(parts) > 0 {
				domains.Append(strings.Join(parts, "."))
			}
		}
	}
	if domains.Len() == 0 {
		return nil, false
	}
	return domains, true
}

// Extract Domain from a Value, or Values (must be urlish), doesn't do much/any validation.
// if input is a list of strings, only first is evaluated, for plural see domains()
//
//     domain("http://www.lytics.io/index.html") =>  "lytics.io"
//
type Domain struct{}

// Type string
func (m *Domain) Type() value.ValueType { return value.StringType }
func (m *Domain) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for Domain(field) but got %s", n)
	}
	return domainEval, nil
}
func domainEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	urlstr := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		urlstr = itemT.Val()
	case value.StringsValue:
		for _, sv := range itemT.Val() {
			urlstr = sv
			break
		}
	}

	urlstr = strings.ToLower(urlstr)
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil && len(urlParsed.Host) > 2 {
		parts := strings.Split(urlParsed.Host, ".")
		if len(parts) > 2 {
			parts = parts[len(parts)-2:]
		}
		if len(parts) > 0 {
			return value.NewStringValue(strings.Join(parts, ".")), true
		}

	}
	return value.EmptyStringValue, false
}

// Extract host from a String (must be urlish), doesn't do much/any validation
// In the event the value contains more than one input url, will ONLY evaluate first
//
//     host("http://www.lytics.io/index.html") =>  www.lytics.io
//
type Host struct{}

// Type string
func (m *Host) Type() value.ValueType { return value.StringType }
func (m *Host) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for Host(field) but got %s", n)
	}
	return HostEval, nil
}
func HostEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.Slice:
		if itemT.Len() == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.SliceValue()[0].ToString()
	}

	if val == "" {
		return value.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) < 8 {
		return value.EmptyStringValue, false
	}
	if !strings.HasPrefix(urlstr, "http") {
		urlstr = "http://" + urlstr
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		//u.Infof("url.parse: %#v", urlParsed)
		return value.NewStringValue(urlParsed.Host), true
	}

	return value.EmptyStringValue, false
}

// Extract hosts from a Strings (must be urlish), doesn't do much/any validation
//
//     hosts("http://www.lytics.io", "http://www.activate.lytics.io") => www.lytics.io, www.activate.lytics.io
//
type Hosts struct{}

// Type strings
func (m *Hosts) Type() value.ValueType { return value.StringsType }
func (m *Hosts) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("Expected 1 or more args for Hosts() but got %s", n)
	}
	return HostsEval, nil
}

func HostsEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	vals := value.NewStringsValue(make([]string, 0))
	for _, item := range args {
		switch itemT := item.(type) {
		case value.StringValue:
			val := itemT.Val()
			if len(val) > 0 {
				vals.Append(itemT.Val())
			}
		case value.Slice:
			for _, sv := range itemT.SliceValue() {
				val := sv.ToString()
				if len(val) > 0 {
					vals.Append(val)
				}
			}
		}
	}

	if vals.Len() == 0 {
		return vals, false
	}

	hosts := value.NewStringsValue(make([]string, 0))
	for _, val := range vals.Val() {
		urlstr := strings.ToLower(val)
		if len(urlstr) < 8 {
			continue
		}
		if !strings.HasPrefix(urlstr, "http") {
			urlstr = "http://" + urlstr
		}
		if urlParsed, err := url.Parse(urlstr); err == nil {
			//u.Infof("url.parse: %#v", urlParsed)
			hosts.Append(urlParsed.Host)
		}
	}
	if hosts.Len() == 0 {
		return nil, false
	}
	return hosts, true
}

// url decode a string
//
//     urldecode("http://www.lytics.io/index.html") =>  http://www.lytics.io
//
// In the event the value contains more than one input url, will ONLY evaluate first
//
type UrlDecode struct{}

// Type string
func (m *UrlDecode) Type() value.ValueType { return value.StringType }
func (m *UrlDecode) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for UrlDecode(field) but got %s", n)
	}
	return urlDecodeEval, nil
}
func urlDecodeEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.Slice:
		if itemT.Len() == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.SliceValue()[0].ToString()
	}

	if val == "" {
		return value.EmptyStringValue, false
	}
	val, err := url.QueryUnescape(val)
	return value.NewStringValue(val), err == nil
}

// UrlPath Extract url path from a String (must be urlish), doesn't do much/any validation
//
//     path("http://www.lytics.io/blog/index.html") =>  blog/index.html
//
// In the event the value contains more than one input url, will ONLY evaluate first
//
type UrlPath struct{}

// Type string
func (m *UrlPath) Type() value.ValueType { return value.StringType }
func (m *UrlPath) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for UrlPath() but got %s", n)
	}
	return urlPathEval, nil
}
func urlPathEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.Slice:
		if itemT.Len() == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.SliceValue()[0].ToString()
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)
	if len(urlstr) > 6 && !strings.Contains(urlstr[:5], "/") {
		if !strings.HasPrefix(urlstr, "http") {
			urlstr = "http://" + urlstr
		}
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		return value.NewStringValue(urlParsed.Path), true
	}

	return value.EmptyStringValue, false
}

// Qs Extract qs param from a string (must be url valid)
//
//     qs("http://www.lytics.io/?utm_source=google","utm_source")  => "google", true
//
type Qs struct{}

// Type string
func (m *Qs) Type() value.ValueType { return value.StringType }
func (m *Qs) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for Qs(url, param) but got %s", n)
	}
	return qsEval, nil
}
func qsEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	urlstr := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		urlstr = itemT.Val()
	case value.Slice:
		if itemT.Len() == 0 {
			return value.EmptyStringValue, false
		}
		urlstr = itemT.SliceValue()[0].ToString()
	}
	if urlstr == "" {
		return value.EmptyStringValue, false
	}

	qsParam, ok := value.ValueToString(args[1])
	if !ok || qsParam == "" {
		return value.EmptyStringValue, false
	}
	if len(urlstr) > 6 && !strings.Contains(urlstr[:5], "/") {
		if !strings.HasPrefix(urlstr, "http") {
			urlstr = "http://" + urlstr
		}
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		qsval := urlParsed.Query().Get(qsParam)
		if len(qsval) > 0 {
			return value.NewStringValue(qsval), true
		}
	}

	return value.EmptyStringValue, false
}

// Qs Extract qs param from a string (must be url valid)
//
//     qs("http://www.lytics.io/?utm_source=google","utm_source")  => "google", true
//
type QsDeprecate struct{}

// Type string
func (m *QsDeprecate) Type() value.ValueType { return value.StringType }
func (m *QsDeprecate) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for Qs(url, param) but got %s", n)
	}
	return qsDeprecateEval, nil
}
func qsDeprecateEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.Slice:
		if itemT.Len() == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.SliceValue()[0].ToString()
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	urlstr := strings.ToLower(val)

	keyVal, ok := value.ValueToString(args[1])
	if !ok || keyVal == "" {
		return value.EmptyStringValue, false
	}
	if len(urlstr) > 6 && !strings.Contains(urlstr[:5], "/") {
		if !strings.HasPrefix(urlstr, "http") {
			urlstr = "http://" + urlstr
		}
	}
	if urlParsed, err := url.Parse(urlstr); err == nil {
		qsval := urlParsed.Query().Get(keyVal)
		if len(qsval) > 0 {
			return value.NewStringValue(qsval), true
		}
	}

	return value.EmptyStringValue, false
}

// UrlMain remove the querystring and scheme from url
//
//     urlmain("http://www.lytics.io/?utm_source=google")  => "www.lytics.io/", true
//
type UrlMain struct{}

// Type string
func (m *UrlMain) Type() value.ValueType { return value.StringType }
func (m *UrlMain) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 arg for UrlMain() but got %s", n)
	}
	return urlMainEval, nil
}

func urlMainEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.Slice:
		if itemT.Len() == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.SliceValue()[0].ToString()
	}
	if val == "" {
		return value.EmptyStringValue, false
	}
	if up, err := url.Parse(val); err == nil {
		return value.NewStringValue(fmt.Sprintf("%s%s", up.Host, up.Path)), true
	}

	return value.EmptyStringValue, false
}

// UrlMinusQs removes a specific query parameter and its value from a url
//
//     urlminusqs("http://www.lytics.io/?q1=google&q2=123", "q1") => "http://www.lytics.io/?q2=123", true
//
type UrlMinusQs struct{}

// Type string
func (m *UrlMinusQs) Type() value.ValueType { return value.StringType }
func (m *UrlMinusQs) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for UrlMinusQs(url, qsparam) but got %s", n)
	}
	return urlMinusQsEval, nil
}

func urlMinusQsEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	urlstr := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		urlstr = itemT.Val()
	case value.Slice:
		if itemT.Len() == 0 {
			return value.EmptyStringValue, false
		}
		urlstr = itemT.SliceValue()[0].ToString()
	}
	if urlstr == "" {
		return value.EmptyStringValue, false
	}

	if len(urlstr) > 6 && !strings.Contains(urlstr[:5], "/") {
		if !strings.HasPrefix(urlstr, "http") {
			urlstr = "http://" + urlstr
		}
	}
	keyVal, ok := value.ValueToString(args[1])
	if !ok || keyVal == "" {
		return value.EmptyStringValue, false
	}

	if up, err := url.Parse(urlstr); err == nil {
		qsval := up.Query()
		_, ok := qsval[keyVal]
		if !ok {
			return value.NewStringValue(fmt.Sprintf("%s://%s%s?%s", up.Scheme, up.Host, up.Path, up.RawQuery)), true
		}
		qsval.Del(keyVal)
		up.RawQuery = qsval.Encode()
		if up.RawQuery == "" {
			return value.NewStringValue(fmt.Sprintf("%s://%s%s", up.Scheme, up.Host, up.Path)), true
		}
		return value.NewStringValue(fmt.Sprintf("%s://%s%s?%s", up.Scheme, up.Host, up.Path, up.RawQuery)), true
	}

	return value.EmptyStringValue, false
}

// UrlWithQueryFunc strips a url and retains only url parameters that match
// the supplied regular expressions.
//
//     url.matchqs(url, re1, re2, ...)  => url_withoutqs
//
type UrlWithQuery struct{}

// Type string
func (m *UrlWithQuery) Type() value.ValueType { return value.StringType }
func (*UrlWithQuery) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) == 0 {
		return nil, fmt.Errorf("Expected at least 1 args for urlwithqs(url, param, param2) but got %s", n)
	}

	if len(n.Args) == 1 {
		return UrlWithQueryEval(nil), nil
	}

	// Memoize these compiled reg-expressions
	include := make([]*regexp.Regexp, 0)
	for _, n := range n.Args[1:] {
		keyItem, ok := vm.Eval(nil, n)
		if !ok {
			return nil, fmt.Errorf("Could not evaluate %v", n)
		}
		keyVal, ok := value.ValueToString(keyItem)
		if !ok || keyVal == "" {
			return nil, fmt.Errorf("Could not convert %q to regex", n.String())
		}

		keyRegexp, err := regexp.Compile(keyVal)
		if err != nil {
			return nil, err
		}
		include = append(include, keyRegexp)
	}
	return UrlWithQueryEval(include), nil
}

// UrlWithQueryEval pass reg-expressions to match qs args.
// Must match one regexp or else the qs param is dropped.
func UrlWithQueryEval(include []*regexp.Regexp) expr.EvaluatorFunc {
	return func(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

		val := ""
		switch itemT := args[0].(type) {
		case value.StringValue:
			val = itemT.Val()
		case value.Slice:
			if itemT.Len() == 0 {
				return value.EmptyStringValue, false
			}
			val = itemT.SliceValue()[0].ToString()
		}
		if val == "" {
			return value.EmptyStringValue, false
		}

		up, err := url.Parse(val)
		if err != nil {
			return value.EmptyStringValue, false
		}

		if len(args) == 1 {
			return value.NewStringValue(fmt.Sprintf("%s%s", up.Host, up.Path)), true
		}

		oldvals := up.Query()
		newvals := make(url.Values)
		for k, v := range oldvals {
			// include fields specified as arguments
			for _, pattern := range include {
				if pattern.MatchString(k) {
					newvals[k] = v
					break
				}
			}
		}

		up.RawQuery = newvals.Encode()
		if up.RawQuery == "" {
			return value.NewStringValue(fmt.Sprintf("%s%s", up.Host, up.Path)), true
		}

		return value.NewStringValue(fmt.Sprintf("%s%s?%s", up.Host, up.Path, up.RawQuery)), true
	}
}

// UserAgent Extract user agent features
//
//     useragent(user_agent_field,"mobile")  => "true", true
//
type UserAgent struct{}

// Type string
func (m *UserAgent) Type() value.ValueType { return value.StringType }
func (m *UserAgent) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for UserAgent(user_agent_field, feature) but got %s", n)
	}
	return userAgentEval, nil
}

func userAgentEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.Slice:
		if itemT.Len() == 0 {
			return value.EmptyStringValue, false
		}
		val = itemT.SliceValue()[0].ToString()
	}
	if val == "" {
		return value.EmptyStringValue, false
	}

	/*
	   fmt.Printf("%v\n", ua.Mobile())   // => false
	   fmt.Printf("%v\n", ua.Bot())      // => false
	   fmt.Printf("%v\n", ua.Mozilla())  // => "5.0"

	   fmt.Printf("%v\n", ua.Platform()) // => "X11"
	   fmt.Printf("%v\n", ua.OS())       // => "Linux x86_64"

	   name, version := ua.Engine()
	   fmt.Printf("%v\n", name)          // => "AppleWebKit"
	   fmt.Printf("%v\n", version)       // => "537.11"

	   name, version = ua.Browser()
	   fmt.Printf("%v\n", name)          // => "Chrome"
	   fmt.Printf("%v\n", version)       // => "23.0.1271.97"

	   // Let's see an example with a bot.

	   ua.Parse("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)")

	   fmt.Printf("%v\n", ua.Bot())      // => true

	   name, version = ua.Browser()
	   fmt.Printf("%v\n", name)          // => Googlebot
	   fmt.Printf("%v\n", version)       // => 2.1
	*/

	method, ok := value.ValueToString(args[1])
	if !ok || method == "" {
		return value.EmptyStringValue, false
	}

	ua := user_agent.New(val)

	switch strings.ToLower(method) {
	case "bot":
		return value.NewStringValue(fmt.Sprintf("%v", ua.Bot())), true
	case "mobile":
		return value.NewStringValue(fmt.Sprintf("%v", ua.Mobile())), true
	case "mozilla":
		return value.NewStringValue(ua.Mozilla()), true
	case "platform":
		return value.NewStringValue(ua.Platform()), true
	case "os":
		return value.NewStringValue(ua.OS()), true
	case "engine":
		name, _ := ua.Engine()
		return value.NewStringValue(name), true
	case "engine_version":
		_, version := ua.Engine()
		return value.NewStringValue(version), true
	case "browser":
		name, _ := ua.Browser()
		return value.NewStringValue(name), true
	case "browser_version":
		_, version := ua.Browser()
		return value.NewStringValue(version), true
	}
	return value.EmptyStringValue, false
}

// UserAgentMap Extract user agent features
//
//     useragent.map(user_agent_field)  => {"mobile": "false","platform":"X11"}, true
//
type UserAgentMap struct{}

// Type MapString
func (m *UserAgentMap) Type() value.ValueType { return value.MapStringType }

func (m *UserAgentMap) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for useragentmap(user_agent) but got %s", n)
	}
	return userAgentMapEval, nil
}

func userAgentMapEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.Slice:
		if itemT.Len() == 0 {
			return nil, false
		}
		val = itemT.SliceValue()[0].ToString()
	}
	if val == "" {
		return nil, false
	}

	/*
	   fmt.Printf("%v\n", ua.Mobile())   // => false
	   fmt.Printf("%v\n", ua.Bot())      // => false
	   fmt.Printf("%v\n", ua.Mozilla())  // => "5.0"

	   fmt.Printf("%v\n", ua.Platform()) // => "X11"
	   fmt.Printf("%v\n", ua.OS())       // => "Linux x86_64"

	   name, version := ua.Engine()
	   fmt.Printf("%v\n", name)          // => "AppleWebKit"
	   fmt.Printf("%v\n", version)       // => "537.11"

	   name, version = ua.Browser()
	   fmt.Printf("%v\n", name)          // => "Chrome"
	   fmt.Printf("%v\n", version)       // => "23.0.1271.97"

	   // Let's see an example with a bot.

	   ua.Parse("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)")

	   fmt.Printf("%v\n", ua.Bot())      // => true

	   name, version = ua.Browser()
	   fmt.Printf("%v\n", name)          // => Googlebot
	   fmt.Printf("%v\n", version)       // => 2.1
	*/

	ua := user_agent.New(val)
	out := make(map[string]string)
	out["bot"] = fmt.Sprintf("%v", ua.Bot())
	out["mobile"] = fmt.Sprintf("%v", ua.Mobile())
	out["mozilla"] = ua.Mozilla()
	out["platform"] = ua.Platform()
	out["os"] = ua.OS()
	name, version := ua.Engine()
	out["engine"] = name
	out["engine_version"] = version
	name, version = ua.Browser()
	out["browser"] = name
	out["browser_version"] = version
	return value.NewMapStringValue(out), true
}
