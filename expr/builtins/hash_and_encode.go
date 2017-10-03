package builtins

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/dchest/siphash"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

// hash.sip() hash a value to a 64 bit int
//
//     hash.sip("/blog/index.html")  =>  1234
//
type HashSip struct{}

// Type int
func (m *HashSip) Type() value.ValueType { return value.IntType }
func (m *HashSip) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for hash.sip(field_to_hash) but got %s", n)
	}
	return hashSipEval, nil
}

func hashSipEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {

	val := ""
	switch itemT := args[0].(type) {
	case value.StringValue:
		val = itemT.Val()
	case value.Slice:
		if itemT.Len() == 0 {
			return value.NewIntValue(0), false
		}
		val = itemT.SliceValue()[0].ToString()
	}
	if val == "" {
		return value.NewIntValue(0), false
	}

	hash := siphash.Hash(0, 1, []byte(val))

	return value.NewIntValue(int64(hash)), true
}

// HashMd5Func Hash a value to MD5 string
//
//     hash.md5("/blog/index.html")  =>  abc345xyz
//
type HashMd5 struct{}

// Type string
func (m *HashMd5) Type() value.ValueType { return value.StringType }
func (m *HashMd5) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for hash.md5(field_to_hash) but got %s", n)
	}
	return hashMd5Eval, nil
}
func hashMd5Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, false
	}
	hasher := md5.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}

// HashSha1Func Hash a value to SHA256 string
//
//     hash.sha1("/blog/index.html")  =>  abc345xyz
//
type HashSha1 struct{}

// Type string
func (m *HashSha1) Type() value.ValueType { return value.StringType }
func (m *HashSha1) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for HashSha1(field_to_hash) but got %s", n)
	}
	return hashSha1Eval, nil
}
func hashSha1Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, false
	}
	hasher := sha1.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}

// HashSha256Func Hash a value to SHA256 string
//
//     hash.sha256("/blog/index.html")  =>  abc345xyz
//
type HashSha256 struct{}

// Type string
func (m *HashSha256) Type() value.ValueType { return value.StringType }
func (m *HashSha256) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for HashSha256(field_to_hash) but got %s", n)
	}
	return hashSha256Eval, nil
}
func hashSha256Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, false
	}
	hasher := sha256.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}

// HashSha512Func Hash a value to SHA512 string
//
//     hash.sha512("/blog/index.html")  =>  abc345xyz
//
type HashSha512 struct{}

// Type string
func (m *HashSha512) Type() value.ValueType { return value.StringType }
func (m *HashSha512) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for HashSha512(field_to_hash) but got %s", n)
	}
	return hashSha512Eval, nil
}
func hashSha512Eval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, false
	}
	hasher := sha512.New()
	hasher.Write([]byte(args[0].ToString()))
	return value.NewStringValue(hex.EncodeToString(hasher.Sum(nil))), true
}

// Base 64 encoding function
//
//     encoding.b64encode("hello world=")  =>  aGVsbG8gd29ybGQ=
//
type EncodeB64Encode struct{}

// Type string
func (m *EncodeB64Encode) Type() value.ValueType { return value.StringType }
func (m *EncodeB64Encode) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for encoding.b64encode(field) but got %s", n)
	}
	return encodeB64EncodeEval, nil
}
func encodeB64EncodeEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, false
	}
	encodedString := base64.StdEncoding.EncodeToString([]byte(args[0].ToString()))
	return value.NewStringValue(encodedString), true
}

// Base 64 encoding function
//
//     encoding.b64decode("aGVsbG8gd29ybGQ=")  =>  "hello world"
//
type EncodeB64Decode struct{}

// Type string
func (m *EncodeB64Decode) Type() value.ValueType { return value.StringType }
func (m *EncodeB64Decode) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 1 {
		return nil, fmt.Errorf("Expected 1 args for encoding.b64decode(field) but got %s", n)
	}
	return encodeB64DecodeEval, nil
}
func encodeB64DecodeEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	if args[0] == nil || args[0].Err() || args[0].Nil() {
		return value.EmptyStringValue, false
	}

	by, err := base64.StdEncoding.DecodeString(args[0].ToString())
	if err != nil {
		return value.EmptyStringValue, false
	}
	return value.NewStringValue(string(by)), true
}
