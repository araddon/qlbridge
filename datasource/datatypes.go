package datasource

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
)

var (
	// ErrNotDate an error for trying to corece/convert to Time a field that is not a time.
	ErrNotDate = errors.New("Unable to conver to time value")
)

// These are data-types that implement the database/sq interface for Scan() for custom types

type (
	// TimeValue Convert a string/bytes to time.Time by parsing the string
	// with a wide variety of different date formats that are supported
	// in http://godoc.org/github.com/araddon/dateparse
	TimeValue time.Time

	// StringArray Convert json to array of strings
	StringArray []string

	// JsonWrapper json data
	JsonWrapper json.RawMessage

	// JsonHelperScannable expects map json's (not array) map[string]interface
	JsonHelperScannable u.JsonHelper
)

func (m *TimeValue) MarshalJSON() ([]byte, error) {
	by, err := time.Time(*m).MarshalJSON()
	return by, err
}

func (m *TimeValue) UnmarshalJSON(data []byte) error {
	var t time.Time
	err := json.Unmarshal(data, &t)
	if err == nil {
		*m = TimeValue(t)
	}
	return err
}

func (m TimeValue) Value() (driver.Value, error) {
	by, err := json.Marshal(time.Time(m))
	return by, err
}

func (m *TimeValue) Time() time.Time {
	if m == nil {
		return time.Time{}
	}
	return time.Time(*m)
}

func (m *TimeValue) Scan(src interface{}) error {

	var t time.Time
	var dstr string
	switch val := src.(type) {
	case string:
		dstr = val
	case []byte:
		dstr = string(val)
	default:
		return ErrNotDate
	}
	if dstr == "" {
		*m = TimeValue(time.Time{})
		return nil
	}
	t2, err := dateparse.ParseAny(dstr)
	if err == nil {
		*m = TimeValue(t2)
		return nil
	}
	err = json.Unmarshal([]byte(dstr), &t)
	if err == nil {
		*m = TimeValue(t)
		return nil
	}
	return err
}

func (m *JsonWrapper) MarshalJSON() ([]byte, error) { return *m, nil }

// UnmarshalJSON bytes into this typed struct
func (m *JsonWrapper) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("JsonWrapper must not be nil")
	}
	*m = append((*m)[0:0], data...)
	return nil

}

// Value This is the go sql/driver interface we need to implement to allow
// conversion back forth
func (m JsonWrapper) Value() (driver.Value, error) {
	var jsonRaw json.RawMessage
	err := m.Unmarshal(&jsonRaw)
	if err != nil {
		return []byte{}, err
	}
	return []byte(m), nil
}

func (m *JsonWrapper) Scan(src interface{}) error {
	var jsonBytes []byte
	switch src.(type) {
	case string:
		jsonBytes = []byte(src.(string))
	case []byte:
		jsonBytes = src.([]byte)
	default:
		return errors.New("Incompatible type for JsonWrapper")
	}
	*m = JsonWrapper(append((*m)[0:0], jsonBytes...))
	return nil
}

func (m *JsonWrapper) Unmarshal(v interface{}) error {
	return json.Unmarshal([]byte(*m), v)
}

func (m *JsonHelperScannable) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.JsonHelper(*m))
}

// UnmarshalJSON bytes into this typed struct
func (m *JsonHelperScannable) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("JsonHelperScannable must not be nil")
	}
	jh := make(u.JsonHelper)
	if err := json.Unmarshal(data, &jh); err != nil {
		return err
	}
	*m = JsonHelperScannable(jh)
	return nil
}

// Value This is the go sql/driver interface we need to implement to allow
// conversion back forth
func (m JsonHelperScannable) Value() (driver.Value, error) {
	jsonBytes, err := json.Marshal(u.JsonHelper(m))
	if err != nil {
		return []byte{}, err
	}
	return jsonBytes, nil
}

// Scan the database/sql interface for scanning sql byte vals into this
// typed structure.
func (m *JsonHelperScannable) Scan(src interface{}) error {
	var jsonBytes []byte
	switch tv := src.(type) {
	case string:
		jsonBytes = []byte(tv)
	case []byte:
		jsonBytes = tv
	case nil:
		return nil
	default:
		return fmt.Errorf("Incompatible type:%T for JsonHelperScannable", src)
	}
	jh := make(u.JsonHelper)
	if err := json.Unmarshal(jsonBytes, &jh); err != nil {
		return err
	}
	*m = JsonHelperScannable(jh)
	return nil
}

func (m *StringArray) MarshalJSON() ([]byte, error) {
	by, err := json.Marshal(*m)
	return by, err
}

func (m *StringArray) UnmarshalJSON(data []byte) error {
	var l []string
	err := json.Unmarshal(data, &l)
	if err != nil {
		return err
	}
	*m = StringArray(l)
	return nil
}

// Value convert string to json values
func (m StringArray) Value() (driver.Value, error) {
	by, err := json.Marshal(m)
	return by, err
}

// Scan the database/sql interface for scanning sql byte vals into this
// typed structure.
func (m *StringArray) Scan(src interface{}) error {

	var srcBytes []byte
	switch val := src.(type) {
	case string:
		srcBytes = []byte(val)
	case []byte:
		srcBytes = val
	default:
		return fmt.Errorf("Incompatible type for StringArray got %T", src)
	}
	sa := make([]string, 0)
	if u.IsJsonArray(srcBytes) {
		err := json.Unmarshal(srcBytes, &sa)
		if err != nil {
			return err
		}
	} else if parts := strings.Split(string(srcBytes), ","); len(parts) > 0 {
		sa = parts
	}

	*m = StringArray(sa)
	return nil
}
