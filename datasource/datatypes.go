package datasource

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
)

// A collection of data-types that implment database/sql Scan() interface
// for converting byte fields to richer go data types

type (

	// Convert string/bytes to time.Time
	//  auto-parses a variety of different date formats
	//  that are supported in http://godoc.org/github.com/araddon/dateparse
	TimeValue time.Time

	// Convert json to array of strings
	StringArray []string

	// convert json bytes
	JsonWrapper json.RawMessage

	// json
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
	u.Debugf("Value: %v", m)
	by, err := json.Marshal(time.Time(m))
	return by, err
}

func (m TimeValue) Time() time.Time {
	return time.Time(m)
}

func (m *TimeValue) Scan(src interface{}) error {
	//u.Debugf("scan: '%v'", src)
	var t time.Time
	switch val := src.(type) {
	case string:
		//u.Infof("trying to scan string: '%v'", val)
		t2, err := dateparse.ParseAny(val)
		if err == nil {
			*m = TimeValue(t2)
			return nil
		}
		//u.Infof("%v  %v", t2, err)
		err = json.Unmarshal([]byte(val), &t)
		if err == nil {
			*m = TimeValue(t)
		} else {
			u.Warnf("error for %q  err=%v", val, err)
			return err
		}
	case []byte:
		t2, err := dateparse.ParseAny(string(val))
		if err == nil {
			*m = TimeValue(t2)
			return nil
		}
		err = json.Unmarshal(val, &t)
		if err == nil {
			*m = TimeValue(t)
		} else {
			return err
		}
	case nil:
		return nil
	default:
		u.Warnf("unknown type: %T", m)
		return errors.New("Incompatible type for TimeValue")
	}
	return nil
}

func (m *TimeValue) Unmarshal(v interface{}) error {
	u.Warnf("wat? %T %v", v, v)
	//return json.Unmarshal([]byte(*m), v)
	return fmt.Errorf("not implemented")
}

func (m *JsonWrapper) MarshalJSON() ([]byte, error) { return *m, nil }

// Unmarshall bytes into this typed struct
func (m *JsonWrapper) UnmarshalJSON(data []byte) error {
	if m == nil {
		return errors.New("JsonWrapper must not be nil")
	}
	*m = append((*m)[0:0], data...)
	return nil

}

// This is the go sql/driver interface we need to implement to allow
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

// Unmarshall bytes into this typed struct
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

// This is the go sql/driver interface we need to implement to allow
// conversion back forth
func (m JsonHelperScannable) Value() (driver.Value, error) {
	jsonBytes, err := json.Marshal(u.JsonHelper(m))
	if err != nil {
		return []byte{}, err
	}
	return jsonBytes, nil
}

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

// func (m *JsonHelperScannable) Unmarshal(v interface{}) error {
// 	return json.Unmarshal([]byte(*m), v)
// }

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

func (m StringArray) Value() (driver.Value, error) {
	by, err := json.Marshal(m)
	return by, err
}

func (m *StringArray) Scan(src interface{}) error {
	var srcBytes []byte
	switch val := src.(type) {
	case string:
		srcBytes = []byte(val)
	case []byte:
		srcBytes = val
	default:
		u.Warnf("unknown type: %T", src)
		return errors.New("Incompatible type for StringArray")
	}
	sa := make([]string, 0)
	err := json.Unmarshal(srcBytes, &sa)
	if err != nil {
		u.Warnf("error? %v", err)
		return err
	}
	*m = StringArray(sa)
	return nil
}
