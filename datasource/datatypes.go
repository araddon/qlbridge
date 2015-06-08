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

type TimeValue time.Time

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
		u.Infof("%v  %v", t2, err)
		err = json.Unmarshal([]byte(val), &t)
		if err == nil {
			*m = TimeValue(t)
		} else {
			u.Warnf("error? %v", err)
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

type JsonWrapper json.RawMessage

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
	var err = m.Unmarshal(&jsonRaw)
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
		return errors.New("Incompatible type for JsonText")
	}
	*m = JsonWrapper(append((*m)[0:0], jsonBytes...))
	return nil
}

func (m *JsonWrapper) Unmarshal(v interface{}) error {
	return json.Unmarshal([]byte(*m), v)
}
