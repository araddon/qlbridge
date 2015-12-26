package datasource

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	//"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
)

// Our test struct, try as many different field types as possible
type User struct {
	Name          string
	Created       time.Time
	Updated       *time.Time
	Authenticated bool
	HasSession    *bool
	Roles         []string
	BankAmount    float64
	Address       struct {
		City string
		Zip  int
	}
	Data    json.RawMessage
	Context u.JsonHelper
}

func (m *User) FullName() string {
	return m.Name + ", Jedi"
}

func TestStructWrapper(t *testing.T) {

	t1, _ := dateparse.ParseAny("12/18/2015")
	tr := true
	user := &User{
		Name:          "Yoda",
		Created:       t1,
		Updated:       &t1,
		Authenticated: true,
		HasSession:    &tr,
		Roles:         []string{"admin", "api"},
		BankAmount:    55.5,
	}

	readers := []expr.ContextReader{
		NewContextWrapper(user),
		NewContextSimpleNative(map[string]interface{}{
			"str1": "str1",
			"int1": 1,
			"t1":   t1,
			"Name": "notyoda",
		}),
	}

	nc := NewNestedContextReader(readers, time.Now())
	expected := value.NewMapValue(map[string]interface{}{
		"str1":          "str1",
		"int1":          1,
		"Name":          "Yoda",
		"Authenticated": true,
		"bankamount":    55.5,
		"FullName":      "Yoda, Jedi",
		"Roles":         []string{"admin", "api"},
	})

	for k, v := range expected.Val() {
		//u.Infof("k:%v v:%#v", k, v)
		checkval(t, nc, k, v)
	}
}
