package vm

import (
	"testing"
)

// basic select, aggregate test
//    select count(val_field) from xyz   test
func TestExecCount(t *testing.T) {
	// db := NewDB("2000-01-01T12:00:00Z")
	// db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:00Z", map[string]interface{}{"value": float64(100)})
	// db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:10Z", map[string]interface{}{"value": float64(90)})
	// db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:20Z", map[string]interface{}{"value": float64(80)})
	// db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:30Z", map[string]interface{}{"value": float64(70)})
	// db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:40Z", map[string]interface{}{"value": float64(60)})
	// db.WriteSeries("cpu", map[string]string{}, "2000-01-01T00:00:50Z", map[string]interface{}{"value": float64(50)})

	// // Expected resultset.
	// exp := minify(`[{"name":"cpu","columns":["time","count"],"values":[[0,6]]}]`)

	// // Execute and compare.
	// rs := db.MustPlanAndExecute(`SELECT count(value) FROM cpu`)
	// if act := minify(jsonify(rs)); exp != act {
	// 	t.Fatalf("unexpected resultset: %s", act)
	// }
}
