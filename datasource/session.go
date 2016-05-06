package datasource

import (
	"database/sql/driver"
	"time"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/value"
)

const (
	// Default Max Allowed packets for connections
	MaxAllowedPacket = 4194304
)

// http://dev.mysql.com/doc/refman/5.6/en/server-system-variables.html
var mysqlGlobalVars *ContextSimple = NewMySqlGlobalVars()

func RowsForSession(ctx *plan.Context) [][]driver.Value {

	ses := ctx.Session.Row()
	rows := make([][]driver.Value, 0, len(ses))
	for k, v := range ses {
		rows = append(rows, []driver.Value{k, v.Value()})
	}
	return rows
}

func NewMySqlSessionVars() expr.ContextReadWriter {
	ctx := NewContextSimple()
	ctx.Data["@@max_allowed_packet"] = value.NewIntValue(MaxAllowedPacket)
	ctx.Data["@@session.auto_increment_increment"] = value.NewIntValue(1)
	ctx.Data["@@session.tx_isolation"] = value.NewStringValue("REPEATABLE-READ")
	rdr := NewNestedContextReadWriter([]expr.ContextReader{
		ctx,
		mysqlGlobalVars,
	}, ctx, time.Now())
	return rdr
}

func NewMySqlGlobalVars() *ContextSimple {
	ctx := NewContextSimple()

	ctx.Data["@@session.auto_increment_increment"] = value.NewIntValue(1)
	ctx.Data["auto_increment_increment"] = value.NewIntValue(1)
	ctx.Data["@@session.tx_read_only"] = value.NewIntValue(1)
	//ctx.Data["@@session.auto_increment_increment"] = value.NewBoolValue(true)
	ctx.Data["@@character_set_client"] = value.NewStringValue("utf8")
	ctx.Data["@@character_set_connection"] = value.NewStringValue("utf8")
	ctx.Data["@@character_set_results"] = value.NewStringValue("utf8")
	ctx.Data["@@character_set_server"] = value.NewStringValue("utf8")
	ctx.Data["@@init_connect"] = value.NewStringValue("")
	ctx.Data["@@interactive_timeout"] = value.NewIntValue(28800)
	ctx.Data["@@license"] = value.NewStringValue("MIT")
	ctx.Data["@@lower_case_table_names"] = value.NewIntValue(0)
	ctx.Data["max_allowed_packet"] = value.NewIntValue(MaxAllowedPacket)
	ctx.Data["@@max_allowed_packet"] = value.NewIntValue(MaxAllowedPacket)
	ctx.Data["@@max_allowed_packets"] = value.NewIntValue(MaxAllowedPacket)
	ctx.Data["@@net_buffer_length"] = value.NewIntValue(16384)
	ctx.Data["@@net_write_timeout"] = value.NewIntValue(600)
	ctx.Data["@@query_cache_size"] = value.NewIntValue(1048576)
	ctx.Data["@@query_cache_type"] = value.NewStringValue("OFF")
	ctx.Data["@@sql_mode"] = value.NewStringValue("NO_ENGINE_SUBSTITUTION")
	ctx.Data["@@system_time_zone"] = value.NewStringValue("UTC")
	ctx.Data["@@time_zone"] = value.NewStringValue("SYSTEM")
	ctx.Data["@@tx_isolation"] = value.NewStringValue("REPEATABLE-READ")
	ctx.Data["@@version_comment"] = value.NewStringValue("DataUX (MIT), Release .0.9")
	ctx.Data["@@wait_timeout"] = value.NewIntValue(28800)
	return ctx
}
