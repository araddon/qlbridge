package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/lytics/qlbridge/datasource"
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/expr/builtins"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
	"github.com/lytics/qlbridge/vm"
)

/*

go build && time ./_bm --command=parse --cpuprofile=cpu.prof
go tool pprof _bm cpu.prof


go build && time ./_bm --command=vm --cpuprofile=cpu.prof
go tool pprof _bm cpu.prof


*/
var (
	cpuProfileFile string
	memProfileFile string
	logging        = "info"
	command        = "parse"

	msg = datasource.NewContextSimpleTs(
		map[string]value.Value{
			"int5":       value.NewIntValue(5),
			"item_count": value.NewStringValue("5"),
			"reg_date":   value.NewStringValue("2014/11/01"),
			"user_id":    value.NewStringValue("abc")},
		time.Now(),
	)
)

func init() {

	flag.StringVar(&logging, "logging", "info", "logging [ debug,info ]")
	flag.StringVar(&cpuProfileFile, "cpuprofile", "", "cpuprofile")
	flag.StringVar(&memProfileFile, "memprofile", "", "memProfileFile")
	flag.StringVar(&command, "command", "parse", "command to run [parse,vm]")
	flag.Parse()

	builtins.LoadAllBuiltins()

}

func main() {

	if cpuProfileFile != "" {
		f, err := os.Create(cpuProfileFile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	switch command {
	case "parse":
		runParse(100000, `select user_id, item_count * 2 as itemsx2, yy(reg_date) > 10 as regyy FROM stdio`, msg)

	case "vm":
		runVm(100000, `select user_id, item_count * 2 as itemsx2, yy(reg_date) > 10 as regyy FROM stdio`, msg)
	}
}

func runParse(repeat int, sql string, readContext expr.ContextReader) {
	for i := 0; i < repeat; i++ {
		sel, err := rel.ParseSqlSelect(sql)
		if err != nil {
			panic(err.Error())
		}
		writeContext := datasource.NewContextSimple()
		_, err = vm.EvalSql(sel, writeContext, readContext)
		if err != nil {
			panic(err.Error())
		}
	}
}

func runVm(repeat int, sql string, readContext expr.ContextReader) {
	sel, err := rel.ParseSqlSelect(sql)
	if err != nil {
		panic(err.Error())
	}

	for i := 0; i < repeat; i++ {

		writeContext := datasource.NewContextSimple()
		_, err = vm.EvalSql(sel, writeContext, readContext)
		//log.Println(writeContext.All())
		if err != nil {
			panic(err.Error())
		}
	}
}
