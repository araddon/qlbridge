package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/araddon/qlbridge/builtins"
	"github.com/araddon/qlbridge/vm"
)

/*

go build && ./bm --cpuprofile=cpu.prof
go tool pprof bm cpu.prof


*/
var (
	cpuProfileFile string
	memProfileFile string
	logging        = "info"

	msg = vm.NewContextSimpleTs(
		map[string]vm.Value{
			"int5":       vm.NewIntValue(5),
			"item_count": vm.NewStringValue("5"),
			"reg_date":   vm.NewStringValue("2014/11/01"),
			"user_id":    vm.NewStringValue("abc")},
		time.Now(),
	)
)

func init() {

	flag.StringVar(&logging, "logging", "info", "logging [ debug,info ]")
	flag.StringVar(&cpuProfileFile, "cpuprofile", "", "cpuprofile")
	flag.StringVar(&memProfileFile, "memprofile", "", "memProfileFile")
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

		for i := 0; i < 10000; i++ {
			runSql(`select user_id, item_count * 2 as itemsx2, yy(reg_date) > 10 as regyy FROM stdio`, msg)
		}
	}

}

func runSql(sql string, readContext vm.ContextReader) vm.ContextSimple {

	sqlVm, err := vm.NewSqlVm(sql)
	if err != nil {
		panic(err.Error())
	}

	writeContext := vm.NewContextSimple()
	err = sqlVm.Execute(writeContext, readContext)
	if err != nil {
		panic(err.Error())
	}

	return writeContext
}
