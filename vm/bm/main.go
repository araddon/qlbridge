package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/araddon/qlbridge/builtins"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
)

/*

go build && time ./bm --command=parse --cpuprofile=cpu.prof
go tool pprof bm cpu.prof


go build && time ./bm --command=vm --cpuprofile=cpu.prof
go tool pprof bm cpu.prof


*/
var (
	cpuProfileFile string
	memProfileFile string
	logging        = "info"
	command        = "parse"

	msg = vm.NewContextSimpleTs(
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
		runParse(10000, `select user_id, item_count * 2 as itemsx2, yy(reg_date) > 10 as regyy FROM stdio`, msg)

	case "vm":
		runVm(100000, `select user_id, item_count * 2 as itemsx2, yy(reg_date) > 10 as regyy FROM stdio`, msg)
	}
}

func runParse(repeat int, sql string, readContext vm.ContextReader) {
	for i := 0; i < repeat; i++ {
		sqlVm, err := vm.NewSqlVm(sql)
		if err != nil {
			panic(err.Error())
		}

		writeContext := vm.NewContextSimple()
		err = sqlVm.Execute(writeContext, readContext)
		if err != nil {
			panic(err.Error())
		}
	}
}

func runVm(repeat int, sql string, readContext vm.ContextReader) {
	sqlVm, err := vm.NewSqlVm(sql)
	if err != nil {
		panic(err.Error())
	}

	for i := 0; i < repeat; i++ {

		writeContext := vm.NewContextSimple()
		err = sqlVm.Execute(writeContext, readContext)
		//log.Println(writeContext.All())
		if err != nil {
			panic(err.Error())
		}
	}
}
