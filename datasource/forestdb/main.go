package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	u "github.com/araddon/gou"

	forestdb "github.com/couchbaselabs/goforestdb"
	metrics "github.com/rcrowley/go-metrics"
)

var concurrency *int = flag.Int("concurrency", 4, `how many workers to use while writing to the db file`)

func main() {
	flag.Parse()
	u.SetupLogging("debug")
	u.SetColorOutput()
	runtime.GOMAXPROCS(8)

	forestdb_test(*concurrency)

	// wg := new(sync.WaitGroup)
	// wg.Add(1)
	// wg.Wait()
}

func logMetrics(writeMeter string, concurrency int) {
	ticker := time.NewTicker(time.Second * 10)
	stime := time.Now().Unix()
	fmt.Println("concurrency, seconds_run, puts_sec, event_count")
	for now := range ticker.C {

		wrtsize := metrics.DefaultRegistry.Get(writeMeter).(metrics.Meter)
		runtime := now.Unix() - stime

		fmt.Printf("       %d, %d, %f, %d\n",
			concurrency, runtime, wrtsize.RateMean(), wrtsize.Count())
	}
}

const forestdbMeter = "forestdb.msg.meter"

func forestdb_test(concurrency int) {

	// Open a database
	db, err := forestdb.Open("/tmp/test", nil)
	if err != nil {
		u.Errorf("could not open db err=%#v", err)
		return
	}
	// Close it properly when we're done
	defer db.Close()

	//Start Writers
	wg := new(sync.WaitGroup)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go forestdb_writer(i, db, wg)
		time.Sleep(100 * time.Millisecond)
	}

	go logMetrics(forestdbMeter, concurrency)

	wg.Wait()
}

func forestdb_writer(id int, db *forestdb.File, wg *sync.WaitGroup) {
	defer wg.Done()
	start_time := time.Now().Unix()

	kvstore, err := db.OpenKVStoreDefault(forestdb.DefaultKVStoreConfig())
	if err != nil {
		panic(err)
	}
	defer kvstore.Close()

	gen := metrics.NewMeter()
	metrics.GetOrRegister(forestdbMeter, gen)
	for i1 := 1; i1 < 10000; i1++ {
		if time.Now().Unix() > start_time+120 {
			break
		}

		for i := 1; i < 10000; i++ { // Create 10 messages with in this transition.
			keystr := fmt.Sprintf("%d-%d-%d", i1, i, i)
			key := []byte(keystr)

			// Store the document
			kvstore.SetKV(key, createdata(keystr))

			gen.Mark(1)
		}
	}
}

type testentity struct {
	Key        string
	FirstName  string
	LastName   string
	Address    string
	CreateTime int64
}

func createdata(key string) []byte {
	data := &testentity{
		Key:        key,
		FirstName:  "Eric",
		LastName:   "Foo",
		Address:    "1234 loadtest street, MainLandCity, AZ 97123",
		CreateTime: time.Now().Unix(),
	}

	if bytes, err := json.Marshal(data); err != nil {
		log.Printf("createdata error: %v", err)
		return nil
	} else {
		return bytes
	}
}
