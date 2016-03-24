package datasource

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/schema"
)

// Open a go routine to run this source iteration until signal/complete
func SourceIterChannel(iter schema.Iterator, sigCh <-chan bool) <-chan schema.Message {

	out := make(chan schema.Message, 100)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				u.Errorf("recover panic: %v", r)
			}
			// Can we safely close this?
			close(out)
		}()
		for item := iter.Next(); item != nil; item = iter.Next() {

			//u.Infof("In source Scanner iter %#v", item)
			select {
			case <-sigCh:
				u.Warnf("got signal quit")

				return
			case out <- item:
				// continue
			}
		}
	}()
	return out
}
