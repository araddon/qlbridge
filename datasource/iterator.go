package datasource

import (
	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
)

// simple iterator interface for paging through a datastore Messages/rows
// - used for scanning
// - for datasources that implement exec.Visitor() (ie, select) this
//    represents the alreader filtered, calculated rows
type Iterator interface {
	Next() Message
}

// Open a go routine to run this source iteration until signal/complete
func SourceIterChannel(iter Iterator, filter expr.Node, sigCh <-chan bool) <-chan Message {

	out := make(chan Message, 100)

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
