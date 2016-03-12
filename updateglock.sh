#! /bin/sh


cd $GOPATH/src/github.com/araddon/dateparse && git checkout master && git pull
cd $GOPATH/src/github.com/araddon/gou && git checkout master && git pull
cd $GOPATH/src/github.com/gogo/protobuf && git checkout master && git pull
cd $GOPATH/src/github.com/golang/protobuf && git checkout master && git pull
cd $GOPATH/src/golang.org/x/net/context && git checkout master && git pull
cd $GOPATH/src/github.com/couchbaselabs/goforestdb && git checkout master && git pull
cd $GOPATH/src/github.com/rcrowley/go-metrics && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/datemath && git checkout master && git pull
cd $GOPATH/src/github.com/mb0/glob && git checkout master && git pull
cd $GOPATH/src/github.com/dchest/siphash && git checkout master && git pull
cd $GOPATH/src/github.com/google/btree && git checkout master && git pull
cd $GOPATH/src/github.com/leekchan/timeutil && git checkout master && git pull
cd $GOPATH/src/github.com/pborman/uuid && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/datemath && git checkout master && git pull
cd $GOPATH/src/github.com/pborman/uuid && git checkout master && git pull
cd $GOPATH/src/github.com/bmizerany/assert && git checkout master && git pull
cd $GOPATH/src/github.com/kr/pretty && git checkout master && git pull
cd $GOPATH/src/github.com/kr/text && git checkout master && git pull
cd $GOPATH/src/github.com/kr/pty && git checkout master && git pull

go get -u -v ./...

glock save github.com/araddon/qlbridge

