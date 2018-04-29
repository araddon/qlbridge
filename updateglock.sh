#! /bin/sh


cd $GOPATH/src/github.com/araddon/dateparse && git checkout master && git pull
cd $GOPATH/src/github.com/araddon/gou && git checkout master && git pull
cd $GOPATH/src/github.com/couchbaselabs/goforestdb && git checkout master && git pull
# cd $GOPATH/src/github.com/dataux/dataux && git checkout master && git pull
cd $GOPATH/src/github.com/davecgh/go-spew && git checkout master && git pull
cd $GOPATH/src/github.com/dchest/siphash && git checkout master && git pull
cd $GOPATH/src/github.com/go-sql-driver/mysql && git checkout master && git pull
cd $GOPATH/src/github.com/gogo/protobuf && git checkout master && git pull
cd $GOPATH/src/github.com/golang/protobuf && git checkout master && git pull
cd $GOPATH/src/github.com/googleapis/gax-go && git checkout master && git pull
cd $GOPATH/src/github.com/google/btree && git checkout master && git pull
cd $GOPATH/src/github.com/hashicorp/go-immutable-radix && git checkout master && git pull
cd $GOPATH/src/github.com/hashicorp/go-memdb && git checkout master && git pull
cd $GOPATH/src/github.com/hashicorp/golang-lru && git checkout master && git pull
cd $GOPATH/src/github.com/jmespath/go-jmespath && git checkout master && git pull
cd $GOPATH/src/github.com/jmoiron/sqlx && git checkout master && git pull
cd $GOPATH/src/github.com/kr/pretty && git checkout master && git pull
cd $GOPATH/src/github.com/kr/pty && git checkout master && git pull
cd $GOPATH/src/github.com/kr/text && git checkout master && git pull
cd $GOPATH/src/github.com/leekchan/timeutil && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/cloudstorage && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/confl && git checkout master && git pull
cd $GOPATH/src/github.com/lytics/datemath && git checkout master && git pull
cd $GOPATH/src/github.com/mb0/glob && git checkout master && git pull
cd $GOPATH/src/github.com/mssola/user_agent && git checkout master && git pull
cd $GOPATH/src/github.com/pborman/uuid && git checkout master && git pull
cd $GOPATH/src/github.com/rcrowley/go-metrics && git checkout master && git pull
cd $GOPATH/src/github.com/stretchr/testify && git checkout master && git pull
cd $GOPATH/src/github.com/go.opencensus.io && git checkout master && git pull

cd $GOPATH/src/golang.org/x/crypto && git checkout master && git pull
cd $GOPATH/src/golang.org/x/net && git checkout master && git pull
cd $GOPATH/src/golang.org/x/text && git checkout master && git pull
cd $GOPATH/src/golang.org/x/sys && git checkout master && git pull
cd $GOPATH/src/golang.org/x/oauth2 && git checkout master && git pull


cd $GOPATH/src/google.golang.org/api && git checkout master && git pull
cd $GOPATH/src/google.golang.org/appengine && git checkout master && git pull
cd $GOPATH/src/google.golang.org/genproto && git checkout master && git pull
cd $GOPATH/src/google.golang.org/grpc && git checkout master && git pull
cd $GOPATH/src/cloud.google.com/go/ && git checkout master && git pull


#go get -u -v ./...

#glock save github.com/araddon/qlbridge

