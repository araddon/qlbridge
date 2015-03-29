package exec

import (
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
)

type RuntimeConfig struct {
	Sources        *datasource.DataSources
	singleConn     string // db.driver only allows one connection
	db             string // db.driver only allows one db
	DisableRecover bool
}

func NewRuntimeConfig() *RuntimeConfig {
	c := &RuntimeConfig{
		Sources: datasource.DataSourcesRegistry(),
	}

	return c
}

// given connection info, get datasource
//  @connInfo =    csv:///dev/stdin
//                 mockcsv
//  @from      database name
func (m *RuntimeConfig) Conn(db string) datasource.SourceConn {

	source := m.DataSource(m.singleConn)
	conn, err := source.Open(db)
	//u.Infof("Conn()%p db=%v", conn, db)
	if err != nil {
		u.Errorf("could not open data source: %v  %v", db, err)
		return nil
	}
	return conn

}

// given connection info, get datasource
//  @connInfo =    csv:///dev/stdin
//                 mockcsv
//  @from      database name
func (m *RuntimeConfig) DataSource(connInfo string) datasource.DataSource {
	// if  mysql.tablename allow that convention
	//u.Debugf("get datasource: conn=%v ", connInfo)
	//parts := strings.SplitN(from, ".", 2)
	sourceType := ""
	if len(connInfo) > 0 {
		switch {
		// case strings.HasPrefix(name, "file://"):
		// 	name = name[len("file://"):]
		case strings.HasPrefix(connInfo, "csv://"):
			sourceType = "csv"
			m.db = connInfo[len("csv://"):]
		case strings.Contains(connInfo, "://"):
			strIdx := strings.Index(connInfo, "://")
			sourceType = connInfo[0:strIdx]
			m.db = connInfo[strIdx+3:]
		default:
			sourceType = connInfo
		}
	}

	sourceType = strings.ToLower(sourceType)
	//u.Debugf("source: %v", sourceType)
	if source := m.Sources.Get(sourceType); source != nil {
		//u.Debugf("source: %T", source)
		return source
	} else {
		u.Errorf("source was not found: '%v'", sourceType)
	}

	return nil
}
