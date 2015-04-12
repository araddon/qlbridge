package datasource

import (
	"strings"

	u "github.com/araddon/gou"
)

// The RuntimeSchema config providing access to available datasources
//  given connection info, get datasource
//
type RuntimeConfig struct {
	Sources        *DataSources // All registered DataSources from which we can create connections
	connInfo       string       // db.driver only allows one connection
	db             string       // db.driver only allows one db
	DisableRecover bool
}

func NewRuntimeConfig() *RuntimeConfig {
	c := &RuntimeConfig{
		Sources: DataSourcesRegistry(),
	}
	return c
}

// Our RunTime configuration possibly only supports a single schema/connection
// info.  for example, the sql/driver interface, so will be set here.
//
//  @connInfo =    csv:///dev/stdin
//
func (m *RuntimeConfig) SetConnInfo(connInfo string) {
	m.connInfo = connInfo
}

// Get connection for given Database
//
//  @db      database name
//
func (m *RuntimeConfig) Conn(db string) SourceConn {

	if m.connInfo == "" {
		u.Debugf("RuntimeConfig.Conn(db='%v')   // connInfo='%v'", db, m.connInfo)
		if source := m.Sources.Get(strings.ToLower(db)); source != nil {
			u.Debugf("found source: db=%s   %T", db, source)
			conn, err := source.Open(db)
			if err != nil {
				u.Errorf("could not open data source: %v  %v", db, err)
				return nil
			}
			//u.Infof("source: %T  %#v", conn, conn)
			return conn
		} else {
			u.Errorf("DataSource(%s) was not found", db)
		}
	} else {
		u.Debugf("No Conn? RuntimeConfig.Conn(db='%v')   // connInfo='%v'", db, m.connInfo)
		// We have connection info, likely sq/driver
		source := m.DataSource(m.connInfo)
		//u.Infof("source=%v    about to call Conn() db='%v'", source, db)
		conn, err := source.Open(db)

		if err != nil {
			u.Errorf("could not open data source: %v  %v", db, err)
			return nil
		}
		return conn
	}
	return nil
}

// given connection info, get datasource
//  @connInfo =    csv:///dev/stdin
//                 mockcsv
func (m *RuntimeConfig) DataSource(connInfo string) DataSource {
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
		u.Debugf("source: %T", source)
		return source
	} else {
		u.Errorf("DataSource(conn) was not found: '%v'", sourceType)
	}

	return nil
}
