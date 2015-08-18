package datasource

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"
)

// Open a datasource
//  sourcename = "csv", "elasticsearch"
func OpenConn(sourceName, sourceConfig string) (SourceConn, error) {
	sourcei, ok := sources.sources[sourceName]
	if !ok {
		return nil, fmt.Errorf("datasource: unknown source %q (forgotten import?)", sourceName)
	}
	source, err := sourcei.Open(sourceConfig)
	if err != nil {
		return nil, err
	}
	return source, nil
}

// The RuntimeSchema provides info on available datasources
//  given connection info, get datasource
//
type RuntimeSchema struct {
	Sources        *DataSources // All registered DataSources from which we can create connections
	connInfo       string       // db.driver only allows one connection
	db             string       // db.driver only allows one db
	DisableRecover bool
}

func NewRuntimeSchema() *RuntimeSchema {
	c := &RuntimeSchema{
		Sources: DataSourcesRegistry(),
	}
	return c
}

// Our RunTime configuration possibly only supports a single schema/connection
// info.  for example, the sql/driver interface, so will be set here.
//
//  @connInfo =    csv:///dev/stdin
//
func (m *RuntimeSchema) SetConnInfo(connInfo string) {
	m.connInfo = connInfo
}

// Get connection for given Database
//
//  @db      database name
//
func (m *RuntimeSchema) Conn(db string) SourceConn {

	if m.connInfo == "" {
		//u.Debugf("RuntimeConfig.Conn(db='%v')   // connInfo='%v'", db, m.connInfo)
		if source := m.Sources.Get(strings.ToLower(db)); source != nil {
			//u.Debugf("found source: db=%s   %T", db, source)
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
		//u.Debugf("No Conn? RuntimeConfig.Conn(db='%v')   // connInfo='%v'", db, m.connInfo)
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
func (m *RuntimeSchema) DataSource(connInfo string) DataSource {
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
		u.Errorf("DataSource(conn) was not found: '%v'", sourceType)
	}

	return nil
}

// Our internal map of different types of datasources that are registered
// for our runtime system to use
type DataSources struct {
	sources      map[string]DataSource
	tableSources map[string]DataSource
}

func newDataSources() *DataSources {
	return &DataSources{
		sources:      make(map[string]DataSource),
		tableSources: make(map[string]DataSource),
	}
}

func (m *DataSources) Get(sourceType string) *DataSourceFeatures {
	if source, ok := m.sources[strings.ToLower(sourceType)]; ok {
		//u.Debugf("found source: %v", sourceType)
		return NewFeaturedSource(source)
	}
	if len(m.sources) == 1 {
		for _, src := range m.sources {
			//u.Debugf("only one source?")
			return NewFeaturedSource(src)
		}
	}
	if sourceType == "" {
		u.LogTracef(u.WARN, "No Source Type?")
	} else {
		u.Debugf("datasource.Get('%v')", sourceType)
	}

	if len(m.tableSources) == 0 {
		for _, src := range m.sources {
			tbls := src.Tables()
			for _, tbl := range tbls {
				if _, ok := m.tableSources[tbl]; ok {
					u.Warnf("table names must be unique across sources %v", tbl)
				} else {
					u.Debugf("creating tbl/source: %v  %T", tbl, src)
					m.tableSources[tbl] = src
				}
			}
		}
	}
	if src, ok := m.tableSources[sourceType]; ok {
		//u.Debugf("found src with %v", sourceType)
		return NewFeaturedSource(src)
	} else {
		for src, _ := range m.sources {
			u.Debugf("source: %v", src)
		}
		u.LogTracef(u.WARN, "No table?  len(sources)=%d len(tables)=%v", len(m.sources), len(m.tableSources))
		u.Warnf("could not find table: %v  tables:%v", sourceType, m.tableSources)
	}
	return nil
}

func (m *DataSources) String() string {
	sourceNames := make([]string, 0, len(m.sources))
	for source, _ := range m.sources {
		sourceNames = append(sourceNames, source)
	}
	return fmt.Sprintf("{Sources: [%s] }", strings.Join(sourceNames, ", "))
}

// get registry of all datasource types
func DataSourcesRegistry() *DataSources {
	return sources
}

// Register makes a datasource available by the provided name.
// If Register is called twice with the same name or if source is nil,
// it panics.
func Register(name string, source DataSource) {
	if source == nil {
		panic("qlbridge/datasource: Register driver is nil")
	}
	name = strings.ToLower(name)
	u.Warnf("register datasource: %v %T", name, source)
	//u.LogTracef(u.WARN, "adding source %T to registry", source)
	sourceMu.Lock()
	defer sourceMu.Unlock()
	if _, dup := sources.sources[name]; dup {
		panic("qlbridge/datasource: Register called twice for datasource " + name)
	}
	sources.sources[name] = source
}
