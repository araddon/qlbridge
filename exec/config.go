package exec

import (
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
)

type RuntimeConfig struct {
	Sources *datasource.DataSources
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
func (m *RuntimeConfig) DataSource(connInfo, from string) datasource.DataSource {
	// if  mysql.tablename allow that convention
	u.Debugf("get datasource: conn=%v from=%v  ", connInfo, from)
	//parts := strings.SplitN(from, ".", 2)
	sourceType, fileOrDb := "", ""
	if len(connInfo) > 0 {
		switch {
		// case strings.HasPrefix(name, "file://"):
		// 	name = name[len("file://"):]
		case strings.HasPrefix(connInfo, "csv://"):
			sourceType = "csv"
			fileOrDb = connInfo[len("csv://"):]
		case strings.Contains(connInfo, "://"):
			strIdx := strings.Index(connInfo, "://")
			sourceType = connInfo[0:strIdx]
			fileOrDb = connInfo[strIdx+3:]
		default:
			sourceType = connInfo
			fileOrDb = from
		}
	}

	sourceType = strings.ToLower(sourceType)
	u.Debugf("source: %v  db=%v", sourceType, fileOrDb)
	if source := m.Sources.Get(sourceType); source != nil {
		u.Debugf("source: %T", source)
		dataSource, err := source.Open(fileOrDb)
		if err != nil {
			u.Errorf("could not open data source: %v  %v", fileOrDb, err)
			return nil
		}
		return dataSource
	} else {
		u.Errorf("source was not found: %v", sourceType)
	}

	return nil
}
