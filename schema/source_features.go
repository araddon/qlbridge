package schema

type (
	// DataSourceFeatures describes the features of a datasource.
	DataSourceFeatures struct {
		aggFuncs        map[string]struct{}
		projectionFuncs map[string]*FuncFeature
		GroupBy         bool
		Having          bool
		Partitionable   bool
	}

	// FuncFeature describes the features of a function from datasource.
	FuncFeature struct {
		// Name of the function in underlying source.
		Name string
		// QLBName is the QLBridge name
		QLBName string
	}
)

// FeaturesDefault is list of datasource features.
func FeaturesDefault() *DataSourceFeatures {
	return &DataSourceFeatures{}
}

// HasAgg does this datasource support Agg function (count(*), sum(*)) etc, these func's
// can be pushed down to underlying engine as part of GroupBy query.
func (m *DataSourceFeatures) HasAgg(name string) bool {
	return false
}

// HasProjectionFunc does this datasource support projection function tolower(field)
// can be pushed down to underlying engine as part of projection.
func (m *DataSourceFeatures) HasProjectionFunc(name string) (string, bool) {
	return "", false
}
