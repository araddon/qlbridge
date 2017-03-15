package exec

import (
	"encoding/json"
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	_ TaskRunner = (*Create)(nil)
)

// Create is executeable task for SET SQL Create, Alter
type Create struct {
	*TaskBase
	p *plan.Create
}

// NewCommand creates new create exec task
func NewCreate(ctx *plan.Context, p *plan.Create) *Create {
	m := &Create{
		TaskBase: NewTaskBase(ctx),
		p:        p,
	}
	return m
}

// Close Create
func (m *Create) Close() error {
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

// Run Create
func (m *Create) Run() error {
	defer close(m.msgOutCh)

	if m.Ctx.Session == nil {
		u.Warnf("no Context.Session?")
		return fmt.Errorf("no Context.Session?")
	}

	cs := m.p.Stmt

	switch cs.Tok.T {
	case lex.TokenSource:

		by, err := json.Marshal(cs.With)
		if err != nil {
			u.Errorf("could not convert conf = %v ", cs.With)
			return fmt.Errorf("could not convert conf %v", cs.With)
		}

		sourceConf := &schema.ConfigSource{}
		err = json.Unmarshal(by, sourceConf)
		if err != nil {
			u.Errorf("could not convert conf = %v ", string(by))
			return fmt.Errorf("could not convert conf %v", cs.With)
		}

		reg := datasource.DataSourcesRegistry()

		//is := m.Ctx.Schema.InfoSchema
		ss := schema.NewSchemaSource(cs.Identity, sourceConf.SourceType)
		sch, ok := reg.Schema(cs.With.String("schema"))
		if !ok {
			u.Errorf("could not find schema = %v ", cs.With)
			return fmt.Errorf("could not find schema %v", cs.With)
		}

		ss.Conf = sourceConf

		sch.AddSourceSchema(ss)

		ds := reg.Get(sourceConf.SourceType)

		if ds == nil {
			//u.Warnf("could not find source for %v  %v", sourceName, sourceConf.SourceType)
		} else {
			ss.DS = ds
			ss.Partitions = sourceConf.Partitions
			if err := ss.DS.Setup(ss); err != nil {
				u.Errorf("Error setuping up %+v  err=%v", sourceConf, err)
				return err
			}
			reg.SourceSchemaAdd(sch.Name, ss)
		}

		// Now refresh the schema, ie load meta-data about the now
		// defined sub-schemas
		sch.RefreshSchema()
		return nil
	default:
		u.Warnf("unrecognized create/alter: kw=%v   stmt:%s", cs.Tok, m.p.Stmt)
	}
	return ErrNotImplemented

}
