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

		/*
			// "sub_schema_name" will create a new child schema called "sub_schema_name"
			// that is added to "existing_schema_name"
			// of type elasticsearch
			CREATE source sub_schema_name WITH {
			  "type":"elasticsearch",
			  "schema":"existing_schema_name",
			  "settings" : {
			     "apikey":"GET_YOUR_API_KEY"
			  }
			};
		*/
		by, err := json.MarshalIndent(cs.With, "", "  ")
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

		source, err := reg.GetSource(sourceConf.SourceType)
		if err != nil {
			return err
		}

		schemaName := cs.Identity
		sourceConf.Name = schemaName

		s := schema.NewSchema(schemaName)
		s.Conf = sourceConf
		s.DS = source

		u.Debugf("settings %v", s.Conf.Settings)
		u.Debugf("reg.Get(%q)", sourceConf.SourceType)

		parentSchema := cs.With.String("schema")
		if parentSchema != "" && parentSchema != schemaName {
			parent, ok := reg.Schema(parentSchema)
			if !ok {
				return fmt.Errorf("Could not find schema %q", parentSchema)
			}
			parent.AddChildSchema(s)
		} else {
			reg.SchemaAdd(s)
		}

		if err := s.DS.Setup(s); err != nil {
			u.Errorf("Error setuping up %+v  err=%v", sourceConf, err)
			return err
		}

		return nil
	default:
		u.Warnf("unrecognized create/alter: kw=%v   stmt:%s", cs.Tok, m.p.Stmt)
	}
	return ErrNotImplemented
}
