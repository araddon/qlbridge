package exec

import (
	"encoding/json"
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"
)

var (
	// Ensure that we implement the Task Runner interface
	_ TaskRunner = (*Create)(nil)
	_ TaskRunner = (*Drop)(nil)
	_ TaskRunner = (*Alter)(nil)
)

type (
	// Create is executeable task for SQL Create, Alter, Schema, Source etc.
	Create struct {
		*TaskBase
		p *plan.Create
	}
	// Drop is executeable task for SQL DROP.
	Drop struct {
		*TaskBase
		p *plan.Drop
	}
	// Alter is executeable task for SQL ALTER.
	Alter struct {
		*TaskBase
		p *plan.Alter
	}
)

// NewCreate creates new create exec task
func NewCreate(ctx *plan.Context, p *plan.Create) *Create {
	m := &Create{
		TaskBase: NewTaskBase(ctx),
		p:        p,
	}
	return m
}

// Close Create
func (m *Create) Close() error {
	return m.TaskBase.Close()
}

// Run Create
func (m *Create) Run() error {
	defer close(m.msgOutCh)

	cs := m.p.Stmt

	switch cs.Tok.T {
	case lex.TokenSource, lex.TokenSchema:

		/*
			// "sub_schema_name" will create a new child schema called "sub_schema_name"
			// that is added to "existing_schema_name"
			// of source type elasticsearch
			CREATE source sub_schema_name WITH {
			  "type":"elasticsearch",
			  "schema":"existing_schema_name",
			  "settings" : {
			     "apikey":"GET_YOUR_API_KEY"
			  }
			};
		*/
		// If we specify a parent schema to add this child schema to
		schemaName := cs.Identity
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
		sourceConf.Name = schemaName

		reg := schema.DefaultRegistry()

		return reg.SchemaAddFromConfig(sourceConf)
	default:
		u.Warnf("unrecognized create/alter: kw=%v   stmt:%s", cs.Tok, m.p.Stmt)
	}
	return ErrNotImplemented
}

// NewDrop creates new drop exec task.
func NewDrop(ctx *plan.Context, p *plan.Drop) *Drop {
	m := &Drop{
		TaskBase: NewTaskBase(ctx),
		p:        p,
	}
	return m
}

// Close Drop
func (m *Drop) Close() error {
	return m.TaskBase.Close()
}

// Run Drop
func (m *Drop) Run() error {
	defer close(m.msgOutCh)

	cs := m.p.Stmt
	s := m.Ctx.Schema
	if s == nil {
		return fmt.Errorf("must have schema")
	}

	switch cs.Tok.T {
	case lex.TokenSource, lex.TokenSchema, lex.TokenTable:

		reg := schema.DefaultRegistry()
		return reg.SchemaDrop(s.Name, cs.Identity, cs.Tok.T)

	default:
		u.Warnf("unrecognized DROP: kw=%v   stmt:%s", cs.Tok, m.p.Stmt)
	}
	return ErrNotImplemented
}

// NewAlter creates new ALTER exec task.
func NewAlter(ctx *plan.Context, p *plan.Alter) *Alter {
	m := &Alter{
		TaskBase: NewTaskBase(ctx),
		p:        p,
	}
	return m
}

// Close Alter
func (m *Alter) Close() error {
	return m.TaskBase.Close()
}

// Run Alter
func (m *Alter) Run() error {
	defer close(m.msgOutCh)

	cs := m.p.Stmt

	switch cs.Tok.T {
	default:
		u.Warnf("unrecognized ALTER: kw=%v   stmt:%s", cs.Tok, m.p.Stmt)
	}
	return ErrNotImplemented
}
