package exec

import (
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/rel"
	"github.com/araddon/qlbridge/vm"
)

var (
	_ = u.EMPTY

	// Ensure that we implement the Task Runner interface
	_ TaskRunner = (*Command)(nil)
)

// Command is executeable task for SET SQL commands
type Command struct {
	*TaskBase
	p *plan.Command
}

// NewCommand creates new command exec task
func NewCommand(ctx *plan.Context, p *plan.Command) *Command {
	m := &Command{
		TaskBase: NewTaskBase(ctx),
		p:        p,
	}
	return m
}

// Close Command
func (m *Command) Close() error {
	if err := m.TaskBase.Close(); err != nil {
		return err
	}
	return nil
}

// Run Command
func (m *Command) Run() error {
	//defer m.Ctx.Recover()
	defer close(m.msgOutCh)

	if m.Ctx.Session == nil {
		u.Warnf("no Context.Session?")
		return fmt.Errorf("no Context.Session?")
	}

	switch kw := m.p.Stmt.Keyword(); kw {
	case lex.TokenSet:
		return m.runSet()
	case lex.TokenRollback, lex.TokenCommit:
		u.Debugf("ignorning transaction, not implemented.  %v", kw.String())
		return nil
	default:
		u.Warnf("unrecognized command: kw=%v   stmt:%s", kw, m.p.Stmt)
	}
	return ErrNotImplemented

}
func (m *Command) runSet() error {

	writeContext, ok := m.Ctx.Session.(expr.ContextWriter)
	if !ok || writeContext == nil {
		u.Warnf("expected context writer but no for %T", m.Ctx.Session)
		return fmt.Errorf("No write context?")
	}

	//u.Debugf("running set? %v", m.p.Stmt.String())
	for _, col := range m.p.Stmt.Columns {
		err := evalSetExpression(col, m.Ctx.Session, col.Expr)
		if err != nil {
			u.Warnf("Could not evaluate [%s] err=%v", col.Expr, err)
			return err
		}
	}
	// for k, v := range m.Ctx.Session.Row() {
	// 	u.Infof("%p session? %s: %v", m.Ctx.Session, k, v.Value())
	// }
	return nil
}

func evalSetExpression(col *rel.CommandColumn, ctx expr.ContextReadWriter, arg expr.Node) error {

	switch bn := arg.(type) {
	case *expr.BinaryNode:
		_, ok := bn.Args[0].(*expr.IdentityNode)
		if !ok {
			u.Warnf("expected identity but got %T in %s", bn.Args[0], arg.String())
			return fmt.Errorf("Expected identity but got %T", bn.Args[0])
		}
		rhv, ok := vm.Eval(ctx, bn.Args[1])
		if !ok {
			u.Warnf("expected right side value but got %T in %s", bn.Args[1], arg.String())
			return fmt.Errorf("Expected value but got %T", bn.Args[1])
		}
		//u.Infof(`writeContext.Put("%v",%v)`, col.Key(), rhv.Value())
		ctx.Put(col, ctx, rhv)
	case nil:
		// Special statements
		name := strings.ToLower(col.Name)
		switch {
		case strings.HasPrefix(name, "names ") || strings.HasPrefix(name, "character set"):
			// http://dev.mysql.com/doc/refman/5.7/en/charset-connection.html
			// hm, no idea what to do
			/*
				SET character_set_client = charset_name;
				SET character_set_results = charset_name;
				SET character_set_connection = charset_name;
			*/
		}
	default:
		u.Errorf("SET command only accepts binary nodes but got type:  %#v", arg)
		return fmt.Errorf("Un recognized command %T", arg)
	}
	return nil
}
