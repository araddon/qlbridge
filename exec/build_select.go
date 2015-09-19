package exec

import (
	"fmt"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/expr"
)

func (m *JobBuilder) VisitSelect(stmt *expr.SqlSelect) (expr.Task, error) {

	u.Debugf("VisitSelect %+v", stmt)

	/*
		General plan to improve/reimplement this

		- Fold:   n number of sources would fold
		- Some datasources can plan for themselves in which case we don't need to poly fill
	*/

	tasks := make(Tasks, 0)

	if len(stmt.From) == 1 {
		task, err := m.VisitSubselect(stmt.From[0])
		if err != nil {
			return nil, err
		}
		tasks.Add(task.(TaskRunner))
	} else {
		// for now, only support 1 join
		if len(stmt.From) != 2 {
			return nil, fmt.Errorf("3 or more Table/Join not currently implemented")
		}

		// This needs to go into sometype of plan.Finalize()
		var prevTask TaskRunner
		//var prevJoin *JoinMerge
		//sourceTasks := make([]TaskRunner, len(stmt.From)-1)

		for i, from := range stmt.From {
			from.Rewrite(stmt)
			sourceTask, err := m.VisitSubselect(from)
			if err != nil {
				u.Errorf("Could not visitsubselect %v  %s", err, from)
				return nil, err
			}

			// now fold into previous task
			curTask := sourceTask.(TaskRunner)
			//sourceTasks[i] = curTask
			if i != 0 {
				from.Seekable = true
				twoTasks := []TaskRunner{prevTask, curTask}
				curMergeTask := NewTaskParallel("select-sources", nil, twoTasks)
				tasks.Add(curMergeTask)

				// TODO:    Fold n <- n+1
				in, err := NewJoinNaiveMerge(prevTask, curTask, m.schema)
				if err != nil {
					return nil, err
				}
				tasks.Add(in)
			}
			prevTask = curTask
			u.Debugf("got task: %T", prevTask)
		}

		/*
			// This doesn't work bc the Source.go needs to be fixed re
			//  inappropriate usage of source tasks
			for i, from := range stmt.From {
				from.Rewrite(stmt)
				sourceTask, err := m.VisitSubselect(from)
				if err != nil {
					u.Errorf("Could not visitsubselect %v  %s", err, from)
					return nil, err
				}

				curTask := sourceTask.(TaskRunner)
				if i == 0 {
					prevTask = curTask
					continue
				}
				//sourceTasks[i] = curTask

				// now fold into previous task
				from.Seekable = true
				twoTasks := []TaskRunner{prevTask, curTask}
				curMergeTask := NewTaskParallel("select-sources", nil, twoTasks)

				// TODO:    Fold n <- n+1
				joinTask, err := NewJoinNaiveMerge(prevTask, curMergeTask, m.schema)
				if err != nil {
					return nil, err
				}

				mergeTasks := []TaskRunner{prevTask, curTask}
				seqTask := NewSequential("select-source-merge", mergeTasks)
				tasks.Add(mergeTask)

				prevTask = seqTask
				u.Debugf("got task: %T", prevTask)
			}

			// This works fine
			for i, from := range stmt.From {
				from.Rewrite(stmt)
				sourceTask, err := m.VisitSubselect(from)
				if err != nil {
					u.Errorf("Could not visitsubselect %v  %s", err, from)
					return nil, err
				}

				// now fold into previous task
				curTask := sourceTask.(TaskRunner)
				//sourceTasks[i] = curTask
				if i != 0 {
					from.Seekable = true
					twoTasks := []TaskRunner{prevTask, curTask}
					curMergeTask := NewTaskParallel("select-sources", nil, twoTasks)
					tasks.Add(curMergeTask)

					// TODO:    Fold n <- n+1
					in, err := NewJoinNaiveMerge(prevTask, curTask, m.schema)
					if err != nil {
						return nil, err
					}
					tasks.Add(in)
				}
				prevTask = curTask
				u.Debugf("got task: %T", prevTask)
			}



			sourceTasks := NewTaskParallel("select-sources", nil, fromTasks)
			tasks.Add(sourceTasks)

			// TODO:    Fold n <- n+1
			in, err := NewJoinNaiveMerge(fromTasks[0], fromTasks[1], m.schema)
			if err != nil {
				return nil, err
			}
			tasks.Add(in)
		*/
	}

	if stmt.Where != nil {
		switch {
		case stmt.Where.Source != nil:
			u.Warnf("Found un-supported subquery: %#v", stmt.Where)
			return nil, fmt.Errorf("Unsupported Where Type")
		case stmt.Where.Expr != nil:
			u.Debugf("adding where: %q", stmt.Where.Expr)
			where := NewWhereFinal(stmt.Where.Expr, stmt)
			tasks.Add(where)
		default:
			u.Warnf("Found un-supported where type: %#v", stmt.Where)
			return nil, fmt.Errorf("Unsupported Where Type")
		}

	}

	// Add a Projection
	projection := NewProjection(stmt)
	//u.Infof("adding projection: %#v", projection)
	tasks.Add(projection)

	return NewSequential("select", tasks), nil
}

func (m *JobBuilder) VisitSubselect(from *expr.SqlSource) (expr.Task, error) {
	if from.Source != nil {
		u.Debugf("VisitSubselect from.source = %q", from.Source)
	} else {
		u.Debugf("VisitSubselect from=%q", from)
	}

	tasks := make(Tasks, 0)

	switch {

	case from.Name != "" && from.Source == nil:
		// If we have name and no Source(sub-query/join-query) then get from data source

		sourceConn := m.schema.Conn(from.Name)
		//u.Debugf("sourceConn: %T  %#v", sourceConn, sourceConn)
		// Must provider either Scanner, and or Seeker interfaces
		scanner, hasScanner := sourceConn.(datasource.Scanner)
		if !hasScanner {
			return nil, fmt.Errorf("%T Must Implement Scanner for %q", sourceConn, from.String())
		}

		sourceTask := NewSource(from, scanner)
		tasks.Add(sourceTask)

	case from.Source != nil && from.JoinExpr != nil:
		// Join partial query source
		joinSource, err := m.VisitJoin(from)
		if err != nil {
			return nil, err
		}
		tasks.Add(joinSource.(TaskRunner))

	case from.Source != nil && from.JoinExpr == nil:
		// Sub-Query

		source := m.schema.Conn(from.Name)
		//u.Debugf("left source: %T", source)
		// Must provider either Scanner, SourcePlanner, Seeker interfaces
		if sourcePlan, ok := source.(datasource.SourcePlanner); ok {
			//  This is flawed, visitor pattern would have you pass in a object which implements interface
			//    but is one of many different objects that implement that interface so that the
			//    Accept() method calls the apppropriate method
			u.Warnf("SourcePlanner????")
			scanner, err := sourcePlan.Accept(NewSourcePlan(from))
			if err == nil {
				return NewSource(from, scanner), nil
			}
			u.Errorf("Could not source plan for %v  %T %#v", from.Name, source, source)
		}

		scanner, ok := source.(datasource.Scanner)
		if !ok {
			u.Errorf("Could not create scanner for %v  %T %#v", from.Name, source, source)
			return nil, fmt.Errorf("Must Implement Scanner")
		}

		sourceTask := NewSource(from, scanner)
		tasks.Add(sourceTask)

	default:
		u.Warnf("Not able to understand subquery? %s", from.String())
		return nil, expr.ErrNotImplemented
	}

	if from.Source != nil && from.Source.Where != nil {
		switch {
		case from.Source.Where.Expr != nil:
			u.Debugf("adding where: %q", from.Source.Where.Expr)
			where := NewWhereSource(from.Source.Where.Expr, from.Source)
			tasks.Add(where)
		default:
			u.Warnf("Found un-supported where type: %#v", from.Source)
			return nil, fmt.Errorf("Unsupported Where clause:  %q", from)
		}
	}

	// Plan?   Parallel?  hash?
	return NewSequential("sub-select", tasks), nil
}

func (m *JobBuilder) VisitJoin(from *expr.SqlSource) (expr.Task, error) {
	u.Debugf("VisitJoin %s", from.Source)
	//u.Debugf("from.Name:'%v' : %v", from.Name, from.Source.String())
	source := m.schema.Conn(from.Name)
	//u.Debugf("left source: %T", source)
	// Must provider either Scanner, SourcePlanner, Seeker interfaces
	if sourcePlan, ok := source.(datasource.SourcePlanner); ok {
		//  This is flawed, visitor pattern would have you pass in a object which implements interface
		//    but is one of many different objects that implement that interface so that the
		//    Accept() method calls the apppropriate method
		u.Warnf("SourcePlanner????")
		scanner, err := sourcePlan.Accept(NewSourcePlan(from))
		if err == nil {
			return NewSourceJoin(from, scanner), nil
		}
		u.Errorf("Could not source plan for %v  %T %#v", from.Name, source, source)
	}

	scanner, ok := source.(datasource.Scanner)
	if !ok {
		u.Errorf("Could not create scanner for %v  %T %#v", from.Name, source, source)
		return nil, fmt.Errorf("Must Implement Scanner")
	} else {
		return NewSourceJoin(from, scanner), nil
		//u.Debugf("got scanner: %T  %#v", scanner, scanner)
	}
	return nil, expr.ErrNotImplemented
}
