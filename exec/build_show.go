package exec

import (
	"database/sql/driver"
	"fmt"
	"strings"

	u "github.com/araddon/gou"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/datasource/membtree"
	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/value"
)

var (
	_ = u.EMPTY
)

func DescribeTable(tbl *datasource.Table) (*membtree.StaticDataSource, *expr.Projection) {
	if len(tbl.Fields) == 0 {
		u.Warnf("NO Fields!!!!! for %s p=%p", tbl.Name, tbl)
	}
	proj := expr.NewProjection()
	for _, f := range datasource.DescribeHeaders {
		proj.AddColumnShort(string(f.Name), f.Type)
		//u.Debugf("found field:  vals=%#v", f)
	}
	tableVals := membtree.NewStaticDataSource("describetable", 0, tbl.DescribeValues, datasource.DescribeCols)
	return tableVals, proj
}

func ShowTables(s *datasource.RuntimeSchema) (*membtree.StaticDataSource, *expr.Projection) {

	/*
		mysql> show full tables from temp like '%';
		+--------------------+------------+
		| Tables_in_temp (%) | Table_type |
		+--------------------+------------+
		| emails             | BASE TABLE |
		| events             | BASE TABLE |
		| evtnames           | BASE TABLE |
		| username           | BASE TABLE |
		+--------------------+------------+
		5 rows in set (0.00 sec)

		mysql> show tables;
		+----------------+
		| Tables_in_temp |
		+----------------+
		| emails         |
		| events         |
		| evtnames       |
		| username       |
		+----------------+
		5 rows in set (0.00 sec)

		mysql> show tables from temp like '%';
		+--------------------+
		| Tables_in_temp (%) |
		+--------------------+
		| emails             |
		| events             |
		| evtnames           |
		| username           |
		+--------------------+
		5 rows in set (0.00 sec)

	*/
	tables := s.Tables()
	vals := make([][]driver.Value, len(tables))
	idx := 0
	if len(tables) == 0 {
		u.Warnf("NO TABLES!!!!! for %+v", s)
	}
	for _, tbl := range tables {
		vals[idx] = []driver.Value{tbl}
		//u.Infof("found table: %v   vals=%v", tbl, vals[idx])
		idx++
	}
	showTableVals := membtree.NewStaticDataSource("schematables", 0, vals, []string{"Table"})
	proj := expr.NewProjection()
	proj.AddColumnShort("Table", value.StringType)
	//u.Infof("showtables:  %v", m.showTableVals)
	return showTableVals, proj
}

func ShowVariables(name string, val driver.Value) (*membtree.StaticDataSource, *expr.Projection) {
	/*
	   MariaDB [(none)]> SHOW SESSION VARIABLES LIKE 'lower_case_table_names';
	   +------------------------+-------+
	   | Variable_name          | Value |
	   +------------------------+-------+
	   | lower_case_table_names | 0     |
	   +------------------------+-------+
	*/
	vals := make([][]driver.Value, 1)
	vals[0] = []driver.Value{name, val}
	dataSource := membtree.NewStaticDataSource("schematables", 0, vals, []string{"Variable_name", "Value"})
	p := expr.NewProjection()
	p.AddColumnShort("Variable_name", value.StringType)
	p.AddColumnShort("Value", value.StringType)
	return dataSource, p
}

func (m *JobBuilder) emptyTask(name string) (TaskRunner, expr.VisitStatus, error) {
	source := membtree.NewStaticDataSource(name, 0, nil, []string{name})
	proj := expr.NewProjection()
	proj.AddColumnShort(name, value.StringType)
	m.Projection = plan.NewProjectionStatic(proj)
	tasks := make(Tasks, 0)
	sourceTask := NewSource(nil, source)
	tasks.Add(sourceTask)
	return NewSequential(name, tasks), expr.VisitContinue, nil
}

func (m *JobBuilder) VisitShow(stmt *expr.SqlShow) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitShow %q  %s", stmt.Identity, stmt.Raw)

	raw := strings.ToLower(stmt.Raw)
	switch {
	case stmt.Create && strings.ToLower(stmt.Identity) == "table":
		// SHOW CREATE TABLE

	case strings.ToLower(stmt.Identity) == "variables":
		// SHOW variables;
		vals := make([][]driver.Value, 2)
		vals[0] = []driver.Value{"auto_increment_increment", "1"}
		vals[1] = []driver.Value{"collation", "utf8"}
		source := membtree.NewStaticDataSource("variables", 0, vals, []string{"Variable_name", "Value"})
		proj := expr.NewProjection()
		proj.AddColumnShort("Variable_name", value.StringType)
		proj.AddColumnShort("Value", value.StringType)
		m.Projection = plan.NewProjectionStatic(proj)
		tasks := make(Tasks, 0)
		sourceTask := NewSource(nil, source)
		u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return NewSequential("variables", tasks), expr.VisitContinue, nil
	case strings.ToLower(stmt.Identity) == "databases":
		// SHOW databases;
		vals := make([][]driver.Value, 1)
		vals[0] = []driver.Value{m.connInfo}
		source := membtree.NewStaticDataSource("databases", 0, vals, []string{"Database"})
		proj := expr.NewProjection()
		proj.AddColumnShort("Database", value.StringType)
		m.Projection = plan.NewProjectionStatic(proj)
		tasks := make(Tasks, 0)
		sourceTask := NewSource(nil, source)
		u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return NewSequential("databases", tasks), expr.VisitContinue, nil
	case strings.ToLower(stmt.Identity) == "collation":
		// SHOW collation;
		vals := make([][]driver.Value, 1)
		// utf8_general_ci          | utf8     |  33 | Yes     | Yes      |       1 |
		vals[0] = []driver.Value{"utf8_general_ci", "utf8", 33, "Yes", "Yes", 1}
		cols := []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}
		source := membtree.NewStaticDataSource("collation", 0, vals, cols)
		proj := expr.NewProjection()
		proj.AddColumnShort("Collation", value.StringType)
		proj.AddColumnShort("Charset", value.StringType)
		proj.AddColumnShort("Id", value.IntType)
		proj.AddColumnShort("Default", value.StringType)
		proj.AddColumnShort("Compiled", value.StringType)
		proj.AddColumnShort("Sortlen", value.IntType)
		m.Projection = plan.NewProjectionStatic(proj)
		tasks := make(Tasks, 0)
		sourceTask := NewSource(nil, source)
		u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return NewSequential("collation", tasks), expr.VisitContinue, nil
	case strings.HasPrefix(raw, "show session"):
		//SHOW SESSION VARIABLES LIKE 'lower_case_table_names';
		source, proj := ShowVariables("lower_case_table_names", 0)
		m.Projection = plan.NewProjectionStatic(proj)
		tasks := make(Tasks, 0)
		sourceTask := NewSource(nil, source)
		u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return NewSequential("session", tasks), expr.VisitContinue, nil
	case strings.ToLower(stmt.Identity) == "tables" || strings.ToLower(stmt.Identity) == m.connInfo:
		if stmt.Full {
			u.Debugf("show tables: %+v", m.Conf)
			tables := m.Conf.Tables()
			vals := make([][]driver.Value, len(tables))
			row := 0
			for _, tbl := range tables {
				vals[row] = []driver.Value{tbl, "BASE TABLE"}
				row++
			}
			source := membtree.NewStaticDataSource("tables", 0, vals, []string{"Tables", "Table_type"})
			proj := expr.NewProjection()
			proj.AddColumnShort("Tables", value.StringType)
			proj.AddColumnShort("Table_type", value.StringType)
			m.Projection = plan.NewProjectionStatic(proj)
			tasks := make(Tasks, 0)
			sourceTask := NewSource(nil, source)
			u.Infof("source rowct=%d  %#v", row, source)
			tasks.Add(sourceTask)
			return NewSequential("show-tables", tasks), expr.VisitContinue, nil
		}
		// SHOW TABLES;
		//u.Debugf("show tables: %+v", m.Conf)
		source, proj := ShowTables(m.Conf)
		m.Projection = plan.NewProjectionStatic(proj)
		tasks := make(Tasks, 0)
		sourceTask := NewSource(nil, source)
		//u.Infof("source:  %#v", source)
		tasks.Add(sourceTask)
		return NewSequential("describe", tasks), expr.VisitContinue, nil
	case strings.ToLower(stmt.Identity) == "procedure":
		// SHOW PROCEDURE STATUS WHERE Db='mydb'
		return m.emptyTask("Procedures")
	case strings.ToLower(stmt.Identity) == "function":
		// SHOW FUNCTION STATUS WHERE Db='mydb'
		return m.emptyTask("Function")
	default:
		// SHOW FULL TABLES FROM `auths`
		desc := expr.SqlDescribe{}
		desc.Identity = stmt.Identity
		return m.VisitDescribe(&desc)
	}
	return nil, expr.VisitError, fmt.Errorf("No handler found")
}

func (m *JobBuilder) VisitDescribe(stmt *expr.SqlDescribe) (expr.Task, expr.VisitStatus, error) {
	u.Debugf("VisitDescribe %+v", stmt)

	if m.Conf == nil {
		return nil, expr.VisitError, ErrNoSchemaSelected
	}
	tbl, err := m.Conf.Table(strings.ToLower(stmt.Identity))
	if err != nil {
		u.Errorf("could not get table: %v", err)
		return nil, expr.VisitError, err
	}
	source, proj := DescribeTable(tbl)
	m.Projection = plan.NewProjectionStatic(proj)

	tasks := make(Tasks, 0)
	sourceTask := NewSource(nil, source)
	u.Infof("source:  %#v", source)
	tasks.Add(sourceTask)

	return NewSequential("describe", tasks), expr.VisitContinue, nil
}
