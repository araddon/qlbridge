package expr

import (
	"fmt"
	"strconv"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/value"
)

// Parses Tokens and returns an request.
func ParseSql(sqlQuery string) (SqlStatement, error) {
	l := lex.NewSqlLexer(sqlQuery)
	p := Sqlbridge{l: l, SqlTokenPager: NewSqlTokenPager(l), buildVm: false}
	p.firstToken = p.Next()
	return p.parse()
}
func ParseSqlVm(sqlQuery string) (SqlStatement, error) {
	l := lex.NewSqlLexer(sqlQuery)
	p := Sqlbridge{l: l, SqlTokenPager: NewSqlTokenPager(l), buildVm: true}
	p.firstToken = p.Next()
	return p.parse()
}

// generic SQL parser evaluates should be sufficient for most
//  sql compatible languages
type Sqlbridge struct {
	buildVm bool
	l       *lex.Lexer
	*SqlTokenPager
	firstToken lex.Token
}

// parse the request
func (m *Sqlbridge) parse() (SqlStatement, error) {
	switch m.firstToken.T {
	case lex.TokenPrepare:
		return m.parsePrepare()
	case lex.TokenSelect:
		return m.parseSqlSelect()
	case lex.TokenInsert:
		return m.parseSqlInsert()
	case lex.TokenDelete:
		return m.parseSqlDelete()
		// case lex.TokenTypeSqlUpdate:
		// 	return this.parseSqlUpdate()
	case lex.TokenShow:
		return m.parseShow()
	case lex.TokenDescribe, lex.TokenDesc:
		return m.parseDescribe()
	}
	return nil, fmt.Errorf("Unrecognized request type")
}

// First keyword was SELECT, so use the SELECT parser rule-set
func (m *Sqlbridge) parseSqlSelect() (*SqlSelect, error) {

	req := NewSqlSelect()
	m.Next()

	// columns
	if m.Cur().T != lex.TokenStar {
		if err := m.parseColumns(req); err != nil {
			u.Error(err)
			return nil, err
		}
	} else if err := m.parseSelectStar(req); err != nil {
		u.Error(err)
		return nil, err
	}

	//u.Infof("cur? %v", m.Cur())
	// select @@myvar limit 1
	if m.Cur().T == lex.TokenLimit {
		if err := m.parseLimit(req); err != nil {
			return req, nil
		}
		if m.isEnd() {
			return req, nil
		}
	}

	// SPECIAL END CASE for simple selects
	// SELECT last_insert_id();
	if m.Cur().T == lex.TokenEOS || m.Cur().T == lex.TokenEOF {
		// valid end
		return req, nil
	}

	// FROM
	//u.Debugf("token:  %v", m.Cur())
	if m.Cur().T != lex.TokenFrom {
		return nil, fmt.Errorf("expected From but got: %v", m.Cur())
	} else {
		// table name
		m.Next() // page forward off of From to Table name
		//u.Debugf("found from?  %v", m.Cur())
		if m.Cur().T != lex.TokenIdentity && m.Cur().T != lex.TokenValue {
			//u.Warnf("No From? %v toktype:%v", m.Cur().V, m.Cur().T.String())
			return nil, fmt.Errorf("expected from name but got: %v", m.Cur())
		} else {
			req.From = m.Cur().V
			m.Next()
		}
	}

	// WHERE
	//u.Infof("where? %v", m.Cur())
	//m.Next()
	//u.Debugf("cur lex.Token: %s", m.Cur().T.String())
	if errreq := m.parseWhere(req); errreq != nil {
		return nil, errreq
	}

	// GROUP BY
	//u.Debugf("GroupBy?  : %v", m.Cur())
	if errreq := m.parseGroupBy(req); errreq != nil {
		return nil, errreq
	}

	// GROUP BY
	//u.Debugf("GroupBy?  : %v", m.Cur())
	if errreq := m.parseGroupBy(req); errreq != nil {
		return nil, errreq
	}

	// ORDER BY
	if errreq := m.parseOrderBy(req); errreq != nil {
		return nil, errreq
	}
	// LIMIT
	if err := m.parseLimit(req); err != nil {
		return req, nil
	}

	// we are good
	return req, nil
}

// First keyword was INSERT
func (m *Sqlbridge) parseSqlInsert() (*SqlInsert, error) {

	// insert into mytable (id, str) values (0, "a")
	req := NewSqlInsert()
	m.Next()

	// into
	//u.Debugf("token:  %v", m.Cur())
	if m.Cur().T != lex.TokenInto {
		return nil, fmt.Errorf("expected INTO but got: %v", m.Cur())
	} else {
		// table name
		m.Next()
		//u.Debugf("found into?  %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenTable:
			req.Into = m.Cur().V
		default:
			return nil, fmt.Errorf("expected table name but got : %v", m.Cur().V)
		}
	}

	// list of fields
	m.Next()
	if err := m.parseFieldList(req); err != nil {
		u.Error(err)
		return nil, err
	}
	m.Next()
	//u.Debugf("found ?  %v", m.Cur())
	switch m.Cur().T {
	case lex.TokenValues:
		m.Next()
	default:
		return nil, fmt.Errorf("expected values but got : %v", m.Cur().V)
	}
	//u.Debugf("found ?  %v", m.Cur())
	if err := m.parseValueList(req); err != nil {
		u.Error(err)
		return nil, err
	}
	// we are good
	return req, nil
}

// First keyword was DELETE
func (m *Sqlbridge) parseSqlDelete() (*SqlDelete, error) {

	req := NewSqlDelete()
	m.Next()

	// from
	//u.Debugf("token:  %v", m.Cur())
	if m.Cur().T != lex.TokenFrom {
		return nil, fmt.Errorf("expected FROM but got: %v", m.Cur())
	} else {
		// table name
		m.Next()
		u.Debugf("found table?  %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenTable:
			req.Table = m.Cur().V
		default:
			return nil, fmt.Errorf("expected table name but got : %v", m.Cur().V)
		}
	}

	m.Next()
	//u.Debugf("cur lex.Token: %s", m.Cur().T.String())
	if errreq := m.parseWhereDelete(req); errreq != nil {
		return nil, errreq
	}
	// we are good
	return req, nil
}

// First keyword was PREPARE
func (m *Sqlbridge) parsePrepare() (*PreparedStatement, error) {

	req := NewPreparedStatement()
	m.Next()

	// statement name/alias
	//u.Debugf("found table?  %v", m.Cur())
	switch m.Cur().T {
	case lex.TokenTable, lex.TokenIdentity:
		req.Alias = m.Cur().V
	default:
		return nil, fmt.Errorf("expected statement name but got : %v", m.Cur().V)
	}

	// from
	m.Next()
	//u.Debugf("token:  %v", m.Cur())
	if m.Cur().T != lex.TokenFrom {
		return nil, fmt.Errorf("expected FROM but got: %v", m.Cur())
	}

	m.Next()
	if m.Cur().T != lex.TokenValue {
		return nil, fmt.Errorf("expected statement value but got: %v", m.Cur())
	}
	stmt, err := ParseSql(m.Cur().V)
	if err != nil {
		return nil, err
	}
	req.Statement = stmt
	// we are good
	return req, nil
}

// First keyword was DESCRIBE
func (m *Sqlbridge) parseDescribe() (*SqlDescribe, error) {

	req := &SqlDescribe{}
	m.Next()

	//u.Debugf("token:  %v", m.Cur())
	if m.Cur().T != lex.TokenIdentity {
		return nil, fmt.Errorf("expected idenity but got: %v", m.Cur())
	}
	req.Identity = m.Cur().V
	return req, nil
}

// First keyword was SHOW
func (m *Sqlbridge) parseShow() (*SqlShow, error) {

	req := &SqlShow{}
	m.Next()

	//u.Debugf("token:  %v", m.Cur())
	if m.Cur().T != lex.TokenIdentity {
		return nil, fmt.Errorf("expected idenity but got: %v", m.Cur())
	}
	req.Identity = m.Cur().V
	return req, nil
}

// Recursively descend down a node looking for first Identity Field
//
//     min(year)                 == min_year
//     eq(min(year), max(month)) == eq_year
func findIdentityField(depth int, node Node, prefix string) string {

	switch n := node.(type) {
	case *IdentityNode:
		if prefix == "" {
			return n.Text
		}
		return fmt.Sprintf("%s_%s", prefix, n.Text)
	case *BinaryNode:
		for _, arg := range n.Args {
			return findIdentityField(depth+1, arg, strings.ToLower(arg.String()))
		}
	case *FuncNode:
		if depth > 10 {
			return ""
		}
		for _, arg := range n.Args {
			return findIdentityField(depth+1, arg, strings.ToLower(n.F.Name))
		}
	}
	return ""

}

func (m *Sqlbridge) parseColumns(stmt *SqlSelect) error {

	var col *Column

	for {

		//u.Debug(m.Cur())
		switch m.Cur().T {
		case lex.TokenUdfExpr:
			// we have a udf/functional expression column
			//u.Infof("udf: %v", m.Cur().V)
			col = &Column{As: m.Cur().V, Tree: NewTree(m.SqlTokenPager)}
			m.parseNode(col.Tree)

			if m.Cur().T != lex.TokenAs {
				switch n := col.Tree.Root.(type) {
				case *FuncNode:
					col.As = findIdentityField(0, n, "")
					if col.As == "" {
						col.As = n.Name
					}
				case *BinaryNode:
					//u.Debugf("udf? %T ", col.Tree.Root)
					col.As = findIdentityField(0, n, "")
					if col.As == "" {
						u.Errorf("could not find as name: %#v", col.Tree)
					}
				}
			}
			//u.Debugf("next? %v", m.Cur())

		case lex.TokenIdentity:
			//u.Warnf("?? %v", m.Cur())
			col = &Column{As: m.Cur().V, Tree: NewTree(m.SqlTokenPager)}
			m.parseNode(col.Tree)
		case lex.TokenValue:
			// Value Literal
			col = &Column{As: m.Cur().V, Tree: NewTree(m.SqlTokenPager)}
			m.parseNode(col.Tree)
		}
		//u.Debugf("after colstart?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenAs:
			m.Next()
			//u.Debug(m.Cur())
			switch m.Cur().T {
			case lex.TokenIdentity, lex.TokenValue:
				col.As = m.Cur().V
				//u.Infof("set AS=%v", col.As)
				m.Next()
				continue
			}
			return fmt.Errorf("expected identity but got: %v", m.Cur().String())
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			// This indicates we have come to the End of the columns
			stmt.Columns = append(stmt.Columns, col)
			//u.Debugf("Ending column ")
			return nil
		case lex.TokenIf:
			// If guard
			m.Next()
			//u.Infof("if guard: %v", m.Cur())
			col.Guard = NewTree(m.SqlTokenPager)
			//m.Next()
			//u.Infof("if guard 2: %v", m.Cur())
			m.parseNode(col.Guard)
			//u.Debugf("after if guard?:   %v  ", m.Cur())
		case lex.TokenCommentSingleLine:
			m.Next()
			col.Comment = m.Cur().V
		case lex.TokenRightParenthesis:
			// loop on my friend
		case lex.TokenComma:
			stmt.Columns = append(stmt.Columns, col)
			//u.Debugf("comma, added cols:  %v", len(stmt.Columns))
		default:
			return fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	//u.Debugf("cols: %d", len(stmt.Columns))
	return nil
}

func (m *Sqlbridge) parseFieldList(stmt *SqlInsert) error {

	var col *Column
	if m.Cur().T != lex.TokenLeftParenthesis {
		return fmt.Errorf("Expecting opening paren ( but got %v", m.Cur())
	}
	m.Next()

	for {

		//u.Debug(m.Cur().String())
		switch m.Cur().T {
		// case lex.TokenUdfExpr:
		// 	// we have a udf/functional expression column
		// 	col = &Column{As: m.Cur().V, Tree: NewTree(m.pager)}
		// 	m.parseNode(col.Tree)
		case lex.TokenIdentity:
			col = &Column{As: m.Cur().V}
			m.Next()
		}
		//u.Debugf("after colstart?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF,
			lex.TokenRightParenthesis:
			// This indicates we have come to the End of the columns
			stmt.Columns = append(stmt.Columns, col)
			//u.Debugf("Ending column ")
			return nil
		case lex.TokenComma:
			stmt.Columns = append(stmt.Columns, col)
			//u.Debugf("comma, added cols:  %v", len(stmt.Columns))
		default:
			return fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	//u.Debugf("cols: %d", len(stmt.Columns))
	return nil
}

func (m *Sqlbridge) parseValueList(stmt *SqlInsert) error {

	if m.Cur().T != lex.TokenLeftParenthesis {
		return fmt.Errorf("Expecting opening paren ( but got %v", m.Cur())
	}
	//m.Next()
	stmt.Rows = make([][]value.Value, 0)
	var row []value.Value
	for {

		//u.Debug(m.Cur().String())
		switch m.Cur().T {
		case lex.TokenLeftParenthesis:
			// start of row
			row = make([]value.Value, 0)
		case lex.TokenRightParenthesis:
			stmt.Rows = append(stmt.Rows, row)
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			// This indicates we have come to the End of the values
			//u.Debugf("Ending %v ", m.Cur())
			return nil
		case lex.TokenValue:
			row = append(row, value.NewStringValue(m.Cur().V))
		case lex.TokenInteger:
			iv, _ := strconv.ParseInt(m.Cur().V, 10, 64)
			row = append(row, value.NewIntValue(iv))
		case lex.TokenComma:
			//row = append(row, col)
			//u.Debugf("comma, added cols:  %v", len(stmt.Columns))
		default:
			u.Warnf("don't know how to handle ?  %v", m.Cur())
			return fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	//u.Debugf("cols: %d", len(stmt.Columns))
	return nil
}

// Parse an expression tree or root Node
func (m *Sqlbridge) parseNode(tree *Tree) error {
	//u.Debugf("cur token parse: token=%v", m.Cur())
	err := tree.BuildTree(m.buildVm)
	if err != nil {
		u.Errorf("error: %v", err)
	}
	return err
}

func (m *Sqlbridge) parseSelectStar(req *SqlSelect) error {

	req.Star = true
	req.Columns = make(Columns, 0)
	col := &Column{Star: true}
	req.Columns = append(req.Columns, col)

	m.Next()
	return nil
}

func (m *Sqlbridge) parseWhere(req *SqlSelect) (err error) {

	if m.Cur().T != lex.TokenWhere {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			u.Errorf("where error? %v \n %v", r, m.Cur())
			if m.Cur().T == lex.TokenSelect {
				// TODO this is deeply flawed, need to fix/use tokenpager
				// with rewind ability
				err = m.parseWhereSelect(req)
				return
			}
			err = fmt.Errorf("panic err: %v", r)
		}
	}()
	m.Next()
	//u.Infof("%v", m.Cur())
	tree := NewTree(m.SqlTokenPager)
	m.parseNode(tree)
	req.Where = tree.Root
	//u.Debugf("where: %v", m.Cur())
	return err
}

func (m *Sqlbridge) parseGroupBy(req *SqlSelect) (err error) {

	if m.Cur().T != lex.TokenGroupBy {
		return nil
	}
	m.Next()

	var col *Column

	for {

		//u.Debugf("Group By? %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenUdfExpr:
			// we have a udf/functional expression column
			//u.Infof("udf: %v", m.Cur().V)
			col = &Column{As: m.Cur().V, Tree: NewTree(m.SqlTokenPager)}
			m.parseNode(col.Tree)

			if m.Cur().T != lex.TokenAs {
				switch n := col.Tree.Root.(type) {
				case *FuncNode:
					col.As = findIdentityField(0, n, "")
					if col.As == "" {
						col.As = n.Name
					}
				case *BinaryNode:
					//u.Debugf("udf? %T ", col.Tree.Root)
					col.As = findIdentityField(0, n, "")
					if col.As == "" {
						u.Errorf("could not find as name: %#v", col.Tree)
					}
				}
			}
			//u.Debugf("next? %v", m.Cur())

		case lex.TokenIdentity:
			//u.Warnf("?? %v", m.Cur())
			col = &Column{As: m.Cur().V, Tree: NewTree(m.SqlTokenPager)}
			m.parseNode(col.Tree)
		case lex.TokenValue:
			// Value Literal
			col = &Column{As: m.Cur().V, Tree: NewTree(m.SqlTokenPager)}
			m.parseNode(col.Tree)
		}
		//u.Debugf("GroupBy after colstart?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenAs:
			m.Next()
			//u.Debug(m.Cur())
			switch m.Cur().T {
			case lex.TokenIdentity, lex.TokenValue:
				col.As = m.Cur().V
				//u.Infof("set AS=%v", col.As)
				m.Next()
				continue
			}
			return fmt.Errorf("expected identity but got: %v", m.Cur().String())
		case lex.TokenFrom, lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			// This indicates we have come to the End of the columns
			req.GroupBy = append(req.GroupBy, col)
			//u.Debugf("Ending column ")
			return nil
		case lex.TokenIf:
			// If guard
			m.Next()
			//u.Infof("if guard: %v", m.Cur())
			col.Guard = NewTree(m.SqlTokenPager)
			//m.Next()
			//u.Infof("if guard 2: %v", m.Cur())
			m.parseNode(col.Guard)
			//u.Debugf("after if guard?:   %v  ", m.Cur())
		case lex.TokenCommentSingleLine:
			m.Next()
			col.Comment = m.Cur().V
		case lex.TokenRightParenthesis:
			// loop on my friend
		case lex.TokenComma:
			req.GroupBy = append(req.GroupBy, col)
			//u.Debugf("comma, added groupby:  %v", len(stmt.GroupBy))
		default:
			return fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	//u.Debugf("groupby: %d", len(req.GroupBy))
	return nil
}

func (m *Sqlbridge) parseOrderBy(req *SqlSelect) (err error) {

	if m.Cur().T != lex.TokenOrderBy {
		return nil
	}
	m.Next() // Consume Order By

	var col *Column

	for {

		//u.Debugf("Order By? %v", m.Cur())
		switch m.Cur().T {
		case lex.TokenUdfExpr:
			// we have a udf/functional expression column
			//u.Infof("udf: %v", m.Cur().V)
			col = &Column{As: m.Cur().V, Tree: NewTree(m.SqlTokenPager)}
			m.parseNode(col.Tree)
			switch n := col.Tree.Root.(type) {
			case *FuncNode:
				col.As = findIdentityField(0, n, "")
				if col.As == "" {
					col.As = n.Name
				}
			case *BinaryNode:
				//u.Debugf("udf? %T ", col.Tree.Root)
				col.As = findIdentityField(0, n, "")
				if col.As == "" {
					u.Errorf("could not find as name: %#v", col.Tree)
				}
			}
			//u.Debugf("next? %v", m.Cur())
		case lex.TokenIdentity:
			//u.Warnf("?? %v", m.Cur())
			col = &Column{As: m.Cur().V, Tree: NewTree(m.SqlTokenPager)}
			m.parseNode(col.Tree)
		}
		//u.Debugf("OrderBy after colstart?:   %v  ", m.Cur())

		// since we can loop inside switch statement
		switch m.Cur().T {
		case lex.TokenAsc, lex.TokenDesc:
			col.Order = strings.ToUpper(m.Cur().V)

		case lex.TokenInto, lex.TokenLimit, lex.TokenEOS, lex.TokenEOF:
			// This indicates we have come to the End of the columns
			req.OrderBy = append(req.OrderBy, col)
			//u.Debugf("Ending column ")
			return nil
		case lex.TokenCommentSingleLine:
			m.Next()
			col.Comment = m.Cur().V
		case lex.TokenRightParenthesis:
			// loop on my friend
		case lex.TokenComma:
			req.OrderBy = append(req.OrderBy, col)
			//u.Debugf("comma, added groupby:  %v", len(stmt.OrderBy))
		default:
			return fmt.Errorf("expected column but got: %v", m.Cur().String())
		}
		m.Next()
	}
	//u.Debugf("OrderBy: %d", len(req.OrderBy))
	return nil
}

func (m *Sqlbridge) parseWhereSelect(req *SqlSelect) error {

	if m.Cur().T != lex.TokenSelect {
		return nil
	}
	stmt, err := m.parseSqlSelect()
	if err != nil {
		return err
	}
	u.Infof("found sub-select %+v", stmt)
	req.SubQuery = stmt
	return nil
}

func (m *Sqlbridge) parseWhereDelete(req *SqlDelete) error {

	if m.Cur().T != lex.TokenWhere {
		return nil
	}

	m.Next()
	tree := NewTree(m.SqlTokenPager)
	m.parseNode(tree)
	req.Where = tree.Root
	return nil
}

func (m *Sqlbridge) parseLimit(req *SqlSelect) error {
	m.Next()
	if m.Cur().T != lex.TokenInteger {
		return fmt.Errorf("Limit must be an integer %v %v", m.Cur().T, m.Cur().V)
	}
	iv, err := strconv.Atoi(m.Cur().V)
	m.Next()
	if err != nil {
		return fmt.Errorf("Could not convert limit to integer %v", m.Cur().V)
	}
	req.Limit = int(iv)
	return nil
}

func (m *Sqlbridge) isEnd() bool {
	return m.IsEnd()
}

// TokenPager is responsible for determining end of
// current tree (column, etc)
type SqlTokenPager struct {
	*LexTokenPager
}

func NewSqlTokenPager(lex *lex.Lexer) *SqlTokenPager {
	pager := NewLexTokenPager(lex)
	return &SqlTokenPager{pager}
}

func (m *SqlTokenPager) IsEnd() bool {
	tok := m.Cur()
	//u.Debugf("IsEnd()? tok:  %v", tok)
	switch tok.T {
	case lex.TokenEOF, lex.TokenEOS, lex.TokenFrom, lex.TokenComma, lex.TokenIf,
		lex.TokenAs, lex.TokenLimit, lex.TokenSelect:
		return true
	}
	return false
}
