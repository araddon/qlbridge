package vm

type Token struct {
	PosImpl // StmtImpl provide Pos() function.
	Tok     int
	Lit     string
}

// Stmt provides all of interfaces for statement.
type Stmt interface {
	Pos
	stmt()
}

// Expr provides all of interfaces for expression.
type Expr interface {
	Pos
	expr()
}

// Pos interface provies two functions to get/set the position for expression or statement.
type Pos interface {
	Position() Position
	SetPosition(Position)
}

// Position provides interface to store code locations.
type Position struct {
	Line   int
	Column int
}

// PosImpl provies commonly implementations for Pos.
type PosImpl struct {
	pos Position
}

// Position return the position of the expression or statement.
func (x *PosImpl) Position() Position {
	return x.pos
}

// SetPosition is a function to specify position of the expression or statement.
func (x *PosImpl) SetPosition(pos Position) {
	x.pos = pos
}

// StmtImpl provide commonly implementations for Stmt..
type StmtImpl struct {
	PosImpl // StmtImpl provide Pos() function.
}

// stmt provide restraint interface.
func (x *StmtImpl) stmt() {}

// ExprStmt provide expression statement.
type ExprStmt struct {
	StmtImpl
	Expr Expr
}

// ExprImpl provide commonly implementations for Expr.
type ExprImpl struct {
	PosImpl // provide Pos() function.
}

// expr provide restraint interface.
func (x *ExprImpl) expr() {}

// FuncExpr provide function expression.
type FuncExpr struct {
	ExprImpl
	Name   string
	Stmts  []Stmt
	Args   []string
	VarArg bool
}

func (x *FuncExpr) stmt() {}

func NewFuncExpr(name string, v ...string) FuncExpr {
	fe := FuncExpr{}
	pi := PosImpl{}
	pi.SetPosition(Position{0, 0})
	fe.ExprImpl = ExprImpl{PosImpl: pi}
	fe.Name = name
	fe.Args = v
	return fe
}

// CallExpr provide calling expression.
type CallExpr struct {
	ExprImpl
	Func     interface{}
	Name     string
	SubExprs []Expr
}
