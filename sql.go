package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

var SafeSqlFunc = []string{
	"SUM", "AVG", "MAX", "MIN", "COUNT", "CONCAT", "SUBSTRING", "CHAR_LENGTH",
	"LOWER", "UPPER", "NOW", "DATE_FORMAT", "DATE_ADD", "DATEDIFF", "ABS",
	"CEIL", "FLOOR", "EXP", "LOG", "AND", "OR", "NOT", "CAST", "CONVERT",
	"COALESCE", "NULLIF",
}

type Record struct {
	Msg string `json:"msg"`
}

func CheckSQL(ctx context.Context, sqlQuery string) (bool, string, error) {
	stmt, err := sqlparser.Parse(sqlQuery)
	if err != nil {
		return false, err.Error(), err
	}

	r := &Record{}
	return checkStmt(ctx, stmt, r), r.Msg, nil
}

func checkStmt(ctx context.Context, stmt sqlparser.Statement, r *Record) bool {
	if stmt == nil {
		return true
	}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		for _, i := range stmt.SelectExprs {
			if !checkSelectExpr(ctx, i, r) {
				return false
			}
		}

		for _, i := range stmt.From {
			if !checkTableExpr(ctx, i, r) {
				return false
			}
		}

		if stmt.Where != nil {
			if !checkExpr(ctx, stmt.Where.Expr, r) {
				return false
			}
		}

		for _, g := range stmt.GroupBy {
			if !checkExpr(ctx, g, r) {
				return false
			}
		}

		if stmt.Having != nil {
			if !checkExpr(ctx, stmt.Having.Expr, r) {
				return false
			}
		}

		for _, o := range stmt.OrderBy {
			if o == nil {
				continue
			}
			if !checkExpr(ctx, o.Expr, r) {
				return false
			}
		}

		if stmt.Limit != nil {
			if !checkExpr(ctx, stmt.Limit.Offset, r) {
				return false
			}

			if !checkExpr(ctx, stmt.Limit.Rowcount, r) {
				return false
			}
		}
	default:
		r.Msg = "bad stmt operation"
		return false
	}

	return true
}

func checkTableExpr(ctx context.Context, expr sqlparser.TableExpr, r *Record) bool {
	if expr == nil {
		return true
	}

	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return checkSimpleTableExpr(ctx, expr.Expr, r)
	case *sqlparser.ParenTableExpr:
		for _, e := range expr.Exprs {
			if !checkTableExpr(ctx, e, r) {
				return false
			}
		}
		return true
	case *sqlparser.JoinTableExpr:
		if !checkTableExpr(ctx, expr.LeftExpr, r) {
			return false
		}

		if !checkTableExpr(ctx, expr.RightExpr, r) {
			return false
		}

		if !checkExpr(ctx, expr.Condition.On, r) {
			return false
		}
		return true
	}

	r.Msg = "bad table expr"
	return false
}

func checkExpr(ctx context.Context, expr sqlparser.Expr, r *Record) bool {
	if expr == nil {
		return true
	}

	switch expr := expr.(type) {
	default:
		return false
	case *sqlparser.AndExpr:
		if !checkExpr(ctx, expr.Left, r) {
			return false
		}
		if !checkExpr(ctx, expr.Right, r) {
			return false
		}
	case *sqlparser.OrExpr:
		if !checkExpr(ctx, expr.Left, r) {
			return false
		}
		if !checkExpr(ctx, expr.Right, r) {
			return false
		}
	case *sqlparser.NotExpr:
		if !checkExpr(ctx, expr.Expr, r) {
			return false
		}
	case *sqlparser.ParenExpr:
		if !checkExpr(ctx, expr.Expr, r) {
			return false
		}
	case *sqlparser.ComparisonExpr:
		if !checkExpr(ctx, expr.Left, r) {
			return false
		}
		if !checkExpr(ctx, expr.Right, r) {
			return false
		}
		if !checkExpr(ctx, expr.Escape, r) {
			return false
		}
	case *sqlparser.RangeCond:
		if !checkExpr(ctx, expr.Left, r) {
			return false
		}
		if !checkExpr(ctx, expr.From, r) {
			return false
		}
		if !checkExpr(ctx, expr.To, r) {
			return false
		}
	case *sqlparser.IsExpr:
		if !checkExpr(ctx, expr.Expr, r) {
			return false
		}
	case *sqlparser.ExistsExpr:
		if !checkExpr(ctx, expr.Subquery, r) {
			return false
		}
	case *sqlparser.SQLVal:
		// 可以
	case *sqlparser.NullVal:
		// 可以
	case sqlparser.BoolVal:
		// 可以
	case *sqlparser.ColName:
		// 检查ColName
	case sqlparser.ValTuple:
		for _, e := range expr {
			if !checkExpr(ctx, e, r) {
				return false
			}
		}
	case *sqlparser.Subquery:
		if !checkStmt(ctx, expr.Select, r) {
			return false
		}
	case sqlparser.ListArg:
		// 可以
	case *sqlparser.BinaryExpr:
		if !checkExpr(ctx, expr.Left, r) {
			return false
		}
		if !checkExpr(ctx, expr.Right, r) {
			return false
		}
	case *sqlparser.UnaryExpr:
		if !checkExpr(ctx, expr.Expr, r) {
			return false
		}
	case *sqlparser.IntervalExpr:
		if !checkExpr(ctx, expr.Expr, r) {
			return false
		}
	case *sqlparser.CollateExpr:
		if !checkExpr(ctx, expr.Expr, r) {
			return false
		}
	case *sqlparser.FuncExpr:
		if !checkFuncName(ctx, expr.Qualifier.String(), expr.Name.String(), r) {
			return false
		}

		for _, s := range expr.Exprs {
			if !checkSelectExpr(ctx, s, r) {
				return false
			}
		}
	case *sqlparser.CaseExpr:
		if !checkExpr(ctx, expr.Expr, r) {
			return false
		}
		if !checkExpr(ctx, expr.Else, r) {
			return false
		}
		for _, w := range expr.Whens {
			if w == nil {
				continue
			}
			if !checkExpr(ctx, w.Cond, r) {
				return false
			}
			if !checkExpr(ctx, w.Val, r) {
				return false
			}
		}
	case *sqlparser.ValuesFuncExpr:
		r.Msg = "bad values expr"
		return false // values表达式用于插入
	case *sqlparser.ConvertExpr:
		if !checkExpr(ctx, expr.Expr, r) {
			return false
		}
	case *sqlparser.SubstrExpr:
		if !checkExpr(ctx, expr.From, r) {
			return false
		}
		if !checkExpr(ctx, expr.To, r) {
			return false
		}
		if !checkColName(ctx, expr.Name, r) {
			return false
		}
	case *sqlparser.ConvertUsingExpr:
		if !checkExpr(ctx, expr.Expr, r) {
			return false
		}
	case *sqlparser.MatchExpr:
		if !checkExpr(ctx, expr.Expr, r) {
			return false
		}
		for _, s := range expr.Columns {
			if !checkSelectExpr(ctx, s, r) {
				return false
			}
		}
	case *sqlparser.GroupConcatExpr:
		for _, s := range expr.Exprs {
			if !checkSelectExpr(ctx, s, r) {
				return false
			}
		}
		for _, o := range expr.OrderBy {
			if o == nil {
				continue
			}
			if !checkExpr(ctx, o.Expr, r) {
				return false
			}
		}
	case *sqlparser.Default:
		if !checkColNameString(ctx, expr.ColName, r) {
			return false
		}
	}

	return true
}

func checkSimpleTableExpr(ctx context.Context, expr sqlparser.SimpleTableExpr, r *Record) bool {
	if expr == nil {
		return true
	}

	switch expr := expr.(type) {
	case sqlparser.TableName:
		return checkTableName(ctx, expr, r)
	case *sqlparser.Subquery:
		return checkStmt(ctx, expr.Select, r)
	}

	r.Msg = "bad simple table expr"
	return false
}

func checkSelectExpr(ctx context.Context, expr sqlparser.SelectExpr, r *Record) bool {
	if expr == nil {
		return true
	}

	switch expr := expr.(type) {
	case *sqlparser.StarExpr:
		return checkTableName(ctx, expr.TableName, r)
	case *sqlparser.AliasedExpr:
		return checkExpr(ctx, expr.Expr, r)
	case sqlparser.Nextval:
		return checkExpr(ctx, expr.Expr, r)
	}

	r.Msg = "bad select expr"
	return false
}

func checkTableName(ctx context.Context, tableName sqlparser.TableName, r *Record) bool {
	if tableName.IsEmpty() {
		return true
	}

	allowTableName, ok := ctx.Value("Allow-Table-Name").([]string)
	if ok && !tableName.Name.IsEmpty() && !InList(allowTableName, tableName.Name.String()) {
		r.Msg = fmt.Sprintf("bad table name for %s", tableName.Name.String())
		return false
	}

	allowDBName, ok := ctx.Value("Allow-DataBase-Name").([]string)
	if ok {
		if !tableName.Qualifier.IsEmpty() && !InList(allowDBName, tableName.Qualifier.String()) {
			r.Msg = fmt.Sprintf("bad table qualifier for %s", tableName.Qualifier.String())
			return false
		}
	} else {
		if !tableName.Qualifier.IsEmpty() {
			r.Msg = fmt.Sprintf("bad table qualifier for %s", tableName.Qualifier.String())
			return false
		}
	}

	return true
}

func checkColName(ctx context.Context, colname *sqlparser.ColName, r *Record) bool {
	if !checkTableName(ctx, colname.Qualifier, r) {
		return false
	}

	return checkColNameString(ctx, colname.Name.String(), r)
}

func checkColNameString(ctx context.Context, colname string, r *Record) bool {
	if len(colname) == 0 {
		return true
	}

	allowColName, ok := ctx.Value("Allow-Col-Name").([]string)
	if ok && !InList(allowColName, colname) && !InList(allowColName, fmt.Sprintf("`%s`", colname)) {
		r.Msg = fmt.Sprintf("bad table col for %s", colname)
		return false
	}

	return true
}

func checkFuncName(ctx context.Context, ident string, funcName string, r *Record) bool {
	if len(funcName) == 0 && len(ident) == 0 {
		return false
	}

	if len(ident) != 0 {
		allowColIdent, ok := ctx.Value("Allow-Func-Ident").([]string)
		if ok && !InList(allowColIdent, ident) {
			r.Msg = fmt.Sprintf("bad func ident for %s", ident)
			return false
		}
	}

	allowColName, ok := ctx.Value("Allow-Func-Name").([]string)
	if ok && !InList(allowColName, strings.ToUpper(funcName)) {
		r.Msg = fmt.Sprintf("bad func name for %s", funcName)
		return false
	} else if !ok {
		r.Msg = fmt.Sprintf("bad func name for %s", funcName)
		return false
	}

	return true
}

func InList[T string | int64](lst []T, element T) bool {
	for _, i := range lst {
		if i == element {
			return true
		}
	}

	return false
}
