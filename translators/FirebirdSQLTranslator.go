package translators

import (
	"strings"

	"github.com/quintans/goSQL/db"
	tk "github.com/quintans/toolkit"
)

type FirebirdSQLTranslator struct {
	*GenericTranslator
}

var _ db.Translator = &FirebirdSQLTranslator{}

func NewFirebirdSQLTranslator() *FirebirdSQLTranslator {
	this := new(FirebirdSQLTranslator)
	this.GenericTranslator = new(GenericTranslator)
	this.Init(this)
	this.QueryProcessorFactory = func() QueryProcessor { return NewQueryBuilder(this) }
	this.InsertProcessorFactory = func() InsertProcessor { return NewInsertBuilder(this) }
	this.UpdateProcessorFactory = func() UpdateProcessor { return NewUpdateBuilder(this) }
	this.DeleteProcessorFactory = func() DeleteProcessor { return NewDeleteBuilder(this) }
	return this
}

func (f *FirebirdSQLTranslator) GetAutoKeyStrategy() db.AutoKeyStrategy {
	return db.AUTOKEY_BEFORE
}

func (f *FirebirdSQLTranslator) GetAutoNumberQuery(column *db.Column) string {
	return "select GEN_ID(" + column.GetTable().GetName() + "_GEN, 1) from RDB$DATABASE"
}

// INSERT
// 2013-06-15: available odbc drivers do not implement RETURNING

func (f *FirebirdSQLTranslator) TableName(table *db.Table) string {
	return "\"" + strings.ToUpper(table.GetName()) + "\""
}

func (f *FirebirdSQLTranslator) ColumnName(column *db.Column) string {
	return "\"" + strings.ToUpper(column.GetName()) + "\""
}

func (f *FirebirdSQLTranslator) PaginateSQL(query *db.Query, sql string) string {
	sb := tk.NewStrBuffer()
	if query.GetLimit() > 0 {
		sb.Add(sql, " ROWS ")
		if query.GetSkip() > 0 {
			sb.Add(":", db.OFFSET_PARAM, " TO ")
			query.SetParameter(db.OFFSET_PARAM, query.GetSkip()+1)
		}
		sb.Add(":", db.LIMIT_PARAM)
		query.SetParameter(db.LIMIT_PARAM, query.GetSkip()+query.GetLimit())

		return sb.String()
	}

	return sql
}
