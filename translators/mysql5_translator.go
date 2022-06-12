package translators

import (
	"github.com/quintans/goSQL/db"
	tk "github.com/quintans/toolkit"

	"strings"
)

type MySQL5Translator struct {
	*GenericTranslator
}

var _ db.Translator = &MySQL5Translator{}

func NewMySQL5Translator() *MySQL5Translator {
	this := new(MySQL5Translator)
	this.GenericTranslator = new(GenericTranslator)
	this.Init(this)
	this.QueryProcessorFactory = func() QueryProcessor { return NewQueryBuilder(this) }
	this.InsertProcessorFactory = func() InsertProcessor { return NewInsertBuilder(this) }
	this.UpdateProcessorFactory = func() UpdateProcessor { return NewUpdateBuilder(this) }
	this.DeleteProcessorFactory = func() DeleteProcessor { return NewMySQL5DeleteBuilder(this) }

	return this
}

func NewMySQL5DeleteBuilder(translator db.Translator) *MySQL5DeleteBuilder {
	this := new(MySQL5DeleteBuilder)
	this.Super(translator)
	return this
}

type MySQL5DeleteBuilder struct {
	DeleteBuilder
}

func (m *MySQL5DeleteBuilder) From(del *db.Delete) error {
	table := del.GetTable()
	alias := del.GetTableAlias()
	// Multiple-table syntax:
	m.tablePart.AddAsOne(alias, " USING ", m.translator.TableName(table), " AS ", alias)
	return nil
}

func (m *MySQL5Translator) GetAutoKeyStrategy() db.AutoKeyStrategy {
	return db.AUTOKEY_AFTER
}

func (m *MySQL5Translator) GetAutoNumberQuery(column *db.Column) string {
	return "select LAST_INSERT_ID()"
}

func (m *MySQL5Translator) TableName(table *db.Table) string {
	return "`" + strings.ToUpper(table.GetName()) + "`"
}

func (m *MySQL5Translator) ColumnName(column *db.Column) string {
	return "`" + strings.ToUpper(column.GetName()) + "`"
}

func (m *MySQL5Translator) PaginateSQL(query *db.Query, sql string) string {
	sb := tk.NewStrBuffer()
	if query.GetLimit() > 0 {
		sb.Add(sql, " LIMIT :", db.OFFSET_PARAM, ", :", db.LIMIT_PARAM)
		if query.GetSkip() >= 0 {
			query.SetParameter(db.OFFSET_PARAM, query.GetSkip())
		}
		query.SetParameter(db.LIMIT_PARAM, query.GetLimit())
		return sb.String()
	}

	return sql
}
