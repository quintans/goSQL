package translators

import (
	"github.com/quintans/goSQL/db"
	tk "github.com/quintans/toolkit"

	"fmt"
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
	this.UpdateProcessorFactory = func() UpdateProcessor { return NewPgUpdateBuilder(this) }
	this.DeleteProcessorFactory = func() DeleteProcessor { return NewMySQL5DeleteBuilder(this) }

	this.resgisterTranslations()

	return this
}

func (this *MySQL5Translator) resgisterTranslations() {
	this.RegisterTranslation(db.TOKEN_COALESCE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("COALESCE(%s)", RolloverParameter(dmlType, tx, m, ", "))
	})
}

func NewMySQL5DeleteBuilder(translator db.Translator) *MySQL5DeleteBuilder {
	this := new(MySQL5DeleteBuilder)
	this.Super(translator)
	return this
}

type MySQL5DeleteBuilder struct {
	DeleteBuilder
}

func (this *MySQL5DeleteBuilder) From(del *db.Delete) {
	table := del.GetTable()
	alias := del.GetTableAlias()
	// Multiple-table syntax:
	this.tablePart.AddAsOne(alias, " USING ", this.translator.TableName(table), " AS ", alias)
}

func (this *MySQL5Translator) GetAutoKeyStrategy() db.AutoKeyStrategy {
	return db.AUTOKEY_AFTER
}

func (this *MySQL5Translator) GetAutoNumberQuery(column *db.Column) string {
	return "select LAST_INSERT_ID()"
}

func (this *MySQL5Translator) TableName(table *db.Table) string {
	return "`" + strings.ToLower(table.GetName()) + "`"
}

func (this *MySQL5Translator) ColumnName(column *db.Column) string {
	return "`" + strings.ToLower(column.GetName()) + "`"
}

func (this *MySQL5Translator) PaginateSQL(query *db.Query, sql string) string {
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
