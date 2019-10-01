package translators

import (
	"strconv"

	"github.com/quintans/goSQL/db"

	"fmt"
	"strings"
)

type OracleTranslator struct {
	*GenericTranslator
}

var _ db.Translator = &OracleTranslator{}

func NewOracleTranslator() *OracleTranslator {
	this := new(OracleTranslator)
	this.GenericTranslator = new(GenericTranslator)
	this.Init(this)
	this.QueryProcessorFactory = func() QueryProcessor { return NewQueryBuilder(this) }
	this.InsertProcessorFactory = func() InsertProcessor { return NewInsertBuilder(this) }
	this.UpdateProcessorFactory = func() UpdateProcessor { return NewUpdateBuilder(this) }
	this.DeleteProcessorFactory = func() DeleteProcessor { return NewDeleteBuilder(this) }
	return this
}

func (this *OracleTranslator) GetAutoKeyStrategy() db.AutoKeyStrategy {
	return db.AUTOKEY_BEFORE
}

func (this *OracleTranslator) GetAutoNumberQuery(column *db.Column) string {
	return "select " + strings.ToUpper(column.GetTable().GetName()) + "_SEQ.nextval from dual"
}

func (this *OracleTranslator) TableName(table *db.Table) string {
	return "\"" + strings.ToUpper(table.GetName()) + "\""
}

func (this *OracleTranslator) ColumnName(column *db.Column) string {
	return "\"" + strings.ToUpper(column.GetName()) + "\""
}

func (this *OracleTranslator) PaginateSQL(query *db.Query, sql string) string {
	if query.GetSkip() > 0 {
		query.SetParameter(db.OFFSET_PARAM, query.GetSkip()+1)
		query.SetParameter(db.LIMIT_PARAM, query.GetSkip()+query.GetLimit())
		return fmt.Sprintf("select * from ( select a.*, rownum rnum from ( %s ) a where rownum <= :%s ) where rnum >= :%s",
			sql, db.LIMIT_PARAM, db.OFFSET_PARAM)
	} else if query.GetLimit() > 0 {
		query.SetParameter(db.LIMIT_PARAM, query.GetLimit())
		return fmt.Sprintf("select * from ( %s ) where rownum <= :%s", sql, db.LIMIT_PARAM)
	}

	return sql
}

func (this *OracleTranslator) GetPlaceholder(index int, name string) string {
	return ":" + strconv.Itoa(index+1)
}
