package translators

import (
	"github.com/quintans/goSQL/db"
	tk "github.com/quintans/toolkit"

	"strconv"
	"strings"
)

type PostgreSQLTranslator struct {
	*GenericTranslator
}

var _ db.Translator = &PostgreSQLTranslator{}

func NewPostgreSQLTranslator() *PostgreSQLTranslator {
	this := new(PostgreSQLTranslator)
	this.GenericTranslator = new(GenericTranslator)
	this.Init(this)
	this.QueryProcessorFactory = func() QueryProcessor { return NewQueryBuilder(this) }
	this.InsertProcessorFactory = func() InsertProcessor { return NewInsertBuilder(this) }
	this.UpdateProcessorFactory = func() UpdateProcessor { return NewPgUpdateBuilder(this) }
	this.DeleteProcessorFactory = func() DeleteProcessor { return NewDeleteBuilder(this) }
	return this
}

func (o *PostgreSQLTranslator) GetAutoKeyStrategy() db.AutoKeyStrategy {
	return db.AUTOKEY_RETURNING
}

func (o *PostgreSQLTranslator) GetPlaceholder(index int, name string) string {
	return "$" + strconv.Itoa(index+1)
}

// INSERT
func (o *PostgreSQLTranslator) GetSqlForInsert(insert *db.Insert) string {
	// insert generated by super
	sql := o.GenericTranslator.GetSqlForInsert(insert)

	// only ONE numeric id is allowed
	// if no value was defined for the key, it is assumed an auto number,
	// otherwise is a guid (or something else)
	singleKeyColumn := insert.GetTable().GetSingleKeyColumn()
	if !insert.HasKeyValue && singleKeyColumn != nil {
		str := tk.NewStrBuffer()
		str.Add(sql, " RETURNING ", o.overrider.ColumnName(singleKeyColumn))
		sql = str.String()
	}

	return sql
}

func (o *PostgreSQLTranslator) TableName(table *db.Table) string {
	return strings.ToLower(table.GetName())
}

func (o *PostgreSQLTranslator) ColumnName(column *db.Column) string {
	return strings.ToLower(column.GetName())
}

//// UPDATE

type PgUpdateBuilder struct {
	UpdateBuilder
}

func NewPgUpdateBuilder(translator db.Translator) *PgUpdateBuilder {
	this := new(PgUpdateBuilder)
	this.Super(translator)
	return this
}

func (p *PgUpdateBuilder) Column(update *db.Update) error {
	values := update.GetValues()
	for it := values.Iterator(); it.HasNext(); {
		entry := it.Next()
		column := entry.Key.(*db.Column)
		// use only not virtual columns
		token := entry.Value.(db.Tokener)
		s, err := p.translator.Translate(db.UPDATE, token)
		if err != nil {
			return err
		}
		p.columnPart.AddAsOne(
			p.translator.ColumnName(column),
			" = ", s)
	}
	return nil
}

func (p *PostgreSQLTranslator) PaginateSQL(query *db.Query, sql string) string {
	sb := tk.NewStrBuffer()
	if query.GetLimit() > 0 {
		sb.Add(sql, " LIMIT :", db.LIMIT_PARAM)
		query.SetParameter(db.LIMIT_PARAM, query.GetLimit())
		if query.GetSkip() > 0 {
			sb.Add(" OFFSET :", db.OFFSET_PARAM)
			query.SetParameter(db.OFFSET_PARAM, query.GetSkip())
		}
		return sb.String()
	}

	return sql
}