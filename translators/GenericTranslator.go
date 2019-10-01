package translators

import (
	"github.com/quintans/goSQL/db"
	tk "github.com/quintans/toolkit"

	"fmt"
	"strconv"
)

type IJoiner interface {
	JoinAssociation(fk *db.Association, inner bool)
	JoinCriteria(criteria *db.Criteria)
	JoinPart() string
}

/*
 * =============
 * QueryBuilder
 * =============
 */

type QueryProcessor interface {
	IJoiner

	Column(query *db.Query)
	From(query *db.Query)
	FromSubQuery(query *db.Query)
	Where(query *db.Query)
	WherePart() string
	Group(query *db.Query)
	Having(query *db.Query)
	Order(query *db.Query)
	Union(query *db.Query)
	ColumnPart() string
	FromPart() string
	GroupPart() string
	HavingPart() string
	OrderPart() string
	UnionPart() string
}

type QueryBuilder struct {
	translator db.Translator
	columnPart *tk.Joiner
	fromPart   *tk.Joiner
	joinPart   *tk.StrBuffer
	wherePart  *tk.Joiner
	groupPart  *tk.Joiner
	havingPart *tk.StrBuffer
	orderPart  *tk.Joiner
	unionPart  *tk.StrBuffer
}

func NewQueryBuilder(translator db.Translator) *QueryBuilder {
	this := new(QueryBuilder)
	this.Super(translator)
	return this
}

func (this *QueryBuilder) Super(translator db.Translator) {
	this.translator = translator
	this.columnPart = tk.NewJoiner(", ")
	this.fromPart = tk.NewJoiner(", ")
	this.joinPart = tk.NewStrBuffer()
	this.wherePart = tk.NewJoiner(" AND ")
	this.groupPart = tk.NewJoiner(", ")
	this.havingPart = tk.NewStrBuffer()
	this.orderPart = tk.NewJoiner(", ")
	this.unionPart = tk.NewStrBuffer()

}

func (this *QueryBuilder) ColumnPart() string {
	return this.columnPart.String()
}

func (this *QueryBuilder) FromPart() string {
	return this.fromPart.String()
}

func (this *QueryBuilder) JoinPart() string {
	return this.joinPart.String()
}

func (this *QueryBuilder) WherePart() string {
	return this.wherePart.String()
}

func (this *QueryBuilder) GroupPart() string {
	return this.groupPart.String()
}

func (this *QueryBuilder) HavingPart() string {
	return this.havingPart.String()
}

func (this *QueryBuilder) OrderPart() string {
	return this.orderPart.String()
}

func (this *QueryBuilder) UnionPart() string {
	return this.unionPart.String()
}

func (this *QueryBuilder) Column(query *db.Query) {
	for k, token := range query.Columns {
		this.columnPart.Add(this.translator.Translate(db.QUERY, token))
		a := this.translator.ColumnAlias(token, k+1)
		if a != "" {
			this.columnPart.Append(" AS ", a)
		}
	}
}

func (this *QueryBuilder) From(query *db.Query) {
	table := query.GetTable()
	alias := query.GetTableAlias()
	this.fromPart.AddAsOne(this.translator.TableName(table), " ", alias)
}

func (this *QueryBuilder) FromSubQuery(query *db.Query) {
	subquery := query.GetSubQuery()
	alias := query.GetSubQueryAlias()
	this.fromPart.AddAsOne("(", this.translator.GetSqlForQuery(subquery), ")")
	if alias != "" {
		this.fromPart.Append(" ", alias)
	}
}

func (this *QueryBuilder) JoinAssociation(fk *db.Association, inner bool) {
	if inner {
		this.joinPart.Add(" INNER JOIN ")
	} else {
		this.joinPart.Add(" LEFT OUTER JOIN ")
	}

	this.joinPart.Add(this.translator.TableName(fk.GetTableTo()), " ", fk.GetAliasTo(), " ON ")

	for i, rel := range fk.GetRelations() {
		if i > 0 {
			this.joinPart.Add(" AND ")
		}
		this.joinPart.Add(this.translator.Translate(db.QUERY, rel.From),
			" = ",
			this.translator.Translate(db.QUERY, rel.To))
	}
}

func (this *QueryBuilder) JoinCriteria(criteria *db.Criteria) {
	this.joinPart.Add(" AND ", this.translator.Translate(db.QUERY, criteria))
}

func (this *QueryBuilder) Where(query *db.Query) {
	criteria := query.GetCriteria()
	if criteria != nil {
		this.wherePart.Add(this.translator.Translate(db.QUERY, criteria))
	}
}

func (this *QueryBuilder) Group(query *db.Query) {
	groups := query.GetGroupByTokens()
	for _, group := range groups {
		//this.groupPart.Add(this.translator.ColumnAlias(group.Token, group.Position))
		this.groupPart.Add(this.translator.Translate(db.QUERY, group.Token))
	}
}

func (this *QueryBuilder) Having(query *db.Query) {
	having := query.GetHaving()
	if having != nil {
		this.havingPart.Add(this.translator.Translate(db.QUERY, having))
	}
}

func (this *QueryBuilder) Order(query *db.Query) {
	orders := query.GetOrders()
	for _, ord := range orders {
		if ord.GetHolder() != nil {
			this.orderPart.Add(this.translator.Translate(db.QUERY, ord.GetHolder()))
		} else {
			this.orderPart.Add(ord.GetAlias())
		}

		if ord.IsAsc() {
			this.orderPart.Append(" ASC")
		} else {
			this.orderPart.Append(" DESC")
		}
	}
}

func (this *QueryBuilder) Union(query *db.Query) {
	unions := query.GetUnions()
	for _, u := range unions {
		this.unionPart.Add(" UNION ")
		if u.All {
			this.unionPart.Add("ALL ")
		}
		this.unionPart.Add(this.translator.GetSqlForQuery(u.Query))
	}
}

/*
 * =============
 * UpdateBuilder
 * =============
 */

type UpdateProcessor interface {
	Column(update *db.Update)
	From(update *db.Update)
	ColumnPart() string
	TablePart() string
	Where(update *db.Update)
	WherePart() string
}

type UpdateBuilder struct {
	translator db.Translator
	columnPart *tk.Joiner
	tablePart  *tk.Joiner
	wherePart  *tk.Joiner
}

func NewUpdateBuilder(translator db.Translator) *UpdateBuilder {
	this := new(UpdateBuilder)
	this.Super(translator)
	return this
}

func (this *UpdateBuilder) Super(translator db.Translator) {
	this.translator = translator
	this.columnPart = tk.NewJoiner(", ")
	this.tablePart = tk.NewJoiner(", ")
	this.wherePart = tk.NewJoiner(" AND ")

}

func (this *UpdateBuilder) ColumnPart() string {
	return this.columnPart.String()
}

func (this *UpdateBuilder) TablePart() string {
	return this.tablePart.String()
}

func (this *UpdateBuilder) WherePart() string {
	return this.wherePart.String()
}

func (this *UpdateBuilder) Column(update *db.Update) {
	values := update.GetValues()
	tableAlias := update.GetTableAlias()
	for it := values.Iterator(); it.HasNext(); {
		entry := it.Next()
		column := entry.Key.(*db.Column)
		// use only not virtual columns
		token := entry.Value.(db.Tokener)
		this.columnPart.AddAsOne(tableAlias, ".",
			this.translator.ColumnName(column),
			" = ", this.translator.Translate(db.UPDATE, token))
	}
}

func (this *UpdateBuilder) From(update *db.Update) {
	table := update.GetTable()
	alias := update.GetTableAlias()
	this.tablePart.AddAsOne(this.translator.TableName(table), " ", alias)
}

func (this *UpdateBuilder) Where(update *db.Update) {
	criteria := update.GetCriteria()
	if criteria != nil {
		this.wherePart.Add(this.translator.Translate(db.UPDATE, criteria))
	}
}

/*
 * =============
 * DeleteBuilder
 * =============
 */

type DeleteProcessor interface {
	From(del *db.Delete)
	TablePart() string
	Where(del *db.Delete)
	WherePart() string
}

type DeleteBuilder struct {
	translator db.Translator
	tablePart  *tk.Joiner
	wherePart  *tk.Joiner
}

func NewDeleteBuilder(translator db.Translator) *DeleteBuilder {
	this := new(DeleteBuilder)
	this.Super(translator)
	return this
}

func (this *DeleteBuilder) Super(translator db.Translator) {
	this.translator = translator

	this.tablePart = tk.NewJoiner(", ")
	this.wherePart = tk.NewJoiner(" AND ")
}

func (this *DeleteBuilder) TablePart() string {
	return this.tablePart.String()
}

func (this *DeleteBuilder) WherePart() string {
	return this.wherePart.String()
}

func (this *DeleteBuilder) From(del *db.Delete) {
	table := del.GetTable()
	alias := del.GetTableAlias()
	this.tablePart.AddAsOne(this.translator.TableName(table), " ", alias)
}

func (this *DeleteBuilder) Where(del *db.Delete) {
	criteria := del.GetCriteria()
	if criteria != nil {
		this.wherePart.Add(this.translator.Translate(db.DELETE, criteria))
	}
}

/*
 * =============
 * InsertBuilder
 * =============
 */

type InsertProcessor interface {
	Column(insert *db.Insert)
	From(insert *db.Insert)
	ColumnPart() string
	ValuePart() string
	TablePart() string
}

type InsertBuilder struct {
	translator db.Translator
	columnPart *tk.Joiner
	valuePart  *tk.Joiner
	tablePart  *tk.Joiner
}

func NewInsertBuilder(translator db.Translator) *InsertBuilder {
	this := new(InsertBuilder)
	this.Super(translator)
	return this
}

func (this *InsertBuilder) Super(translator db.Translator) {
	this.translator = translator
	this.columnPart = tk.NewJoiner(", ")
	this.valuePart = tk.NewJoiner(", ")
	this.tablePart = tk.NewJoiner(", ")
}

func (this *InsertBuilder) ColumnPart() string {
	return this.columnPart.String()
}

func (this *InsertBuilder) ValuePart() string {
	return this.valuePart.String()
}

func (this *InsertBuilder) TablePart() string {
	return this.tablePart.String()
}

func (this *InsertBuilder) Column(insert *db.Insert) {
	values := insert.GetValues()
	parameters := insert.GetParameters()
	var val string
	for it := values.Iterator(); it.HasNext(); {
		entry := it.Next()
		column := entry.Key.(*db.Column)
		// use only not virtual columns
		token := entry.Value.(db.Tokener)
		// only includes null keys if IgnoreNullKeys is false
		if column.IsKey() && this.translator.IgnoreNullKeys() &&
			db.TOKEN_PARAM == token.GetOperator() {
			param := token.GetValue().(string)
			if parameters[param] != nil {
				val = this.translator.Translate(db.INSERT, token)
			}
		} else {
			val = this.translator.Translate(db.INSERT, token)
		}

		col := this.translator.ColumnName(column)

		if val != "" {
			this.columnPart.Add(col)
			this.valuePart.Add(val)
		}

		val = ""
	}
}

func (this *InsertBuilder) From(insert *db.Insert) {
	table := insert.GetTable()
	this.tablePart.Add(this.translator.TableName(table))
}

/*
 * =================
 * GenericTranslator
 * =================
 */

type GenericTranslator struct {
	tokens                 map[string]func(dmlType db.DmlType, token db.Tokener, translator db.Translator) string
	overrider              db.Translator
	QueryProcessorFactory  func() QueryProcessor
	InsertProcessorFactory func() InsertProcessor
	UpdateProcessorFactory func() UpdateProcessor
	DeleteProcessorFactory func() DeleteProcessor
}

func RolloverParameter(dmlType db.DmlType, tx db.Translator, parameters []db.Tokener, separator string) string {
	sb := tk.NewStrBuffer()
	for f, p := range parameters {
		if f > 0 && separator != "" {
			sb.Add(separator)
		}
		sb.Add(tx.Translate(dmlType, p))
	}
	return sb.String()
}

func (this *GenericTranslator) Init(overrider db.Translator) {
	this.overrider = overrider
	this.tokens = make(map[string]func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string)

	// Column
	this.RegisterTranslation(db.TOKEN_COLUMN, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		if col, ok := token.GetValue().(*db.Column); ok {
			sb := tk.NewStrBuffer()
			if token.GetTableAlias() != "" {
				sb.Add(token.GetTableAlias())
			} else {
				sb.Add(tx.TableName(col.GetTable()))
			}
			sb.Add(".", tx.ColumnName(col))
			return sb.String()
		}

		return ""
	})

	// Match
	this.RegisterTranslation(db.TOKEN_EQ, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		o := token.GetMembers()
		return tx.Translate(dmlType, o[0]) + " = " + tx.Translate(dmlType, o[1])
	})

	// Match
	this.RegisterTranslation(db.TOKEN_NULL, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		return "NULL"
	})

	// Val
	handle := func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		o := token.GetValue()
		if o != nil {
			if s, ok := o.(string); ok {
				return "'" + s + "'"
			} else {
				return fmt.Sprint(o)
			}
		}
		return "NULL"
	}
	this.RegisterTranslation(db.TOKEN_RAW, handle)
	this.RegisterTranslation(db.TOKEN_ASIS, handle)

	this.RegisterTranslation(db.TOKEN_IEQ, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("UPPER(%s) = UPPER(%s)", tx.Translate(dmlType, m[0]), tx.Translate(dmlType, m[1]))
	})

	// Diferent
	this.RegisterTranslation(db.TOKEN_NEQ, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("%s <> %s", tx.Translate(dmlType, m[0]), tx.Translate(dmlType, m[1]))
	})

	this.RegisterTranslation(db.TOKEN_RANGE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		field := tx.Translate(dmlType, m[0])
		var bottom string
		var top string
		if m[1] != nil {
			bottom = tx.Translate(dmlType, m[1])
		}
		if m[2] != nil {
			top = tx.Translate(dmlType, m[2])
		}

		if bottom != "" && top != "" {
			return fmt.Sprintf("%s >= %s AND %s <= %s", field, bottom, field, top)
		} else if bottom != "" {
			return fmt.Sprintf("%s >= %s", field, bottom)
		} else if top != "" {
			return fmt.Sprintf("%s <= %s", field, top)
		}
		panic("Invalid Range Token")

	})

	// ValueRange
	this.RegisterTranslation(db.TOKEN_VALUERANGE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		bottom := tx.Translate(dmlType, m[0])
		top := tx.Translate(dmlType, m[1])
		var value string
		if m[2] != nil {
			value = tx.Translate(dmlType, m[2])
		}

		if value != "" {
			return fmt.Sprintf(
				"(%1$s IS NULL AND %2$s IS NULL OR %1$s IS NULL AND %2$s <= %3$s OR %2$s IS NULL AND %1$s >= %3$s OR %1$s >= %3$s AND %2$s <= %3$s)",
				top, bottom, top, bottom, value, bottom, top, value, top, value, bottom, value,
			)
		}
		panic("Invalid ValueRange Token")
	})

	// boundedValueRange
	this.RegisterTranslation(db.TOKEN_BOUNDEDRANGE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		bottom := tx.Translate(dmlType, m[0])
		top := tx.Translate(dmlType, m[1])
		var value string
		if m[2] != nil {
			value = tx.Translate(dmlType, m[2])
		}

		if value != "" {
			return fmt.Sprintf("(%1$s >= %3$s AND %2$s <= %3$s)", top, value, bottom, value)
		}
		panic("Invalid BoundedRange Token")
	})

	// In
	this.RegisterTranslation(db.TOKEN_IN, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		var pattern string
		if token.GetOperator() == db.TOKEN_SUBQUERY {
			pattern = "%s%s IN %s"
		} else {
			pattern = "%s%s IN (%s)"
		}

		if c, ok := token.(*db.Criteria); ok {
			return fmt.Sprintf(
				pattern,
				this.isNot(c),
				tx.Translate(dmlType, m[0]),
				RolloverParameter(dmlType, tx, m[1:], ", "),
			)
		}
		return ""
	})

	// Or
	this.RegisterTranslation(db.TOKEN_OR, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("(%s)", RolloverParameter(dmlType, tx, m, " OR "))
	})

	// And
	this.RegisterTranslation(db.TOKEN_AND, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("%s", RolloverParameter(dmlType, tx, m, " AND "))
	})

	// Like
	this.RegisterTranslation(db.TOKEN_LIKE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		c := token.(*db.Criteria)
		m := token.GetMembers()
		return fmt.Sprintf("%s%s LIKE %s",
			tx.Translate(dmlType, m[0]), this.isNot(c), tx.Translate(dmlType, m[1]))
	})

	//	ILike
	this.RegisterTranslation(db.TOKEN_ILIKE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		c := token.(*db.Criteria)
		m := token.GetMembers()
		return fmt.Sprintf("UPPER(%s)%s LIKE UPPER(%s)",
			tx.Translate(dmlType, m[0]), this.isNot(c), tx.Translate(dmlType, m[1]))
	})

	// isNull
	this.RegisterTranslation(db.TOKEN_ISNULL, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		c := token.(*db.Criteria)
		m := token.GetMembers()
		return fmt.Sprintf("%s IS%s NULL", tx.Translate(dmlType, m[0]), this.isNot(c))
	})

	// Greater
	this.RegisterTranslation(db.TOKEN_GT, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("%s > %s", tx.Translate(dmlType, m[0]), tx.Translate(dmlType, m[1]))
	})

	// Lesser
	this.RegisterTranslation(db.TOKEN_LT, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("%s < %s", tx.Translate(dmlType, m[0]), tx.Translate(dmlType, m[1]))
	})

	// GreaterOrEqual
	this.RegisterTranslation(db.TOKEN_GTEQ, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("%s >= %s", tx.Translate(dmlType, m[0]), tx.Translate(dmlType, m[1]))
	})

	// LesserOrEqual
	this.RegisterTranslation(db.TOKEN_LTEQ, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("%s <= %s", tx.Translate(dmlType, m[0]), tx.Translate(dmlType, m[1]))
	})

	// FUNCTIONS
	// Param
	this.RegisterTranslation(db.TOKEN_PARAM, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		return fmt.Sprintf(":%s", token.GetValue())
	})

	// exists
	this.RegisterTranslation(db.TOKEN_EXISTS, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("EXISTS %s", tx.Translate(dmlType, m[0]))
	})

	this.RegisterTranslation(db.TOKEN_NOT, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("NOT %s", tx.Translate(dmlType, m[0]))
	})

	this.RegisterTranslation(db.TOKEN_ALIAS, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetValue()
		if m != nil {
			return fmt.Sprint(m)
		}
		return "NULL"
	})

	this.RegisterTranslation(db.TOKEN_SUM, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("SUM(%s)", RolloverParameter(dmlType, tx, m, ", "))
	})

	this.RegisterTranslation(db.TOKEN_MAX, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("MAX(%s)", RolloverParameter(dmlType, tx, m, ", "))
	})

	this.RegisterTranslation(db.TOKEN_MIN, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("MIN(%s)", RolloverParameter(dmlType, tx, m, ", "))
	})

	this.RegisterTranslation(db.TOKEN_UPPER, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("UPPER(%s)", RolloverParameter(dmlType, tx, m, ", "))
	})

	this.RegisterTranslation(db.TOKEN_LOWER, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("LOWER(%s)", RolloverParameter(dmlType, tx, m, ", "))
	})

	this.RegisterTranslation(db.TOKEN_ADD, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return RolloverParameter(dmlType, tx, m, " + ")
	})

	this.RegisterTranslation(db.TOKEN_MINUS, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return RolloverParameter(dmlType, tx, m, " - ")
	})

	this.RegisterTranslation(db.TOKEN_MULTIPLY, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return RolloverParameter(dmlType, tx, m, " * ")
	})

	this.RegisterTranslation(db.TOKEN_COUNT, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		return "COUNT(*)"
	})

	this.RegisterTranslation(db.TOKEN_COUNT_COLUMN, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("COUNT(%s)", tx.Translate(dmlType, m[0]))
	})

	this.RegisterTranslation(db.TOKEN_RTRIM, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("RTRIM(%s)", tx.Translate(dmlType, m[0]))
	})

	this.RegisterTranslation(db.TOKEN_SUBQUERY, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		v := token.GetValue()
		query := v.(*db.Query)
		return fmt.Sprintf("( %s )", this.GetSqlForQuery(query))
	})

	this.RegisterTranslation(db.TOKEN_COALESCE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("COALESCE(%s)", RolloverParameter(dmlType, tx, m, ", "))
	})

	this.RegisterTranslation(db.TOKEN_CASE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("(CASE %s END)", RolloverParameter(dmlType, tx, m, " "))
	})

	this.RegisterTranslation(db.TOKEN_CASE_WHEN, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("WHEN %s THEN %s", tx.Translate(dmlType, m[0]), tx.Translate(dmlType, m[1]))
	})

	this.RegisterTranslation(db.TOKEN_CASE_ELSE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string {
		m := token.GetMembers()
		return fmt.Sprintf("ELSE %s", tx.Translate(dmlType, m[0]))
	})
}

func (this *GenericTranslator) RegisterTranslation(name string, handler func(dmlType db.DmlType, token db.Tokener, tx db.Translator) string) {
	this.tokens[name] = handler
}

func (this *GenericTranslator) Translate(dmlType db.DmlType, token db.Tokener) string {
	tag := token.GetOperator()
	handle := this.tokens[tag]
	if handle != nil {
		return handle(dmlType, token, this.overrider)
	}
	panic("token " + tag + " is unknown")
}

func (this *GenericTranslator) GetPlaceholder(index int, name string) string {
	return "?"
}

//	@Override
//	func (this *GenericTranslator) String getAutoNumberQuery(Column column) {
//		return getAutoNumberQuery(column, false);
//	}

//	@Override
//	func (this *GenericTranslator) String getCurrentAutoNumberQuery(Column column) {
//		return getAutoNumberQuery(column, true);
//	}

//	func (this *GenericTranslator) abstract String getAutoNumberQuery(Column column, boolean current);

// INSERT
func (this *GenericTranslator) CreateInsertProcessor(insert *db.Insert) InsertProcessor {
	proc := this.InsertProcessorFactory()
	proc.Column(insert)
	proc.From(insert)
	return proc
}

func (this *GenericTranslator) GetSqlForInsert(insert *db.Insert) string {
	proc := this.CreateInsertProcessor(insert)

	str := tk.NewStrBuffer()
	// INSERT
	str.Add("INSERT INTO ", proc.TablePart(),
		"(", proc.ColumnPart(), ") VALUES(", proc.ValuePart(), ")")

	return str.String()
}

func (this *GenericTranslator) IgnoreNullKeys() bool {
	return true
}

func (this *GenericTranslator) GetAutoNumberQuery(column *db.Column) string {
	return ""
}

// UPDATE
func (this *GenericTranslator) CreateUpdateProcessor(update *db.Update) UpdateProcessor {
	proc := this.UpdateProcessorFactory()
	proc.Column(update)
	proc.From(update)
	proc.Where(update)
	return proc
}

func (this *GenericTranslator) GetSqlForUpdate(update *db.Update) string {
	proc := this.CreateUpdateProcessor(update)

	// SET
	sel := tk.NewStrBuffer()
	sel.Add("UPDATE ", proc.TablePart())
	sel.Add(" SET ", proc.ColumnPart())
	// JOINS
	// sel.Add(proc.joinPart.String())
	// WHERE - conditions
	if update.GetCriteria() != nil {
		sel.Add(" WHERE ", proc.WherePart())
	}

	return sel.String()
}

// DELETE
func (this *GenericTranslator) CreateDeleteProcessor(del *db.Delete) DeleteProcessor {
	proc := this.DeleteProcessorFactory()
	proc.From(del)
	proc.Where(del)
	return proc
}

func (this *GenericTranslator) GetSqlForDelete(del *db.Delete) string {
	proc := this.CreateDeleteProcessor(del)

	sb := tk.NewStrBuffer()

	sb.Add("DELETE FROM ", proc.TablePart())
	where := proc.WherePart()
	// INNER JOINS NOT IMPLEMENTED
	if where != "" {
		sb.Add(" WHERE ", where)
	}

	return sb.String()
}

//	@Override
//	func (this *GenericTranslator) String getSql(Sequence sequence, boolean nextValue) {
//		throw new UnsupportedOperationException();
//	}

func (this *GenericTranslator) CreateQueryProcessor(query *db.Query) QueryProcessor {
	proc := this.QueryProcessorFactory()

	proc.Column(query)
	if query.GetTable() != nil {
		proc.From(query)
	} else {
		proc.FromSubQuery(query)
	}
	proc.Where(query)
	// it is after the where clause because the joins can go to the where clause,
	// and this way the restrictions over the driving table will be applied first
	AppendJoins(query.GetJoins(), proc)
	proc.Group(query)
	proc.Having(query)
	proc.Union(query)
	proc.Order(query)

	return proc
}

func (this *GenericTranslator) GetSqlForQuery(query *db.Query) string {
	proc := this.CreateQueryProcessor(query)

	// SELECT COLUNAS
	sel := tk.NewStrBuffer()
	sel.Add("SELECT ")
	if query.IsDistinct() {
		sel.Add("DISTINCT ")
	}
	sel.Add(proc.ColumnPart())
	// FROM
	sel.Add(" FROM ", proc.FromPart())
	// JOINS
	sel.Add(proc.JoinPart())
	// WHERE - conditions
	if query.GetCriteria() != nil {
		sel.Add(" WHERE ", proc.WherePart())
	}
	// GROUP BY
	if len(query.GetGroupBy()) != 0 {
		sel.Add(" GROUP BY ", proc.GroupPart())
	}
	// HAVING
	if query.GetHaving() != nil {
		sel.Add(" HAVING ", proc.HavingPart())
	}
	// UNION
	if len(query.GetUnions()) != 0 {
		sel.Add(proc.UnionPart())
	}
	// ORDER
	if len(query.GetOrders()) != 0 {
		sel.Add(" ORDER BY ", proc.OrderPart())
	}

	sql := this.overrider.PaginateSQL(query, sel.String())

	return sql
}

func (this *GenericTranslator) PaginateSQL(query *db.Query, sql string) string {
	return sql
}

func ReduceAssociations(cachedAssociation [][]*db.PathElement, join *db.Join) ([]*db.PathElement, [][]*db.PathElement) {
	associations := join.GetPathElements()
	common := db.DeepestCommonPath(cachedAssociation, associations)
	cachedAssociation = append(cachedAssociation, join.GetPathElements())
	return associations[len(common):], cachedAssociation
}

func AppendJoins(joins []*db.Join, joiner IJoiner) {
	if len(joins) == 0 {
		return
	}

	// stores the paths already traverse.
	var cachedAssociation [][]*db.PathElement

	for _, join := range joins {
		/*
		 * Example:
		 * SELECT *
		 * FROM sales
		 * INNER JOIN employee
		 * ON sales.DepartmentID = employee.DepartmentID
		 * AND sales.EmployeeID = employee.EmployeeID
		 */

		var associations []*db.PathElement
		associations, cachedAssociation = ReduceAssociations(cachedAssociation, join)
		for _, pe := range associations {
			association := pe.Derived
			if association.IsMany2Many() {
				fromFk := association.FromM2M
				toFk := association.ToM2M

				joiner.JoinAssociation(fromFk, pe.Inner)
				joiner.JoinAssociation(toFk, pe.Inner)
			} else {
				joiner.JoinAssociation(association, pe.Inner)
			}

			if pe.Criteria != nil {
				joiner.JoinCriteria(pe.Criteria)
			}
		}
	}
}

func (this *GenericTranslator) isNot(c *db.Criteria) string {
	if c.IsNot {
		return " NOT"
	}
	return ""
}

// FROM
func (this *GenericTranslator) TableName(table *db.Table) string {
	return table.GetName()
}

func (this *GenericTranslator) ColumnName(column *db.Column) string {
	return column.GetName()
}

func (this *GenericTranslator) ColumnAlias(token db.Tokener, position int) string {
	alias := token.GetAlias()
	if alias == "" {
		if ch, ok := token.(*db.ColumnHolder); ok {
			alias = ch.GetTableAlias() + "_" + ch.GetColumn().GetName()
		} else if db.TOKEN_ALIAS != token.GetOperator() {
			alias = "COL_" + strconv.Itoa(position)
		}
	} else {
		//alias += "_" + strconv.Itoa(position) // avoids collision with reserved words
		alias = token.GetTableAlias() + "_" + alias
	}

	return alias
}

// ORDER BY
func (this *GenericTranslator) OrderBy(query *db.Query, order *db.Order) string {
	var str string
	if order.GetHolder() != nil {
		str = this.Translate(db.QUERY, order.GetHolder())
	} else {
		str = order.GetAlias()
	}

	if order.IsAsc() {
		str += " ASC"
	} else {
		str += " DESC"
	}

	return str
}

// CONDITIONS

//	func (this *GenericTranslator) String autoNumber(token db.Tokener) {
//		throw new UnsupportedOperationException();
//	}
