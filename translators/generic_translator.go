package translators

import (
	"strings"

	"github.com/quintans/faults"
	"github.com/quintans/goSQL/db"
	tk "github.com/quintans/toolkit"

	"fmt"
	"strconv"
)

type IJoiner interface {
	JoinAssociation(fk *db.Association, inner bool) error
	JoinCriteria(criteria *db.Criteria) error
	JoinPart() string
}

/*
 * =============
 * QueryBuilder
 * =============
 */

type QueryProcessor interface {
	IJoiner

	Column(query *db.Query) error
	From(query *db.Query) error
	FromSubQuery(query *db.Query) error
	Where(query *db.Query) error
	WherePart() string
	Group(query *db.Query) error
	Having(query *db.Query) error
	Order(query *db.Query) error
	Union(query *db.Query) error
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

func (q *QueryBuilder) Super(translator db.Translator) {
	q.translator = translator
	q.columnPart = tk.NewJoiner(", ")
	q.fromPart = tk.NewJoiner(", ")
	q.joinPart = tk.NewStrBuffer()
	q.wherePart = tk.NewJoiner(" AND ")
	q.groupPart = tk.NewJoiner(", ")
	q.havingPart = tk.NewStrBuffer()
	q.orderPart = tk.NewJoiner(", ")
	q.unionPart = tk.NewStrBuffer()

}

func (q *QueryBuilder) ColumnPart() string {
	return q.columnPart.String()
}

func (q *QueryBuilder) FromPart() string {
	return q.fromPart.String()
}

func (q *QueryBuilder) JoinPart() string {
	return q.joinPart.String()
}

func (q *QueryBuilder) WherePart() string {
	return q.wherePart.String()
}

func (q *QueryBuilder) GroupPart() string {
	return q.groupPart.String()
}

func (q *QueryBuilder) HavingPart() string {
	return q.havingPart.String()
}

func (q *QueryBuilder) OrderPart() string {
	return q.orderPart.String()
}

func (q *QueryBuilder) UnionPart() string {
	return q.unionPart.String()
}

func (q *QueryBuilder) Column(query *db.Query) error {
	for k, token := range query.Columns {
		s, err := q.translator.Translate(db.QUERY, token)
		if err != nil {
			return faults.Wrap(err)
		}
		q.columnPart.Add(s)
		a := q.translator.ColumnAlias(token, k+1)
		if a != "" {
			q.columnPart.Append(" AS ", a)
		}
	}
	return nil
}

func (q *QueryBuilder) From(query *db.Query) error {
	table := query.GetTable()
	alias := query.GetTableAlias()
	q.fromPart.AddAsOne(q.translator.TableName(table), " ", alias)
	return nil
}

func (q *QueryBuilder) FromSubQuery(query *db.Query) error {
	subquery := query.GetSubQuery()
	alias := query.GetSubQueryAlias()
	q.fromPart.AddAsOne("(", q.translator.GetSqlForQuery(subquery), ")")
	if alias != "" {
		q.fromPart.Append(" ", alias)
	}
	return nil
}

func (q *QueryBuilder) JoinAssociation(fk *db.Association, inner bool) error {
	if inner {
		q.joinPart.Add(" INNER JOIN ")
	} else {
		q.joinPart.Add(" LEFT OUTER JOIN ")
	}

	q.joinPart.Add(q.translator.TableName(fk.GetTableTo()), " ", fk.GetAliasTo(), " ON ")

	for i, rel := range fk.GetRelations() {
		if i > 0 {
			q.joinPart.Add(" AND ")
		}
		args, err := Translate(q.translator.Translate, db.QUERY, rel.From, rel.To)
		if err != nil {
			return faults.Wrap(err)
		}
		q.joinPart.Add(args[0], " = ", args[1])
	}
	return nil
}

func Translate(translator func(db.DmlType, db.Tokener) (string, error), dmlType db.DmlType, tokens ...db.Tokener) ([]string, error) {
	args := make([]string, len(tokens))
	for k, t := range tokens {
		var err error
		args[k], err = translator(dmlType, t)
		if err != nil {
			return nil, err
		}
	}
	return args, nil
}

func (q *QueryBuilder) JoinCriteria(criteria *db.Criteria) error {
	s, err := q.translator.Translate(db.QUERY, criteria)
	if err != nil {
		return faults.Wrap(err)
	}
	q.joinPart.Add(" AND ", s)
	return nil
}

func (q *QueryBuilder) Where(query *db.Query) error {
	criteria := query.GetCriteria()
	if criteria != nil {
		s, err := q.translator.Translate(db.QUERY, criteria)
		if err != nil {
			return faults.Wrap(err)
		}
		q.wherePart.Add(s)
	}
	return nil
}

func (q *QueryBuilder) Group(query *db.Query) error {
	groups := query.GetGroupByTokens()
	for _, group := range groups {
		//this.groupPart.Add(this.translator.ColumnAlias(group.Token, group.Position))
		s, err := q.translator.Translate(db.QUERY, group.Token)
		if err != nil {
			return faults.Wrap(err)
		}
		q.groupPart.Add(s)
	}
	return nil
}

func (q *QueryBuilder) Having(query *db.Query) error {
	having := query.GetHaving()
	if having != nil {
		s, err := q.translator.Translate(db.QUERY, having)
		if err != nil {
			return faults.Wrap(err)
		}
		q.havingPart.Add(s)
	}
	return nil
}

func (q *QueryBuilder) Order(query *db.Query) error {
	orders := query.GetOrders()
	for _, ord := range orders {
		if ord.GetHolder() != nil {
			s, err := q.translator.Translate(db.QUERY, ord.GetHolder())
			if err != nil {
				return faults.Wrap(err)
			}
			q.orderPart.Add(s)
		} else {
			q.orderPart.Add(ord.GetAlias())
		}

		if ord.IsAsc() {
			q.orderPart.Append(" ASC")
		} else {
			q.orderPart.Append(" DESC")
		}
	}
	return nil
}

func (q *QueryBuilder) Union(query *db.Query) error {
	unions := query.GetUnions()
	for _, u := range unions {
		q.unionPart.Add(" UNION ")
		if u.All {
			q.unionPart.Add("ALL ")
		}
		q.unionPart.Add(q.translator.GetSqlForQuery(u.Query))
	}
	return nil
}

/*
 * =============
 * UpdateBuilder
 * =============
 */

type UpdateProcessor interface {
	Column(update *db.Update) error
	From(update *db.Update) error
	ColumnPart() string
	TablePart() string
	Where(update *db.Update) error
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

func (u *UpdateBuilder) Super(translator db.Translator) {
	u.translator = translator
	u.columnPart = tk.NewJoiner(", ")
	u.tablePart = tk.NewJoiner(", ")
	u.wherePart = tk.NewJoiner(" AND ")

}

func (u *UpdateBuilder) ColumnPart() string {
	return u.columnPart.String()
}

func (u *UpdateBuilder) TablePart() string {
	return u.tablePart.String()
}

func (u *UpdateBuilder) WherePart() string {
	return u.wherePart.String()
}

func (u *UpdateBuilder) Column(update *db.Update) error {
	values := update.GetValues()
	tableAlias := update.GetTableAlias()
	for it := values.Iterator(); it.HasNext(); {
		entry := it.Next()
		column := entry.Key.(*db.Column)
		// use only not virtual columns
		token := entry.Value.(db.Tokener)
		s, err := u.translator.Translate(db.UPDATE, token)
		if err != nil {
			return faults.Wrap(err)
		}
		u.columnPart.AddAsOne(tableAlias, ".",
			u.translator.ColumnName(column),
			" = ", s)
	}
	return nil
}

func (u *UpdateBuilder) From(update *db.Update) error {
	table := update.GetTable()
	alias := update.GetTableAlias()
	u.tablePart.AddAsOne(u.translator.TableName(table), " ", alias)
	return nil
}

func (u *UpdateBuilder) Where(update *db.Update) error {
	criteria := update.GetCriteria()
	if criteria != nil {
		s, err := u.translator.Translate(db.UPDATE, criteria)
		if err != nil {
			return faults.Wrap(err)
		}
		u.wherePart.Add(s)
	}
	return nil
}

/*
 * =============
 * DeleteBuilder
 * =============
 */

type DeleteProcessor interface {
	From(del *db.Delete) error
	TablePart() string
	Where(del *db.Delete) error
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

func (d *DeleteBuilder) Super(translator db.Translator) {
	d.translator = translator

	d.tablePart = tk.NewJoiner(", ")
	d.wherePart = tk.NewJoiner(" AND ")
}

func (d *DeleteBuilder) TablePart() string {
	return d.tablePart.String()
}

func (d *DeleteBuilder) WherePart() string {
	return d.wherePart.String()
}

func (d *DeleteBuilder) From(del *db.Delete) error {
	table := del.GetTable()
	alias := del.GetTableAlias()
	d.tablePart.AddAsOne(d.translator.TableName(table), " ", alias)
	return nil
}

func (d *DeleteBuilder) Where(del *db.Delete) error {
	criteria := del.GetCriteria()
	if criteria != nil {
		s, err := d.translator.Translate(db.DELETE, criteria)
		if err != nil {
			return faults.Wrap(err)
		}
		d.wherePart.Add(s)
	}
	return nil
}

/*
 * =============
 * InsertBuilder
 * =============
 */

type InsertProcessor interface {
	Column(insert *db.Insert) error
	From(insert *db.Insert) error
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

func (i *InsertBuilder) Super(translator db.Translator) {
	i.translator = translator
	i.columnPart = tk.NewJoiner(", ")
	i.valuePart = tk.NewJoiner(", ")
	i.tablePart = tk.NewJoiner(", ")
}

func (i *InsertBuilder) ColumnPart() string {
	return i.columnPart.String()
}

func (i *InsertBuilder) ValuePart() string {
	return i.valuePart.String()
}

func (i *InsertBuilder) TablePart() string {
	return i.tablePart.String()
}

func (i *InsertBuilder) Column(insert *db.Insert) error {
	values := insert.GetValues()
	parameters := insert.GetParameters()
	var val string
	for it := values.Iterator(); it.HasNext(); {
		entry := it.Next()
		column := entry.Key.(*db.Column)
		// use only not virtual columns
		token := entry.Value.(db.Tokener)
		// only includes null keys if IgnoreNullKeys is false
		if column.IsKey() && i.translator.IgnoreNullKeys() &&
			db.TOKEN_PARAM == token.GetOperator() {
			param := token.GetValue().(string)
			if parameters[param] != nil {
				var err error
				val, err = i.translator.Translate(db.INSERT, token)
				if err != nil {
					return faults.Wrap(err)
				}
			}
		} else {
			var err error
			val, err = i.translator.Translate(db.INSERT, token)
			if err != nil {
				return faults.Wrap(err)
			}
		}

		col := i.translator.ColumnName(column)

		if val != "" {
			i.columnPart.Add(col)
			i.valuePart.Add(val)
		}

		val = ""
	}
	return nil
}

func (i *InsertBuilder) From(insert *db.Insert) error {
	table := insert.GetTable()
	i.tablePart.Add(i.translator.TableName(table))
	return nil
}

/*
 * =================
 * GenericTranslator
 * =================
 */

type GenericTranslator struct {
	tokens                 map[string]TranslationHandler
	overrider              db.Translator
	QueryProcessorFactory  func() QueryProcessor
	InsertProcessorFactory func() InsertProcessor
	UpdateProcessorFactory func() UpdateProcessor
	DeleteProcessorFactory func() DeleteProcessor
	converters             map[string]db.Converter
}

func (g *GenericTranslator) Init(overrider db.Translator) {
	g.overrider = overrider
	g.tokens = make(map[string]TranslationHandler)
	g.converters = map[string]db.Converter{}

	// Column
	g.RegisterTranslation(db.TOKEN_COLUMN, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		if col, ok := token.GetValue().(*db.Column); ok {
			sb := tk.NewStrBuffer()
			if token.GetTableAlias() != "" {
				sb.Add(token.GetTableAlias())
			} else {
				sb.Add(tx.TableName(col.GetTable()))
			}
			sb.Add(".", tx.ColumnName(col))
			return sb.String(), nil
		}

		return "", nil
	})

	// Match
	g.RegisterTranslation(db.TOKEN_EQ, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		o := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, o[0], o[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s = %s", args[0], args[1]), nil
	})

	// Match
	g.RegisterTranslation(db.TOKEN_NULL, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		return "NULL", nil
	})

	// Val
	handle := func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		o := token.GetValue()
		if o != nil {
			if s, ok := o.(string); ok {
				return "'" + s + "'", nil
			} else {
				return fmt.Sprint(o), nil
			}
		}
		return "NULL", nil
	}
	g.RegisterTranslation(db.TOKEN_RAW, handle)
	g.RegisterTranslation(db.TOKEN_ASIS, handle)

	g.RegisterTranslation(db.TOKEN_IEQ, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("UPPER(%s) = UPPER(%s)", args[0], args[1]), nil
	})

	// Diferent
	g.RegisterTranslation(db.TOKEN_NEQ, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s <> %s", args[0], args[1]), nil
	})

	g.RegisterTranslation(db.TOKEN_RANGE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		field, err := tx.Translate(dmlType, m[0])
		if err != nil {
			return "", err
		}
		var bottom string
		var top string
		if m[1] != nil {
			bottom, err = tx.Translate(dmlType, m[1])
			if err != nil {
				return "", err
			}
		}
		if m[2] != nil {
			top, err = tx.Translate(dmlType, m[2])
			if err != nil {
				return "", err
			}
		}

		if bottom != "" && top != "" {
			return fmt.Sprintf("%s >= %s AND %s <= %s", field, bottom, field, top), nil
		} else if bottom != "" {
			return fmt.Sprintf("%s >= %s", field, bottom), nil
		} else if top != "" {
			return fmt.Sprintf("%s <= %s", field, top), nil
		}
		return "", faults.New("Invalid Range Token")
	})

	// ValueRange
	g.RegisterTranslation(db.TOKEN_VALUERANGE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		bottom, err := tx.Translate(dmlType, m[0])
		if err != nil {
			return "", err
		}
		top, err := tx.Translate(dmlType, m[1])
		if err != nil {
			return "", err
		}
		var value string
		if m[2] != nil {
			value, err = tx.Translate(dmlType, m[2])
			if err != nil {
				return "", err
			}
		}

		if value != "" {
			return fmt.Sprintf(
				"(%s IS NULL AND %s IS NULL OR %s IS NULL AND %s <= %s OR %s IS NULL AND %s >= %s OR %s >= %s AND %s <= %s)",
				top, bottom, top, bottom, value, bottom, top, value, top, value, bottom, value,
			), nil
		}
		return "", faults.New("Invalid ValueRange Token")
	})

	// boundedValueRange
	g.RegisterTranslation(db.TOKEN_BOUNDEDRANGE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		bottom, err := tx.Translate(dmlType, m[0])
		if err != nil {
			return "", err
		}
		top, err := tx.Translate(dmlType, m[1])
		if err != nil {
			return "", err
		}
		var value string
		if m[2] != nil {
			value, err = tx.Translate(dmlType, m[2])
			if err != nil {
				return "", err
			}
		}

		if value != "" {
			return fmt.Sprintf("(%s >= %s AND %s <= %s)", top, value, bottom, value), nil
		}
		return "", faults.New("Invalid BoundedRange Token")
	})

	// In
	g.RegisterTranslation(db.TOKEN_IN, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		c, ok := token.(*db.Criteria)
		if !ok {
			return "", nil
		}

		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		roll := strings.Join(args[1:], ", ")

		sb := tk.NewStrBuffer()
		if c.IsNot {
			sb.Add(" NOT")
		}
		sb.Add(args[0])
		if m[1].GetOperator() == db.TOKEN_SUBQUERY {
			sb.Add(" IN ", roll)
		} else {
			sb.Add(" IN (", roll, ")")
		}
		return sb.String(), nil
	})

	// Or
	g.RegisterTranslation(db.TOKEN_OR, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		var sb strings.Builder
		sb.WriteString("(")
		sb.WriteString(strings.Join(args, " OR "))
		sb.WriteString(")")
		return sb.String(), nil
	})

	// And
	g.RegisterTranslation(db.TOKEN_AND, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		return strings.Join(args, " AND "), nil
	})

	// Like
	g.RegisterTranslation(db.TOKEN_LIKE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		c := token.(*db.Criteria)
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer(args[0], isNot(c), " LIKE ", args[1])
		return sb.String(), nil
	})

	//	ILike
	g.RegisterTranslation(db.TOKEN_ILIKE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		c := token.(*db.Criteria)
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("UPPER(", args[0], ")", isNot(c), " LIKE UPPER(", args[1], ")")
		return sb.String(), nil
	})

	// isNull
	g.RegisterTranslation(db.TOKEN_ISNULL, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		c := token.(*db.Criteria)
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer(args[0], "IS", isNot(c), " NULL")
		return sb.String(), nil
	})

	// Greater
	g.RegisterTranslation(db.TOKEN_GT, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer(args[0], " > ", args[1])
		return sb.String(), nil
	})

	// Lesser
	g.RegisterTranslation(db.TOKEN_LT, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer(args[0], " < ", args[1])
		return sb.String(), nil
	})

	// GreaterOrEqual
	g.RegisterTranslation(db.TOKEN_GTEQ, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer(args[0], " >= ", args[1])
		return sb.String(), nil
	})

	// LesserOrEqual
	g.RegisterTranslation(db.TOKEN_LTEQ, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer(args[0], " <= ", args[1])
		return sb.String(), nil
	})

	// FUNCTIONS
	// Param
	g.RegisterTranslation(db.TOKEN_PARAM, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		sb := tk.NewStrBuffer(":", token.GetValue())
		return sb.String(), nil
	})

	// exists
	g.RegisterTranslation(db.TOKEN_EXISTS, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("EXISTS ", args[0])
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_NOT, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("NOT ", args[0])
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_ALIAS, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetValue()
		if m != nil {
			return fmt.Sprint(m), nil
		}
		return "NULL", nil
	})

	g.RegisterTranslation(db.TOKEN_SUM, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("SUM(", strings.Join(args, ", "), ")")
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_MAX, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("MAX(", strings.Join(args, ", "), ")")
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_MIN, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("MIN(", strings.Join(args, ", "), ")")
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_UPPER, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("UPPER(", strings.Join(args, ", "), ")")
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_LOWER, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("LOWER(", strings.Join(args, ", "), ")")
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_ADD, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		return strings.Join(args, " + "), nil
	})

	g.RegisterTranslation(db.TOKEN_MINUS, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		return strings.Join(args, " - "), nil
	})

	g.RegisterTranslation(db.TOKEN_MULTIPLY, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		return strings.Join(args, " * "), nil
	})

	g.RegisterTranslation(db.TOKEN_COUNT, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		return "COUNT(*)", nil
	})

	g.RegisterTranslation(db.TOKEN_COUNT_COLUMN, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("COUNT(", args[0], ")")
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_RTRIM, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("RTRIM(", args[0], ")")
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_SUBQUERY, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		v := token.GetValue()
		query := v.(*db.Query)
		return fmt.Sprintf("( %s )", g.GetSqlForQuery(query)), nil
	})

	g.RegisterTranslation(db.TOKEN_COALESCE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("COALESCE(", strings.Join(args, ", "), ")")
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_CASE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("CASE ", strings.Join(args, " "), " END")
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_CASE_WHEN, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("WHEN ", args[0], " THEN ", args[1])
		return sb.String(), nil
	})

	g.RegisterTranslation(db.TOKEN_CASE_ELSE, func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error) {
		m := token.GetMembers()
		args, err := Translate(tx.Translate, dmlType, m...)
		if err != nil {
			return "", err
		}
		sb := tk.NewStrBuffer("ELSE ", args[0])
		return sb.String(), nil
	})
}

type TranslationHandler func(dmlType db.DmlType, token db.Tokener, tx db.Translator) (string, error)

func (g *GenericTranslator) RegisterTranslation(name string, handler TranslationHandler) {
	g.tokens[name] = handler
}

func (g *GenericTranslator) Translate(dmlType db.DmlType, token db.Tokener) (string, error) {
	tag := token.GetOperator()
	handle := g.tokens[tag]
	if handle != nil {
		return handle(dmlType, token, g.overrider)
	}
	return "", faults.Errorf("token '%s' is unknown", tag)
}

func (g *GenericTranslator) GetPlaceholder(index int, name string) string {
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
func (g *GenericTranslator) CreateInsertProcessor(insert *db.Insert) InsertProcessor {
	proc := g.InsertProcessorFactory()
	proc.Column(insert)
	proc.From(insert)
	return proc
}

func (g *GenericTranslator) GetSqlForInsert(insert *db.Insert) string {
	proc := g.CreateInsertProcessor(insert)

	str := tk.NewStrBuffer()
	// INSERT
	str.Add("INSERT INTO ", proc.TablePart(),
		"(", proc.ColumnPart(), ") VALUES(", proc.ValuePart(), ")")

	return str.String()
}

func (g *GenericTranslator) IgnoreNullKeys() bool {
	return true
}

func (g *GenericTranslator) GetAutoNumberQuery(column *db.Column) string {
	return ""
}

// UPDATE
func (g *GenericTranslator) CreateUpdateProcessor(update *db.Update) UpdateProcessor {
	proc := g.UpdateProcessorFactory()
	proc.Column(update)
	proc.From(update)
	proc.Where(update)
	return proc
}

func (g *GenericTranslator) GetSqlForUpdate(update *db.Update) string {
	proc := g.CreateUpdateProcessor(update)

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
func (g *GenericTranslator) CreateDeleteProcessor(del *db.Delete) DeleteProcessor {
	proc := g.DeleteProcessorFactory()
	proc.From(del)
	proc.Where(del)
	return proc
}

func (g *GenericTranslator) GetSqlForDelete(del *db.Delete) string {
	proc := g.CreateDeleteProcessor(del)

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

func (g *GenericTranslator) CreateQueryProcessor(query *db.Query) QueryProcessor {
	proc := g.QueryProcessorFactory()

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

func (g *GenericTranslator) GetSqlForQuery(query *db.Query) string {
	proc := g.CreateQueryProcessor(query)

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

	sql := g.overrider.PaginateSQL(query, sel.String())

	return sql
}

func (g *GenericTranslator) PaginateSQL(query *db.Query, sql string) string {
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

func isNot(c *db.Criteria) string {
	if c.IsNot {
		return " NOT"
	}
	return ""
}

// FROM
func (g *GenericTranslator) TableName(table *db.Table) string {
	return table.GetName()
}

func (g *GenericTranslator) ColumnName(column *db.Column) string {
	return column.GetName()
}

func (g *GenericTranslator) ColumnAlias(token db.Tokener, position int) string {
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
func (g *GenericTranslator) OrderBy(query *db.Query, order *db.Order) (string, error) {
	var str string
	if order.GetHolder() != nil {
		var err error
		str, err = g.Translate(db.QUERY, order.GetHolder())
		if err != nil {
			return "", err
		}
	} else {
		str = order.GetAlias()
	}

	if order.IsAsc() {
		str += " ASC"
	} else {
		str += " DESC"
	}

	return str, nil
}

func (g *GenericTranslator) RegisterConverter(name string, c db.Converter) {
	g.converters[name] = c
}

func (g *GenericTranslator) GetConverter(name string) db.Converter {
	return g.converters[name]
}

// CONDITIONS

//	func (this *GenericTranslator) String autoNumber(token db.Tokener) {
//		throw new UnsupportedOperationException();
//	}
