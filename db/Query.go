package db

import (
	"github.com/quintans/goSQL/dbx"
	tk "github.com/quintans/toolkit"
	coll "github.com/quintans/toolkit/collection"

	"database/sql"
	"errors"
	"fmt"
	"time"
)

const OFFSET_PARAM = "OFFSET_PARAM"
const LIMIT_PARAM = "LIMIT_PARAM"

type Query struct {
	DmlBase

	Columns       []Tokener
	subQuery      *Query
	subQueryAlias string
	distinct      bool

	orders []*Order
	unions []*Union
	// saves position of columnHolder
	groupBy   []int
	offset    int64
	limit     int64
	lastToken Tokener
	lastOrder *Order
}

func NewQuery(db IDb, table *Table) *Query {
	this := new(Query)
	this.DmlBase.Super(db, table)
	return this
}

func (this *Query) Alias(alias string) *Query {
	this.alias(alias)
	return this
}

func NewQueryQuery(subquery *Query) *Query {
	return NewQueryQueryAs(subquery, "")
}

func NewQueryQueryAs(subquery *Query, subQueryAlias string) *Query {
	this := new(Query)
	this.Super(subquery.db, nil)
	this.subQuery = subquery
	this.subQueryAlias = subQueryAlias
	// copy the parameters of the subquery to the main query
	for k, v := range subquery.GetParameters() {
		this.SetParameter(k, v)
	}
	return this
}

func (this *Query) All() *Query {
	if this.table != nil {
		for it := this.table.columns.Enumerator(); it.HasNext(); {
			this.Column(it.Next().(*Column))
		}
	}
	return this
}

func (this *Query) Copy(other *Query) {
	this.table = other.table
	this.tableAlias = other.tableAlias

	if other.GetJoins() != nil {
		this.joins = make([]*Join, len(other.joins))
		copy(this.joins, other.joins)
	}
	if other.criteria != nil {
		this.criteria, _ = other.criteria.Clone().(*Criteria)
	}
	if this.parameters != nil {
		for k, v := range other.parameters {
			this.parameters[k] = v
		}
	}

	if other.subQuery != nil {
		q := other.subQuery
		this.subQuery = NewQuery(this.db, q.table)
		this.subQuery.Copy(q)
		this.subQueryAlias = other.subQueryAlias
	}

	this.distinct = other.distinct
	if other.Columns != nil {
		this.Columns = make([]Tokener, len(other.Columns))
		copy(this.Columns, other.Columns)
	}
	if other.orders != nil {
		this.orders = make([]*Order, len(other.orders))
		copy(this.orders, other.orders)
	}
	if other.unions != nil {
		this.unions = make([]*Union, len(other.unions))
		copy(this.unions, other.unions)
	}
	// saves position of columnHolder
	if other.groupBy != nil {
		this.groupBy = make([]int, len(other.groupBy))
		copy(this.groupBy, other.groupBy)
	}

	this.offset = other.offset
	this.limit = other.limit

	this.rawSQL = other.rawSQL
}

func (this *Query) GetOffset() int64 {
	return this.offset
}

func (this *Query) Offset(offset int64) *Query {
	if offset < 0 {
		this.offset = 0
	} else {
		this.offset = offset
	}
	return this
}

func (this *Query) GetLimit() int64 {
	return this.limit
}

func (this *Query) Limit(limit int64) *Query {
	if limit < 0 {
		this.limit = 0
	} else {
		this.limit = limit
	}
	return this
}

func (this *Query) GetSubQuery() *Query {
	return this.subQuery
}

func (this *Query) GetSubQueryAlias() string {
	return this.subQueryAlias
}

func (this *Query) Distinct() *Query {
	this.distinct = true
	this.rawSQL = nil
	return this
}

func (this *Query) IsDistinct() bool {
	return this.distinct
}

// COL ===

func (this *Query) ColumnsReset() {
	this.Columns = nil
}

func (this *Query) Column(column interface{}) *Query {
	token := tokenizeOne(column)
	if _, ok := column.(*Query); ok {
		this.lastToken = token.Clone().(Tokener)
	} else {
		this.lastToken = token
	}
	this.replaceRaw(this.lastToken)

	// TODO: implement virtual columns
	this.joinVirtualColumns(this.lastToken, nil)
	this.lastToken.SetTableAlias(this.tableAlias)
	this.Columns = append(this.Columns, this.lastToken)

	this.rawSQL = nil

	return this
}

// Defines the alias of the last column
// param alias: The Alias
// return: The query
func (this *Query) As(alias string) *Query {
	if this.lastToken != nil {
		this.lastToken.SetAlias(alias)
	}

	this.rawSQL = nil

	return this
}

// WHERE ===
func (this *Query) Where(restriction ...*Criteria) *Query {
	if len(restriction) > 0 {
		this.DmlBase.where(restriction...)
	}
	return this
}

// ===

// ORDER ===
func (this *Query) OrdersReset() {
	this.orders = nil
}

func (this *Query) order(columnHolder *ColumnHolder) *Query {

	this.lastOrder = NewOrder(columnHolder).Asc(true)
	this.orders = append(this.orders, this.lastOrder)

	this.rawSQL = nil

	return this
}

func (this *Query) Order(column *Column) *Query {
	return this.OrderAs(column, this.tableAlias)
}

func (this *Query) OrderAs(column *Column, alias string) *Query {
	ch := NewColumnHolder(column)
	if alias != "" {
		ch.SetTableAlias(alias)
	} else if column.IsVirtual() {
		// the tableAlias is set to nil to allow joinVirtualColumns
		// to the define the alias
		this.joinVirtualColumns(ch, nil)
	} else {
		ch.SetTableAlias(this.tableAlias)
	}

	return this.order(ch)
}

// Order by a column belonging to the table targeted by the association list
//
// param column: the order by column
// param associations: the association list that leads to the desired table
// return: this query
func (this *Query) OrderOn(column *Column, associations ...*Association) *Query {
	pathElements := make([]*PathElement, len(associations))
	for k, association := range associations {
		pathElements[k] = &PathElement{association, nil, false}
	}

	return this.OrderFor(column, pathElements...)
}

func (this *Query) OrderFor(column *Column, pathElements ...*PathElement) *Query {
	var pes []*PathElement
	if column.IsVirtual() {
		// appending the path of the virtual column
		ch := NewColumnHolder(column)
		discriminator := ch.GetColumn().GetVirtual().Association
		pes = append(pes, &PathElement{discriminator, nil, false})
	} else {
		pes = pathElements
	}

	common := DeepestCommonPath(this.cachedAssociation, pes)
	if len(common) == len(pes) {
		orderAlias := common[len(common)-1].Derived.GetAliasTo()
		return this.OrderAs(column, orderAlias)
	}

	panic("The path specified in the order is not valid")
}

// Defines the order column. The column belongs to the table targeted by the last defined association.
// If there is no last association, the column belongs to the driving table
//
// param: The column
func (this *Query) OrderBy(column *Column) *Query {
	if this.lastJoin != nil {
		return this.OrderFor(column, this.lastJoin.GetPathElements()...)
	}
	return this.OrderAs(column, this.lastFkAlias)
}

func (this *Query) OrderByAs(column string) *Query {
	this.lastOrder = NewOrderAs(column).Asc(true)
	this.orders = append(this.orders, this.lastOrder)

	this.rawSQL = nil

	return this
}

// Sets the order direction for the last order by command
//
// return this
func (this *Query) Asc(dir bool) *Query {
	if this.lastOrder != nil {
		this.lastOrder.Asc(dir)

		this.rawSQL = nil
	}
	return this
}

func (this *Query) GetOrders() []*Order {
	return this.orders
}

// JOINS ===

// includes the associations as inner joins to the current path
//
// param: associations
// return this query
func (this *Query) Inner(associations ...*Association) *Query {
	this.DmlBase.inner(associations...)
	return this
}

// includes the associations as outer joins to the current path
//
// param associations
// return
func (this *Query) Outer(associations ...*Association) *Query {
	for _, association := range associations {
		this.path = append(this.path, &PathElement{association, nil, false})
	}

	this.rawSQL = nil

	return this
}

func (this *Query) Fetch() *Query {
	return this.FetchAs("")
}

// indicates that the path should be used to retrive all the columns<br>
// TODAS as colunas das tabelas intermédias são incluidas no select bem como
// a TODAS as colunas da tabela no fim das associações.<br>
//
// param endAlias:
// return
func (this *Query) FetchAs(endAlias string) *Query {
	if this.path != nil {
		cache := this.buildPathCriterias(this.path)
		// process the acumulated conditions
		var firstConditions []*Criteria
		for index, pathCriteria := range cache {
			if pathCriteria != nil {
				conds := pathCriteria.Criterias
				if conds != nil {
					// index == 0 applies to the starting table
					if index == 0 {
						// already with the alias applied
						firstConditions = conds
					} else {
						this.fetch("", pathCriteria.Path...)
						if firstConditions != nil {
							// new coppy
							tmp := make([]*Criteria, len(conds))
							copy(tmp, conds)
							tmp = append(tmp, firstConditions...)
							firstConditions = nil
							conds = tmp
						}
						this.applyOn(And(conds...))
					}
				}
			}
		}

		// if the last one was not processed
		if cache[len(cache)-1] == nil {
			this.fetch(endAlias, this.path...)
		}
		if firstConditions != nil {
			this.applyOn(And(firstConditions...))
		}
	}
	this.path = nil

	this.rawSQL = nil

	return this
}

func (this *Query) Join() *Query {
	return this.JoinAs("")
}

//indicates that the path should be used to join only
//
//param endAlias:
//return
func (this *Query) JoinAs(endAlias string) *Query {
	this.DmlBase.joinAs(endAlias)
	return this
}

// builds an inner join with the passed associations
//
// param associations:
// return
func (this *Query) InnerJoin(associations ...*Association) *Query {
	this.DmlBase.innerJoin(associations...)
	return this
}

// builds an outer join with the passed associations
//
// param endAlias: alias to use for the last table
// param associations
// return
func (this *Query) OuterJoinAs(endAlias string, associations ...*Association) *Query {
	this.Outer(associations...).JoinAs(endAlias)
	return this
}

// builds an outer join with the passed associations
//
// param associations
// return
func (this *Query) OuterJoin(associations ...*Association) *Query {
	this.Outer(associations...).Join()
	return this
}

// adds a column refering the last defined association
//
// param column
// return
func (this *Query) Include(column *Column) *Query {
	return this.IncludeAs(this.lastFkAlias, column)
}

// adds a column refering the last defined association
//
// param tableAlias:the alias to use for the table
// param column
// return
func (this *Query) IncludeAs(tableAlias string, column *Column) *Query {
	ch := NewColumnHolder(column)
	this.joinVirtualColumns(this.lastToken, this.lastJoin.GetPathElements())
	ch.SetTableAlias(tableAlias)
	this.Columns = append(this.Columns, ch)

	this.lastToken = ch

	this.rawSQL = nil

	return this
}

// adds a token refering the last defined association
//
// param function
// return
func (this *Query) IncludeToken(token Tokener) *Query {
	this.lastToken = token.Clone().(Tokener)
	this.joinVirtualColumns(this.lastToken, this.lastJoin.GetPathElements())

	this.lastToken.SetTableAlias(this.lastFkAlias)
	this.Columns = append(this.Columns, this.lastToken)

	this.rawSQL = nil

	return this
}

// Executes an OUTER join with the tables defined by the associations.
// ALL the columns from the intermediate tables are included in the final select
// as well as ALL columns of the last table refered by the association list
//
// param associations: the foreign keys
// return
func (this *Query) OuterFetch(associations ...*Association) *Query {
	this.Outer(associations...).Fetch()
	return this
}

//	/**
// Executa um INNER join com as tabelas definidas pelas foreign keys.<br>
// TODAS as colunas das tabelas intermédias são incluidas no select bem como
// TODAS as colunas da tabela no fim das associações.<br>
//
// param associations
//            as foreign keys que definem uma navegação
// return
///
func (this *Query) InnerFetch(associations ...*Association) *Query {
	this.Inner(associations...).Fetch()
	return this
}

func (this *Query) fetch(endAlias string, pathElements ...*PathElement) *Query {
	//the current path
	var currentPath []*PathElement

	var fresh []*Association
	// finds the ForeignKey's that are not present in any join
	matches := true

	common := DeepestCommonPath(this.cachedAssociation, pathElements)

	for f, pe := range pathElements {
		if matches && f < len(common) {
			if !common[f].Base.Equals(pe.Base) {
				matches = false
			}
		} else {
			matches = false
		}

		if !matches {
			fresh = append(fresh, pe.Base)
		} else {
			currentPath = append(currentPath, common[f])
		}
	}

	// returns a list with the old ones (currentPath) + the new ones (with the alias already defined)
	local := this.addJoin(endAlias, pathElements)
	tmp := make([]*PathElement, len(local))
	// remove old ones, keeping the new ones
	i := 0
	for _, loc := range local {
		// find wich currentPath elements exist in local, and ignore (remove) them
		found := false
		for _, cp := range currentPath {
			if loc == cp {
				found = true
				break
			}
		}
		// ignore if found
		if !found {
			tmp[i] = loc
			i++
		}
	}
	local = tmp[:i]

	// adds all columns of all joins
	for _, pe := range local {
		// find fk with the alias
		fkNew := pe.Derived
		var fk *Association
		if fkNew.IsMany2Many() {
			fk = fkNew.ToM2M
		} else {
			fk = fkNew
		}
		ta := this.joinBag.GetAlias(fk)

		currentPath = append(currentPath, pe)
		// adds all columns for the target table of the association
		for it := fkNew.GetTableTo().GetColumns().Enumerator(); it.HasNext(); {
			column := it.Next().(*Column)
			ch := NewColumnHolder(column)
			this.joinVirtualColumns(ch, currentPath)
			ch.SetTableAlias(ta)
			this.Columns = append(this.Columns, ch)
		}
	}

	return this
}

// Criteria to be applied to the previous associations
//
// param criteria: criteria
// return: this
func (this *Query) On(criteria ...*Criteria) *Query {
	this.DmlBase.on(criteria...)
	return this
}

// UNIONS ===
func (this *Query) Union(query *Query) *Query {
	return this.unite(query, false)
}

func (this *Query) UnionAll(query *Query) *Query {
	return this.unite(query, true)
}

func (this *Query) unite(query *Query, all bool) *Query {
	// copy the parameters of the subquery to the main query
	for k, v := range query.GetParameters() {
		this.SetParameter(k, v)
	}
	this.unions = append(this.unions, &Union{query, all})

	this.rawSQL = nil

	return this
}

func (this *Query) GetUnions() []*Union {
	return this.unions
}

// GROUP BY ===
func (this *Query) GroupByUntil(untilPos int) *Query {
	this.groupBy = make([]int, untilPos)
	for i := 0; i < untilPos; i++ {
		this.groupBy[i] = i + 1
	}

	this.rawSQL = nil

	return this
}

func (this *Query) GroupByPos(pos ...int) *Query {
	this.groupBy = pos

	this.rawSQL = nil

	return this
}

func (this *Query) GetGroupBy() []int {
	return this.groupBy
}

func (this *Query) GetGroupByColumns() []Tokener {
	var tokens []Tokener
	length := len(this.groupBy)
	if length > 0 {
		tokens = make([]Tokener, length)
		for k, idx := range this.groupBy {
			tokens[k] = this.Columns[idx-1]
		}
	}
	return tokens
}

func (this *Query) GroupBy(cols ...*Column) *Query {
	this.rawSQL = nil

	length := len(cols)
	if length == 0 {
		this.groupBy = nil
		return this
	}

	this.groupBy = make([]int, length)

	pos := 1
	for i := 0; i < length; i++ {
		for _, token := range this.Columns {
			if ch, ok := token.(*ColumnHolder); ok {
				if ch.GetColumn().Equals(cols[i]) {
					this.groupBy[i] = pos
					break
				}
			}
		}
		pos++

		if this.groupBy[i] == 0 {
			panic(fmt.Sprintf("Column alias '%' was not found", cols[i]))
		}
	}

	return this
}

func (this *Query) GroupByAs(aliases ...string) *Query {
	this.rawSQL = nil

	length := len(aliases)
	if length == 0 {
		this.groupBy = nil
		return this
	}

	this.groupBy = make([]int, length)

	pos := 1
	for i := 0; i < length; i++ {
		for _, token := range this.Columns {
			if aliases[i] == token.GetAlias() {
				this.groupBy[i] = pos
				break
			}
		}
		pos++

		if this.groupBy[i] == 0 {
			panic(fmt.Sprintf("Column alias '%' was not found", aliases[i]))
		}
	}

	return this
}

// ======== RETRIVE ==============

// list simple variables
// a closure is used to build the result list.
// The types for scanning are supplied by the instances parameter.
//
// ex:
// roles = make([]string, 0)
// var role string
// q.ListSimpleFor(func() {
// 		roles = append(roles, role)
// }, &role)

// query.
func (this *Query) ListSimpleFor(closure func(), instances ...interface{}) error {
	return this.ListClosure(func(rows *sql.Rows) error {
		err := rows.Scan(instances...)
		if err != nil {
			return err
		}
		closure()
		return nil
	})
}

// the transformer will be responsible for creating  the result list
func (this *Query) ListClosure(transformer func(rows *sql.Rows) error) error {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.GetCachedSql()
	this.debugSQL(rsql.OriSql)

	now := time.Now()
	e := this.DmlBase.dba.QueryClosure(rsql.Sql, transformer, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now)
	if e != nil {
		return e
	}
	return nil
}

func (this *Query) ListSimple(transformer func(rows *sql.Rows) (interface{}, error)) ([]interface{}, error) {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.GetCachedSql()
	this.debugSQL(rsql.OriSql)

	now := time.Now()
	list, e := this.DmlBase.dba.Query(rsql.Sql, transformer, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now)
	if e != nil {
		return nil, e
	}
	return list, nil
}

// Executes a query and transform the results according to the transformer
//
// param query: The query
// param rowMapper: The row transformer
// return: A collection of transformed results
func (this *Query) List(rowMapper dbx.IRowTransformer) (coll.Collection, error) {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.GetCachedSql()
	this.debugSQL(rsql.OriSql)

	now := time.Now()
	list, e := this.DmlBase.dba.QueryCollection(rsql.Sql, rowMapper, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now)
	if e != nil {
		return nil, e
	}
	return list, nil
}

// Executes a query and transform the results to the struct type passed as parameter,<br>
// matching the alias with struct property name. If no alias is supplied, it is used the default column alias.
//
// param query: The query to be executed
// param klass: The struct
// return A slice of structs (needs cast)
func (this *Query) ListOf(instance interface{}) (coll.Collection, error) {
	return this.List(NewEntityTransformer(this, instance))
}

// since there is no return collection, this can be used also for non-toolkit.Hasher entities.
func (this *Query) ListFor(factory func() interface{}) error {
	_, err := this.List(NewEntityFactoryTransformer(this, factory))
	return err
}

// Executes a query and transform the results to the struct type,<br>
// matching the alias with struct property name, building a struct tree.
// If the transformed data matches a previous converted entity the previous one is reused.
//
// param query: The query to be executed
// param klass: The struct type
// return A collection of beans
func (this *Query) ListTreeOf(instance tk.Hasher) (coll.Collection, error) {
	return this.List(NewEntityTreeTransformer(this, true, instance))
}

// Executes a query and transform the results to the bean type,<br>
// matching the alias with bean property name, building a struct tree.
// A new instance is created for every new data type.
//
// param query: The query to be executed
// param klass: The struct type
// return A collection of beans
func (this *Query) ListFlatTreeOf(instance interface{}) (coll.Collection, error) {
	return this.List(NewEntityTreeTransformer(this, false, instance))
}

func (this *Query) ListFlatTreeFor(factory func() interface{}) (coll.Collection, error) {
	return this.List(NewEntityTreeFactoryTransformer(this, factory))
}

//	func (this *Query) <T> T selectSingle(Class<T> klass) {
//		return selectSingle(new BeanTransformer<T>(this, klass));
//	}

// the result of the query is put in the passed interface array.
// returns true if a result was found, false if no result
func (this *Query) SelectInto(dest ...interface{}) (bool, error) {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.GetCachedSql()
	this.debugSQL(rsql.OriSql)

	now := time.Now()
	found, e := this.dba.QueryRow(rsql.Sql, rsql.BuildValues(this.DmlBase.parameters), dest...)
	this.debugTime(now)
	if e != nil {
		return false, e
	}
	return found, nil
}

func (this *Query) SelectTree(typ interface{}, reuse bool) (interface{}, error) {
	if reuse {
		_, ok := typ.(tk.Hasher)
		if !ok {
			return nil, errors.New(fmt.Sprintf("When reuse is true, the type %T must implement toolkit.Hasher", typ))
		}

		list, err := this.List(NewEntityTreeTransformer(this, true, typ))
		if err != nil {
			return nil, err
		}

		if list.Size() == 0 {
			return nil, nil
		} else {
			return list.Enumerator().Next(), nil // first one
		}
	}

	return this.Select(NewEntityTreeTransformer(this, false, typ))
}

// the result of the query is put in the passed struct.
// returns true if a result was found, false if no result
func (this *Query) SelectTo(typ interface{}) (bool, error) {
	res, err := this.Select(NewEntityTransformer(this, typ))
	if err != nil {
		return false, err
	}
	if res != nil {
		tk.Set(typ, res)
		return true, nil
	}
	return false, nil
}

func (this *Query) SelectTreeTo(instance interface{}, reuse bool) (bool, error) {
	res, err := this.SelectTree(instance, reuse)
	if err != nil {
		return false, err
	}
	if res != nil {
		tk.Set(instance, res)
		return true, nil
	}
	return false, nil
}

//	func (this *Query) <T> T selectSingleTree(Class<T> klass) {
//		return selectSingleTree(klass, true);
//	}

func (this *Query) Select(rowMapper dbx.IRowTransformer) (interface{}, error) {
	oldMax := this.limit
	this.Limit(1)
	defer this.Limit(oldMax)

	list, err := this.List(rowMapper)
	if err != nil {
		return nil, err
	}

	if list.Size() == 0 {
		return nil, nil
	}
	return list.Enumerator().Next(), nil // first one
}

// SQL String. It is cached for multiple access
func (this *Query) GetCachedSql() *RawSql {
	if this.rawSQL == nil {
		// if the discriminator conditions have not yet been processed, apply them now
		if this.discriminatorCriterias != nil && this.criteria == nil {
			this.DmlBase.where(make([]*Criteria, 0)...)
		}

		sql := this.db.GetTranslator().GetSqlForQuery(this)
		this.rawSQL = ToRawSql(sql, this.db.GetTranslator())
	}

	return this.rawSQL
}
