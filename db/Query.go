package db

import (
	"github.com/quintans/goSQL/dbx"
	tk "github.com/quintans/toolkit"
	coll "github.com/quintans/toolkit/collection"

	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"
)

type Group struct {
	Position int
	Token    Tokener
}

const OFFSET_PARAM = "OFFSET_PARAM"
const LIMIT_PARAM = "LIMIT_PARAM"

type PostRetriver interface {
	PostRetrive(store IDb)
}

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
	having    *Criteria
	skip      int64
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

	this.skip = other.skip
	this.limit = other.limit

	this.rawSQL = other.rawSQL
}

func (this *Query) GetSkip() int64 {
	return this.skip
}

func (this *Query) Skip(skip int64) *Query {
	if skip < 0 {
		this.skip = 0
	} else {
		this.skip = skip
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
	this.lastToken, _ = tokenizeOne(column)
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
		this.DmlBase.where(restriction)
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

/*
Order by a column belonging to the driving table.
*/
func (this *Query) Order(column *Column) *Query {
	return this.OrderAs(column, this.tableAlias)
}

/*
Order by a column for a specific table alias.
*/
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

/*
Order by a column belonging to the table targeted by the supplyied association list.
*/
func (this *Query) OrderOn(column *Column, associations ...*Association) *Query {
	pathElements := make([]*PathElement, len(associations))
	for k, association := range associations {
		pe := new(PathElement)
		pe.Base = association
		pe.Inner = false
		pathElements[k] = pe
	}

	return this.OrderFor(column, pathElements...)
}

/*
Defines the column, belonging to the table targeted by the association, to order by.
*/
func (this *Query) OrderFor(column *Column, pathElements ...*PathElement) *Query {
	var pes []*PathElement
	if column.IsVirtual() {
		// appending the path of the virtual column
		ch := NewColumnHolder(column)
		discriminator := ch.GetColumn().GetVirtual().Association
		pe := new(PathElement)
		pe.Base = discriminator
		pe.Inner = false
		pes = append(pes, pe)
	} else {
		pes = pathElements
	}

	common := DeepestCommonPath(this.cachedAssociation, pes)
	if len(common) == len(pes) {
		var orderAlias string
		derived := common[len(common)-1].Derived
		if derived.IsMany2Many() {
			orderAlias = derived.ToM2M.GetAliasTo()
		} else {
			orderAlias = derived.GetAliasTo()
		}
		return this.OrderAs(column, orderAlias)
	}

	panic("The path specified in the order is not valid")
}

/*
Defines the column to order by.
The column belongs to the table targeted by the last defined association.
If there is no last association, the column belongs to the driving table
*/
func (this *Query) OrderBy(column *Column) *Query {
	if this.path != nil {
		// delay adding order
		last := this.path[len(this.path)-1]
		this.lastOrder = NewOrder(NewColumnHolder(column))
		last.Orders = append(last.Orders, this.lastOrder)
		return this
	} else if this.lastJoin != nil {
		return this.OrderFor(column, this.lastJoin.GetPathElements()...)
	} else {
		return this.OrderAs(column, this.lastFkAlias)
	}
}

/*
Defines the column alias to order by.
*/
func (this *Query) OrderByAs(column string) *Query {
	this.lastOrder = NewOrderAs(column).Asc(true)
	this.orders = append(this.orders, this.lastOrder)

	this.rawSQL = nil

	return this
}

// Sets the order direction for the last order by command
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
		pe := new(PathElement)
		pe.Base = association
		pe.Inner = false
		this.path = append(this.path, pe)
	}

	this.rawSQL = nil

	return this
}

func (this *Query) Fetch() *Query {
	return this.FetchTo("")
}

/*
Include in the select ALL columns of the tables paticipating in the current association chain.
A table end alias can also be supplied.
*/
func (this *Query) FetchTo(endAlias string) *Query {
	if len(this.path) > 0 {
		this.fetch(endAlias, this.path...)

		pathCriterias := this.buildPathCriterias(this.path)
		// process the acumulated conditions
		var firstCriterias []*Criteria
		for index, pathCriteria := range pathCriterias {
			if pathCriteria != nil {
				conds := pathCriteria.Criterias
				if conds != nil {
					// index == 0 applies to the starting table
					if index == 0 {
						// already with the alias applied
						firstCriterias = conds
					} else {
						if firstCriterias != nil {
							// add the criterias restriction refering to the table,
							// due to association discriminator
							tmp := make([]*Criteria, len(conds))
							copy(tmp, conds)
							conds = append(tmp, firstCriterias...)
							firstCriterias = nil
						}
						this.applyOn(this.path[:index], And(conds...))
					}
				}
			}
		}

		// apply orders
		for k, p := range this.path {
			if len(p.Orders) > 0 {
				for _, o := range p.Orders {
					this.OrderFor(o.column.GetColumn(), this.path[:k+1]...)
				}
			}
		}
	}
	this.path = nil

	this.rawSQL = nil

	return this
}

func (this *Query) Join() *Query {
	return this.JoinTo("")
}

//indicates that the path should be used to join only
//
//param endAlias:
//return
func (this *Query) JoinTo(endAlias string) *Query {
	this.DmlBase.joinTo(endAlias, this.path)
	if len(this.path) > 0 {
		// apply orders
		for k, p := range this.path {
			if len(p.Orders) > 0 {
				for _, o := range p.Orders {
					this.OrderFor(o.column.GetColumn(), this.path[:k+1]...)
				}
			}
		}
	}
	this.path = nil
	this.rawSQL = nil
	return this
}

/*
 adds tokens refering the last defined association
*/
func (this *Query) Include(columns ...interface{}) *Query {
	if len(this.path) > 0 {
		var isNew bool
		// create tokens from the columns
		tokens := make([]Tokener, len(columns), len(columns))
		for k, c := range columns {
			this.lastToken, isNew = tokenizeOne(c)
			if !isNew {
				this.lastToken = this.lastToken.Clone().(Tokener)
			}
			tokens[k] = this.lastToken
		}
		// append the tokens to previously added tokens
		toks := this.path[len(this.path)-1].Columns
		if toks == nil {
			toks = make([]Tokener, 0)
		}
		this.path[len(this.path)-1].Columns = append(toks, tokens...)
		this.Columns = append(this.Columns, tokens...)

		this.rawSQL = nil
	} else {
		panic("There is no current join")
	}
	return this
}

func (this *Query) fetch(endAlias string, pathElements ...*PathElement) *Query {
	//the current path
	var currentPath []*PathElement

	common := DeepestCommonPath(this.cachedAssociation, pathElements)

	var pos int
	// finds the ForeignKey's that are not present in any join
	for f, pe := range pathElements {
		if f < len(common) {
			if !common[f].Base.Equals(pe.Base) {
				pos = f
				break
			}
		} else {
			pos = f
			break
		}

		currentPath = append(currentPath, common[f])
	}

	// returns a list with the old ones (currentPath) + the new ones (with the alias already defined)
	local := this.addJoin(endAlias, pathElements, common, true)
	// remove old ones, keeping the new ones
	local = local[pos:]

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

/*
Restriction to apply to the previous association
*/
func (this *Query) On(criteria ...*Criteria) *Query {
	if len(this.path) > 0 {
		var retriction *Criteria
		if len(criteria) > 1 {
			retriction = And(criteria...)
		} else if len(criteria) == 1 {
			retriction = criteria[0]
		} else {
			panic("nil or empty criterias was passed")
		}
		this.path[len(this.path)-1].Criteria = retriction

		this.rawSQL = nil
	} else {
		panic("There is no current join")
	}
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

func (this *Query) GetGroupByTokens() []Group {
	var groups []Group
	length := len(this.groupBy)
	if length > 0 {
		groups = make([]Group, length)
		for k, idx := range this.groupBy {
			groups[k].Position = idx - 1
			groups[k].Token = this.Columns[idx-1]
		}
	}
	return groups
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

/*
Adds a Having clause to the query.
The tokens are not processed. You will have to explicitly set all table alias.
*/
func (this *Query) Having(having ...*Criteria) *Query {
	if len(having) > 0 {
		this.having = And(having...)
		this.replaceAlias(this.having)
	}

	return this
}

func (this *Query) GetHaving() *Criteria {
	return this.having
}

// replaces ALIAS with the respective select parcel
func (this *Query) replaceAlias(token Tokener) {
	members := token.GetMembers()
	if token.GetOperator() == TOKEN_ALIAS {
		alias := token.GetValue().(string)
		for _, v := range this.Columns {
			// full copies the matching
			if v.GetAlias() == alias {
				token.SetAlias(alias)
				token.SetMembers(v.GetMembers()...)
				token.SetOperator(v.GetOperator())
				token.SetTableAlias(v.GetTableAlias())
				token.SetValue(v.GetValue())
				break
			}
		}
		return
	} else {
		if members != nil {
			for _, t := range members {
				if t != nil {
					this.replaceAlias(t)
				}
			}
		}
	}
}

// ======== RETRIVE ==============

/*
List simple variables.
A closure is used to build the result list.
The types for scanning are supplied by the instances parameter.
No reflection is used.

ex:
roles = make([]string, 0)
var role string
q.ListSimple(func() {
	roles = append(roles, role)
}, &role)
*/
func (this *Query) ListSimple(closure func(), instances ...interface{}) error {
	return this.listClosure(func(rows *sql.Rows) error {
		err := rows.Scan(instances...)
		if err != nil {
			return err
		}
		closure()
		return nil
	})
}

// the transformer will be responsible for creating  the result list
func (this *Query) listClosure(transformer func(rows *sql.Rows) error) error {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql)

	now := time.Now()
	e := this.DmlBase.dba.QueryClosure(rsql.Sql, transformer, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now)
	if e != nil {
		return e
	}
	return nil
}

func (this *Query) listSimpleTransformer(transformer func(rows *sql.Rows) (interface{}, error)) ([]interface{}, error) {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql)

	now := time.Now()
	list, e := this.DmlBase.dba.Query(rsql.Sql, transformer, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now)
	if e != nil {
		return nil, e
	}
	return list, nil
}

/*
Executes a query and transform the results according to the transformer

Accepts a row transformer and returns a collection of transformed results
*/
func (this *Query) list(rowMapper dbx.IRowTransformer) (coll.Collection, error) {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql)

	now := time.Now()
	list, e := this.DmlBase.dba.QueryCollection(rsql.Sql, rowMapper, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now)
	if e != nil {
		return nil, e
	}
	return list, nil
}

/*
Executes a query and transform the results to the struct type passed as parameter,
matching the alias with struct property name. If no alias is supplied, it is used the default column alias.

Accepts as parameter the struct type and returns a collection of structs (needs cast)
*/
func (this *Query) ListOf(template interface{}) (coll.Collection, error) {
	return this.list(NewEntityTransformer(this, template))
}

/*
Executes a query, putting the result in a slice, passed as an argument or
delegating the responsability of building the result to a processor function.
The argument must be a function with the signature func(*struct) or a slice like *[]*struct.

This method does not create a tree of related instances.
*/
func (this *Query) List(target interface{}) error {
	caller, typ, ok := checkSlice(target)
	if !ok {
		caller, typ, ok = checkCollector(target)
		if !ok {
			return errors.New(fmt.Sprintf("goSQL: Expected an slice of type *[]*struct or a function with the signature func(<*struct>). got %s", typ.String()))
		}
	}

	_, err := this.list(NewEntityFactoryTransformer(this, typ, caller))
	return err
}

func checkSlice(i interface{}) (func(val reflect.Value), reflect.Type, bool) {
	arr := reflect.ValueOf(i)
	// pointer to the slice
	if arr.Kind() == reflect.Ptr {
		arr = arr.Elem()
	} else {
		return nil, nil, false
	}

	// slice element
	var typ reflect.Type
	if arr.Kind() == reflect.Slice {
		typ = arr.Type().Elem()
	} else {
		return nil, nil, false
	}

	// element element
	if typ.Kind() != reflect.Ptr {
		return nil, nil, false
	}

	slice := reflect.New(arr.Type()).Elem()
	slicer := func(val reflect.Value) {
		slice = reflect.Append(slice, val)
		arr.Set(slice)
	}

	return slicer, typ, true
}

func checkCollector(collector interface{}) (func(val reflect.Value), reflect.Type, bool) {
	var typ reflect.Type
	funcValue := reflect.ValueOf(collector)
	functype := funcValue.Type()
	bad := true
	if functype.NumIn() == 1 {
		typ = functype.In(0)
		if typ.Kind() == reflect.Struct || typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Struct {
			bad = false
		}
	}

	if functype.NumOut() != 0 {
		bad = true
	}

	if bad {
		return nil, nil, false
	}

	caller := func(val reflect.Value) {
		funcValue.Call([]reflect.Value{val})
	}

	return caller, typ, true
}

/*
Executes a query and transform the results into a tree with the passed struct type as the head.
It matches the alias with struct property name, building a struct tree.
If the transformed data matches a previous converted entity the previous one is reused.

Receives a template instance and returns a collection of structs.
*/
func (this *Query) ListTreeOf(template tk.Hasher) (coll.Collection, error) {
	return this.list(NewEntityTreeTransformer(this, true, template))
}

/*
Executes a query and transform the results into a flat tree with the passed struct type as the head.
It matches the alias with struct property name, building a struct tree.
If the transformed data matches a previous converted entity the previous one is reused.

Receives a template instance and returns a collection of structs.
*/
func (this *Query) ListFlatTreeOf(template interface{}) (coll.Collection, error) {
	return this.list(NewEntityTreeTransformer(this, false, template))
}

/*
Executes a query, putting the result in a slice, passed as an argument or
delegating the responsability of building the result to a processor function.
The argument must be a function with the signature func(*struct) or a slice like *[]*struct.
See also List.
*/
func (this *Query) ListFlatTree(target interface{}) error {
	caller, typ, ok := checkSlice(target)
	if !ok {
		caller, typ, ok = checkCollector(target)
		if !ok {
			return errors.New(fmt.Sprintf("goSQL: Expected an slice of type *[]*struct or a function with the signature func(<*struct>). got %s", typ.String()))
		}
	}

	_, err := this.list(NewEntityTreeFactoryTransformer(this, typ, caller))
	return err
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

	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql)

	now := time.Now()
	found, e := this.dba.QueryRow(rsql.Sql, rsql.BuildValues(this.DmlBase.parameters), dest...)
	this.debugTime(now)
	if e != nil {
		return false, e
	}
	return found, nil
}

/*
Returns a struct tree. When reuse is true the supplied template instance must implement
the toolkit.Hasher interface.

This is pretty much the same as SelectTreeTo.
*/
func (this *Query) selectTree(typ interface{}, reuse bool) (interface{}, error) {
	if reuse {
		_, ok := typ.(tk.Hasher)
		if !ok {
			return nil, errors.New(fmt.Sprintf("When reuse is true, the type %T must implement toolkit.Hasher", typ))
		}

		list, err := this.list(NewEntityTreeTransformer(this, true, typ))
		if err != nil {
			return nil, err
		}

		if list.Size() == 0 {
			return nil, nil
		} else {
			return list.Enumerator().Next(), nil // first one
		}
	}

	return this.selectTransformer(NewEntityTreeTransformer(this, false, typ))
}

/*
The first result of the query is put in the passed struct.
Returns true if a result was found, false if no result
*/
func (this *Query) SelectTo(typ interface{}) (bool, error) {
	res, err := this.selectTransformer(NewEntityTransformer(this, typ))
	if err != nil {
		return false, err
	}
	if res != nil {
		tk.Set(typ, res)
		return true, nil
	}
	return false, nil
}

/*
Executes the query and builds a struct tree, reusing previously obtained entities,
putting the first element in the supplied struct pointer.
Since the struct instances are going to be reused it is mandatory that all the structs
participating in the result tree implement the toolkit.Hasher interface.
Returns true if a result was found, false if no result.
See also SelectFlatTree.
*/
func (this *Query) SelectTree(instance tk.Hasher) (bool, error) {
	return this.selectTreeTo(instance, true)
}

/*
Executes the query and builds a flat struct tree putting the first element in the supplied struct pointer.
Since the struct instances are not going to be reused it is not mandatory that the structs implement the toolkit.Hasher interface.
Returns true if a result was found, false if no result.
See also SelectTree.
*/
func (this *Query) SelectFlatTree(instance interface{}) (bool, error) {
	return this.selectTreeTo(instance, false)
}

/*
Executes the query and builds a struct tree putting the first element in the supplied struct pointer.

If the reuse parameter is true, when a
new entity is needed, the cache is checked to see if there is one instance for this entity,
and if found it will use it to build the tree. Because of this the supplied instance
must implement the toolkit.Hasher interface.

If the reuse parameter is false, each element of the tree is always a new instance
even if representing the same entity. This is most useful for tabular results.
Since there is no need for caching the entities it is not mandatory to implement
the toolkit.Hasher interface.

The first result of the query is put in the passed struct.
Returns true if a result was found, false if no result
*/
func (this *Query) selectTreeTo(instance interface{}, reuse bool) (bool, error) {
	res, err := this.selectTree(instance, reuse)
	if err != nil {
		return false, err
	}
	if res != nil {
		tk.Set(instance, res)
		return true, nil
	}
	return false, nil
}

func (this *Query) selectTransformer(rowMapper dbx.IRowTransformer) (interface{}, error) {
	oldMax := this.limit
	this.Limit(1)
	defer this.Limit(oldMax)

	list, err := this.list(rowMapper)
	if err != nil {
		return nil, err
	}

	if list.Size() == 0 {
		return nil, nil
	}
	return list.Enumerator().Next(), nil // first one
}

// SQL String. It is cached for multiple access
func (this *Query) getCachedSql() *RawSql {
	if this.rawSQL == nil {
		// if the discriminator conditions have not yet been processed, apply them now
		if this.discriminatorCriterias != nil && this.criteria == nil {
			this.DmlBase.where(nil)
		}

		sql := this.db.GetTranslator().GetSqlForQuery(this)
		this.rawSQL = ToRawSql(sql, this.db.GetTranslator())
	}

	return this.rawSQL
}
