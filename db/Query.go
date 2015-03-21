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

func (this *Query) CountAll() *Query {
	return this.Column(Count(nil))
}

func (this *Query) Count(column interface{}) *Query {
	return this.Column(Count(column))
}

func (this *Query) Column(column interface{}) *Query {
	this.lastToken = tokenizeOne(column)
	this.replaceRaw(this.lastToken)

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
	this.lastOrder = NewOrder(columnHolder)
	this.orders = append(this.orders, this.lastOrder)

	this.rawSQL = nil

	return this
}

// Order by a column belonging to the driving table.
func (this *Query) Order(column *Column) *Query {
	return this.OrderAs(column, this.tableAlias)
}

// Order by a column for a specific table alias.
func (this *Query) OrderAs(column *Column, alias string) *Query {
	ch := NewColumnHolder(column)
	if alias != "" {
		ch.SetTableAlias(alias)
	} else {
		ch.SetTableAlias(this.tableAlias)
	}

	return this.order(ch)
}

// Order by a column belonging to the table targeted by the supplyied association list.
func (this *Query) OrderOn(column *Column, associations ...*Association) *Query {
	pathElements := make([]*PathElement, len(associations))
	for k, association := range associations {
		pe := new(PathElement)
		pe.Base = association
		pe.Inner = false
		pathElements[k] = pe
	}

	return this.orderFor(column, pathElements...)
}

// Defines the column, belonging to the table targeted by the association, to order by.
func (this *Query) orderFor(column *Column, pathElements ...*PathElement) *Query {
	var pes []*PathElement
	pes = pathElements

	common := DeepestCommonPath(this.cachedAssociation, pes)
	if len(common) == len(pes) {
		return this.OrderAs(column, pathElementAlias(common[len(common)-1]))
	}

	panic("The path specified in the order is not valid")
}

//Defines the column to order by.
//The column belongs to the table targeted by the last defined association.
//If there is no last association, the column belongs to the driving table
func (this *Query) OrderBy(column *Column) *Query {
	if this.path != nil {
		last := this.path[len(this.path)-1]
		if last.Orders == nil {
			last.Orders = make([]*Order, 0)
		}
		// delay adding order
		this.lastOrder = NewOrder(NewColumnHolder(column))
		last.Orders = append(last.Orders, this.lastOrder)
		return this
	} else if this.lastJoin != nil {
		return this.orderFor(column, this.lastJoin.GetPathElements()...)
	} else {
		return this.OrderAs(column, this.lastFkAlias)
	}
}

//Defines the column alias to order by.
func (this *Query) OrderByAs(column string) *Query {
	this.lastOrder = NewOrderAs(column).Asc(true)
	this.orders = append(this.orders, this.lastOrder)

	this.rawSQL = nil

	return this
}

func (this *Query) Asc() *Query {
	return this.Dir(true)
}

func (this *Query) Desc() *Query {
	return this.Dir(false)
}

// Sets the order direction for the last order by command. true=asc, false=desc
func (this *Query) Dir(asc bool) *Query {
	if this.lastOrder != nil {
		this.lastOrder.Asc(asc)

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

//This will trigger a result that can be dumped in a tree object
//using current association path to build the tree result.
//
//It will includes all the columns of all the tables referred by the association path,
// except where columns were explicitly included.
func (this *Query) Fetch() *Query {
	return this.FetchTo("")
}

//The as Fetch() but using an end alias.
func (this *Query) FetchTo(endAlias string) *Query {
	if this.path != nil {
		for _, pe := range this.path {
			this.includeInPath(pe) // includes all columns
		}
	}

	this.joinTo(endAlias, true)

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
	this.joinTo(endAlias, false)
	return this
}

func (this *Query) joinTo(endAlias string, fetch bool) {
	if this.path != nil {
		tokens := make([]Tokener, 0)
		for _, pe := range this.path {
			funs := pe.Columns
			if funs != nil {
				for _, fun := range funs {
					tokens = append(tokens, fun)
					if !fetch {
						fun.SetPseudoTableAlias(this.tableAlias)
					}
				}
			}
		}

		this.Columns = append(this.Columns, tokens...)
	}

	// only after this the joins will have the proper join table alias
	this.DmlBase.joinTo(endAlias, this.path, fetch)

	// process pending orders
	if this.path != nil {
		for _, pe := range this.path {
			if pe.Orders != nil {
				for _, o := range pe.Orders {
					o.column.SetTableAlias(pathElementAlias(pe))
					this.orders = append(this.orders, o)
				}
			}
		}
	}

	this.path = nil
	this.rawSQL = nil
}

func pathElementAlias(pe *PathElement) string {
	derived := pe.Derived
	if derived.IsMany2Many() {
		return derived.ToM2M.GetAliasTo()
	} else {
		return derived.GetAliasTo()
	}
}

//adds tokens refering the last defined association
func (this *Query) Include(columns ...interface{}) *Query {
	lenPath := len(this.path)
	if lenPath > 0 {
		lastPath := this.path[lenPath-1]
		this.includeInPath(lastPath, columns...)

		this.rawSQL = nil
	} else {
		panic("There is no current join")
	}
	return this
}

func (this *Query) includeInPath(lastPath *PathElement, columns ...interface{}) {
	if len(columns) > 0 || len(lastPath.Columns) == 0 {
		if len(columns) == 0 {
			// use all columns of the targeted table
			columns = lastPath.Base.GetTableTo().GetColumns().Elements()
		}
		if lastPath.Columns == nil {
			lastPath.Columns = make([]Tokener, 0)
		}
		for _, c := range columns {
			this.lastToken = tokenizeOne(c)
			lastPath.Columns = append(lastPath.Columns, this.lastToken)
		}
	}
}

//Restriction to apply to the previous association
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

//Adds a Having clause to the query.
//The tokens are not processed. You will have to explicitly set all table alias.
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

//List simple variables.
//A closure is used to build the result list.
//The types for scanning are supplied by the instances parameter.
//No reflection is used.
//
//ex:
//  roles = make([]string, 0)
//  var role string
//  q.ListSimple(func() {
//  	roles = append(roles, role)
//  }, &role)
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

//List using the closure arguments.
//A function is used to build the result list.
//The types for scanning are supplied by the function arguments. Arguments can be pointers or not.
//Reflection is used to determine the arguments types.
//The argument can also be a function with the signature func(*struct).
//The results will not be assembled in as a tree.
//
//ex:
//  roles = make([]string, 0)
//  var role string
//  q.ListInto(func(role *string) {
//	  roles = append(roles, *role)
//  })
func (this *Query) ListInto(closure interface{}) ([]interface{}, error) {
	// determine types and instanciate them
	ftype := reflect.TypeOf(closure)
	if ftype.Kind() != reflect.Func {
		return nil, fmt.Errorf("goSQL: Expected a function with the signature func(*struct) [*struct] or func(primitive1, ..., primitiveN) [anything]. Got %s.", ftype.String())
	}

	caller, typ, ok := checkCollector(closure)
	if ok {
		coll, err := this.list(NewEntityFactoryTransformer(this, typ, caller))
		if err != nil {
			return nil, err
		}
		return coll.Elements(), nil
	} else {
		return this.listIntoClosure(closure)
	}
}

// the transformer will be responsible for creating  the result list
func (this *Query) listIntoClosure(transformer interface{}) ([]interface{}, error) {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql, 2)

	now := time.Now()
	r, e := this.DmlBase.dba.QueryInto(rsql.Sql, transformer, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now, 2)
	if e != nil {
		return nil, e
	}
	return r, nil
}

// the transformer will be responsible for creating  the result list
func (this *Query) listClosure(transformer func(rows *sql.Rows) error) error {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql, 2)

	now := time.Now()
	e := this.DmlBase.dba.QueryClosure(rsql.Sql, transformer, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now, 2)
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
	this.debugSQL(rsql.OriSql, 2)

	now := time.Now()
	list, e := this.DmlBase.dba.Query(rsql.Sql, transformer, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now, 2)
	if e != nil {
		return nil, e
	}
	return list, nil
}

//Executes a query and transform the results according to the transformer
//Accepts a row transformer and returns a collection of transformed results
func (this *Query) list(rowMapper dbx.IRowTransformer) (coll.Collection, error) {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql, 2)

	now := time.Now()
	list, e := this.DmlBase.dba.QueryCollection(rsql.Sql, rowMapper, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now, 2)
	if e != nil {
		return nil, e
	}
	return list, nil
}

//Executes a query and transform the results to the struct type passed as parameter,
//matching the alias with struct property name. If no alias is supplied, it is used the default column alias.
//
//Accepts as parameter the struct type and returns a collection of structs (needs cast)
func (this *Query) ListOf(template interface{}) (coll.Collection, error) {
	return this.list(NewEntityTransformer(this, template))
}

// Executes a query and transform the results into a tree with the passed struct type as the head.
// It matches the alias with struct property name, building a struct tree.
// If the transformed data matches a previous converted entity the previous one is reused.
//
// Receives a template instance and returns a collection of structs.
func (this *Query) ListTreeOf(template tk.Hasher) (coll.Collection, error) {
	return this.list(NewEntityTreeTransformer(this, true, template))
}

//Executes a query, putting the result in a slice, passed as an argument
//
//This method does not create a tree of related instances.
func (this *Query) List(target interface{}) error {
	caller, typ, ok := checkSlice(target)
	if !ok {
		return errors.New(fmt.Sprintf("goSQL: Expected a slice of type *[]*struct. Got %s", typ.String()))
	}

	_, err := this.list(NewEntityFactoryTransformer(this, typ, caller))
	return err
}

func checkSlice(i interface{}) (func(val reflect.Value) reflect.Value, reflect.Type, bool) {
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

	ptrElem := (typ.Kind() == reflect.Ptr)
	if !ptrElem {
		typ = reflect.PtrTo(typ) // get the pointer
	}

	slice := reflect.New(arr.Type()).Elem()
	slicer := func(val reflect.Value) reflect.Value {
		var v reflect.Value
		// slice elements are pointers
		if ptrElem {
			// if pointer type use directly
			v = val
		} else {
			// use underlying value of the pointer
			v = val.Elem()
		}

		slice = reflect.Append(slice, v)
		arr.Set(slice)
		return reflect.Value{}
	}

	return slicer, typ, true
}

func checkCollector(collector interface{}) (func(val reflect.Value) reflect.Value, reflect.Type, bool) {
	var typ reflect.Type
	funcValue := reflect.ValueOf(collector)
	functype := funcValue.Type()
	bad := true
	var isPtr bool
	if functype.NumIn() == 1 {
		typ = functype.In(0)
		if typ.Kind() == reflect.Struct {
			typ = reflect.PtrTo(typ) // get the pointer
			bad = false
		} else if typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Struct {
			isPtr = true
			bad = false
		}
	}

	if functype.NumOut() > 1 {
		bad = true
	} else if functype.NumOut() == 1 {
		typOut := functype.Out(0)
		if !(typOut.Kind() == reflect.Struct || typOut.Kind() == reflect.Ptr && typOut.Elem().Kind() == reflect.Struct) {
			bad = true
		}
	}

	if bad {
		return nil, nil, false
	}

	caller := func(val reflect.Value) reflect.Value {
		var v reflect.Value
		// slice elements are pointers
		if isPtr {
			// if pointer type use directly
			v = val
		} else {
			// use underlying value of the pointer
			v = val.Elem()
		}

		results := funcValue.Call([]reflect.Value{v})
		if len(results) > 0 {
			return results[0]
		} else {
			return reflect.Value{}
		}
	}

	return caller, typ, true
}

//Executes a query and transform the results into a flat tree with the passed struct type as the head.
//It matches the alias with struct property name, building a struct tree.
//There is no reuse of previous converted entites.
//
//Receives a template instance and returns a collection of structs.
func (this *Query) ListFlatTreeOf(template interface{}) (coll.Collection, error) {
	return this.list(NewEntityTreeTransformer(this, false, template))
}

//Executes a query, putting the result in a slice, passed as an argument or
//delegating the responsability of building the result to a processor function.
//The argument must be a function with the signature func(<<*>struct>) or a slice like *[]<*>struct.
//See also List.
func (this *Query) ListFlatTree(target interface{}) error {
	caller, typ, ok := checkSlice(target)
	if !ok {
		caller, typ, ok = checkCollector(target)
		if !ok {
			return errors.New(fmt.Sprintf("goSQL: Expected a slice of type *[]<*>struct or a function with the signature func(<<*>struct>). got %s", typ.String()))
		}
	}

	_, err := this.list(NewEntityTreeFactoryTransformer(this, typ, caller))
	return err
}

// the result of the query is put in the passed interface array.
// returns true if a result was found, false if no result
func (this *Query) SelectInto(dest ...interface{}) (bool, error) {
	// if no columns were added, add all columns of the driving table
	if len(this.Columns) == 0 {
		this.All()
	}

	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql, 1)

	now := time.Now()
	found, e := this.dba.QueryRow(rsql.Sql, rsql.BuildValues(this.DmlBase.parameters), dest...)
	this.debugTime(now, 1)
	if e != nil {
		return false, e
	}
	return found, nil
}

//Returns a struct tree. When reuse is true the supplied template instance must implement
//the toolkit.Hasher interface.
//
//This is pretty much the same as SelectTreeTo.
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

//The first result of the query is put in the passed struct.
//Returns true if a result was found, false if no result
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

//Executes the query and builds a struct tree, reusing previously obtained entities,
//putting the first element in the supplied struct pointer.
//Since the struct instances are going to be reused it is mandatory that all the structs
//participating in the result tree implement the toolkit.Hasher interface.
//Returns true if a result was found, false if no result.
//See also SelectFlatTree.
func (this *Query) SelectTree(instance tk.Hasher) (bool, error) {
	return this.selectTreeTo(instance, true)
}

//Executes the query and builds a flat struct tree putting the first element in the supplied struct pointer.
//Since the struct instances are not going to be reused it is not mandatory that the structs implement the toolkit.Hasher interface.
//Returns true if a result was found, false if no result.
//See also SelectTree.
func (this *Query) SelectFlatTree(instance interface{}) (bool, error) {
	return this.selectTreeTo(instance, false)
}

//Executes the query and builds a struct tree putting the first element in the supplied struct pointer.
//
//If the reuse parameter is true, when a
//new entity is needed, the cache is checked to see if there is one instance for this entity,
//and if found it will use it to build the tree. Because of this the supplied instance
//must implement the toolkit.Hasher interface.
//
//If the reuse parameter is false, each element of the tree is always a new instance
//even if representing the same entity. This is most useful for tabular results.
//Since there is no need for caching the entities it is not mandatory to implement
//the toolkit.Hasher interface.
//
//The first result of the query is put in the passed struct.
//Returns true if a result was found, false if no result
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
