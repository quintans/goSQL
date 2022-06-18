package db

import (
	"database/sql"
	"reflect"

	"github.com/quintans/faults"
	"github.com/quintans/goSQL/dbx"
	tk "github.com/quintans/toolkit"
	coll "github.com/quintans/toolkit/collections"
)

type Group struct {
	Position int
	Token    Tokener
}

const (
	OFFSET_PARAM = "OFFSET_PARAM"
	LIMIT_PARAM  = "LIMIT_PARAM"
)

type PostRetriever interface {
	PostRetrieve(store IDb)
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

	err error
}

func NewQuery(db IDb, table *Table) *Query {
	this := new(Query)
	this.DmlBase.Super(db, table)
	return this
}

func (q *Query) Alias(alias string) *Query {
	q.alias(alias)
	return q
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

func (q *Query) All() *Query {
	if q.err != nil {
		return q
	}

	if q.table != nil {
		for it := q.table.columns.Enumerator(); it.HasNext(); {
			q.Column(it.Next().(*Column))
		}
	}
	return q
}

func (q *Query) Copy(other *Query) {
	q.table = other.table
	q.tableAlias = other.tableAlias

	if other.GetJoins() != nil {
		q.joins = make([]*Join, len(other.joins))
		copy(q.joins, other.joins)
	}
	if other.criteria != nil {
		q.criteria, _ = other.criteria.Clone().(*Criteria)
	}
	if q.parameters != nil {
		for k, v := range other.parameters {
			q.parameters[k] = v
		}
	}

	if other.subQuery != nil {
		sq := other.subQuery
		q.subQuery = NewQuery(q.db, sq.table)
		q.subQuery.Copy(sq)
		q.subQueryAlias = other.subQueryAlias
	}

	q.distinct = other.distinct
	if other.Columns != nil {
		q.Columns = make([]Tokener, len(other.Columns))
		copy(q.Columns, other.Columns)
	}
	if other.orders != nil {
		q.orders = make([]*Order, len(other.orders))
		copy(q.orders, other.orders)
	}
	if other.unions != nil {
		q.unions = make([]*Union, len(other.unions))
		copy(q.unions, other.unions)
	}
	// saves position of columnHolder
	if other.groupBy != nil {
		q.groupBy = make([]int, len(other.groupBy))
		copy(q.groupBy, other.groupBy)
	}

	q.skip = other.skip
	q.limit = other.limit

	q.rawSQL = other.rawSQL
}

func (q *Query) GetSkip() int64 {
	return q.skip
}

func (q *Query) Skip(skip int64) *Query {
	if q.err != nil {
		return q
	}

	if skip < 0 {
		q.skip = 0
	} else {
		q.skip = skip
	}
	return q
}

func (q *Query) GetLimit() int64 {
	return q.limit
}

func (q *Query) Limit(limit int64) *Query {
	if q.err != nil {
		return q
	}

	if limit < 0 {
		q.limit = 0
	} else {
		q.limit = limit
	}
	return q
}

func (q *Query) GetSubQuery() *Query {
	return q.subQuery
}

func (q *Query) GetSubQueryAlias() string {
	return q.subQueryAlias
}

func (q *Query) Distinct() *Query {
	if q.err != nil {
		return q
	}

	q.distinct = true
	q.rawSQL = nil
	return q
}

func (q *Query) IsDistinct() bool {
	return q.distinct
}

// COL ===

func (q *Query) ColumnsReset() {
	q.Columns = nil
}

func (q *Query) CountAll() *Query {
	if q.err != nil {
		return q
	}
	return q.Column(Count(nil))
}

func (q *Query) Count(column interface{}) *Query {
	if q.err != nil {
		return q
	}

	return q.Column(Count(column))
}

func (q *Query) Column(columns ...interface{}) *Query {
	if q.err != nil {
		return q
	}

	for _, column := range columns {
		q.lastToken = tokenizeOne(column)
		q.replaceRaw(q.lastToken)

		q.lastToken.SetTableAlias(q.tableAlias)
		q.Columns = append(q.Columns, q.lastToken)
	}

	q.rawSQL = nil

	return q
}

// Defines the alias of the last column
// param alias: The Alias
// return: The query
func (q *Query) As(alias string) *Query {
	if q.err != nil {
		return q
	}

	if q.lastToken != nil {
		q.lastToken.SetAlias(alias)
	} else if q.path != nil {
		q.path[len(q.path)-1].PreferredAlias = alias
	}

	q.rawSQL = nil

	return q
}

// WHERE ===
func (q *Query) Where(restriction ...*Criteria) *Query {
	if q.err != nil {
		return q
	}

	if len(restriction) > 0 {
		q.DmlBase.where(restriction)
	}
	return q
}

// ===

// ORDER ===
func (q *Query) OrdersReset() {
	q.orders = nil
}

// Order by a column for a specific table alias.
//
// use: query.OrderAs(Column.For("x"))
func (q *Query) OrderAs(columnHolder *ColumnHolder) *Query {
	if q.err != nil {
		return q
	}

	q.lastOrder = NewOrder(columnHolder)
	q.orders = append(q.orders, q.lastOrder)

	q.rawSQL = nil

	return q
}

// Order by a column belonging to the driving table.
func (q *Query) Order(column *Column) *Query {
	if q.err != nil {
		return q
	}

	return q.OrderAs(column.For(q.tableAlias))
}

// Order by a column belonging to the table targeted by the supplyied association list.
func (q *Query) OrderOn(column *Column, associations ...*Association) *Query {
	if q.err != nil {
		return q
	}

	pathElements := make([]*PathElement, len(associations))
	for k, association := range associations {
		pe := new(PathElement)
		pe.Base = association
		pe.Inner = false
		pathElements[k] = pe
	}

	// Defines the column, belonging to the table targeted by the association, to order by
	pes := pathElements

	common := DeepestCommonPath(q.cachedAssociation, pes)
	if len(common) == len(pes) {
		return q.OrderAs(column.For(pathElementAlias(common[len(common)-1])))
	}

	return &Query{
		err: faults.New("the path specified in the order is not valid"),
	}
}

// Defines the column to order by.
// The column belongs to the table targeted by the last defined association.
// If there is no last association, the column belongs to the driving table
func (q *Query) OrderBy(column *Column) *Query {
	if q.err != nil {
		return q
	}

	if q.path != nil {
		last := q.path[len(q.path)-1]
		if last.Orders == nil {
			last.Orders = make([]*Order, 0)
		}
		// delay adding order
		q.lastOrder = NewOrder(NewColumnHolder(column))
		last.Orders = append(last.Orders, q.lastOrder)
		return q
	} else if q.lastJoin != nil {
		return q.OrderAs(column.For(q.lastFkAlias))
	} else {
		return q.OrderAs(column.For(q.tableAlias))
	}
}

// Defines the column alias to order by.
func (q *Query) OrderByAs(column string) *Query {
	if q.err != nil {
		return q
	}

	q.lastOrder = NewOrderAs(column).Asc(true)
	q.orders = append(q.orders, q.lastOrder)

	q.rawSQL = nil

	return q
}

func (q *Query) Asc() *Query {
	if q.err != nil {
		return q
	}

	return q.Dir(true)
}

func (q *Query) Desc() *Query {
	if q.err != nil {
		return q
	}

	return q.Dir(false)
}

// Sets the order direction for the last order by command. true=asc, false=desc
func (q *Query) Dir(asc bool) *Query {
	if q.err != nil {
		return q
	}

	if q.lastOrder != nil {
		q.lastOrder.Asc(asc)

		q.rawSQL = nil
	}
	return q
}

func (q *Query) GetOrders() []*Order {
	return q.orders
}

// JOINS ===

// includes the associations as inner joins to the current path
//
//   param: associations
//   return this query
func (q *Query) Inner(associations ...*Association) *Query {
	if q.err != nil {
		return q
	}

	q.DmlBase.inner(true, associations...)
	q.lastToken = nil

	return q
}

// includes the associations as outer joins to the current path
//
//   param associations
//   return
func (q *Query) Outer(associations ...*Association) *Query {
	if q.err != nil {
		return q
	}

	q.DmlBase.inner(false, associations...)
	q.lastToken = nil

	return q
}

//This will trigger a result that can be dumped in a tree object
//using current association path to build the tree result.
//
//It will includes all the columns of all the tables referred by the association path,
// except where columns were explicitly included.
func (q *Query) Fetch() *Query {
	if q.err != nil {
		return q
	}

	if q.path != nil {
		for _, pe := range q.path {
			q.includeInPath(pe) // includes all columns
		}
	}

	q.join(true)

	return q
}

// indicates that the path should be used to join only
func (q *Query) Join() *Query {
	if q.err != nil {
		return q
	}

	q.join(false)
	return q
}

func (q *Query) join(fetch bool) {
	if q.path != nil {
		tokens := make([]Tokener, 0)
		for _, pe := range q.path {
			funs := pe.Columns
			for _, fun := range funs {
				tokens = append(tokens, fun)
				if !fetch {
					fun.SetPseudoTableAlias(q.tableAlias)
				}
			}
		}

		q.Columns = append(q.Columns, tokens...)
	}

	// only after this the joins will have the proper join table alias
	q.DmlBase.joinTo(q.path, fetch)

	// process pending orders
	if q.path != nil {
		for _, pe := range q.path {
			if pe.Orders != nil {
				for _, o := range pe.Orders {
					o.column.SetTableAlias(pathElementAlias(pe))
					q.orders = append(q.orders, o)
				}
			}
		}
	}

	q.path = nil
	q.rawSQL = nil
}

func pathElementAlias(pe *PathElement) string {
	derived := pe.Derived
	if derived.IsMany2Many() {
		return derived.ToM2M.GetAliasTo()
	} else {
		return derived.GetAliasTo()
	}
}

// adds tokens refering the last defined association
func (q *Query) Include(columns ...interface{}) *Query {
	if q.err != nil {
		return q
	}

	lenPath := len(q.path)
	if lenPath > 0 {
		lastPath := q.path[lenPath-1]
		q.includeInPath(lastPath, columns...)

		q.rawSQL = nil
	} else {
		return &Query{
			err: faults.New("there is no current join"),
		}
	}
	return q
}

func (q *Query) includeInPath(lastPath *PathElement, columns ...interface{}) {
	if len(columns) > 0 || len(lastPath.Columns) == 0 {
		if len(columns) == 0 {
			// use all columns of the targeted table
			columns = lastPath.Base.GetTableTo().GetColumns().Elements()
		}
		if lastPath.Columns == nil {
			lastPath.Columns = make([]Tokener, 0)
		}
		for _, c := range columns {
			q.lastToken = tokenizeOne(c)
			lastPath.Columns = append(lastPath.Columns, q.lastToken)
		}
	}
}

// Restriction to apply to the previous association
func (q *Query) On(criteria ...*Criteria) *Query {
	if q.err != nil {
		return q
	}

	if len(q.path) > 0 {
		var retriction *Criteria
		if len(criteria) > 1 {
			retriction = And(criteria...)
		} else if len(criteria) == 1 {
			retriction = criteria[0]
		} else {
			return &Query{
				err: faults.New("nil or empty criterias was passed"),
			}
		}
		q.path[len(q.path)-1].Criteria = retriction

		q.rawSQL = nil
	} else {
		return &Query{
			err: faults.New("there is no current join"),
		}
	}
	return q
}

// UNIONS ===
func (q *Query) Union(query *Query) *Query {
	if q.err != nil {
		return q
	}

	return q.unite(query, false)
}

func (q *Query) UnionAll(query *Query) *Query {
	if q.err != nil {
		return q
	}

	return q.unite(query, true)
}

func (q *Query) unite(query *Query, all bool) *Query {
	// copy the parameters of the subquery to the main query
	for k, v := range query.GetParameters() {
		q.SetParameter(k, v)
	}
	q.unions = append(q.unions, &Union{query, all})

	q.rawSQL = nil

	return q
}

func (q *Query) GetUnions() []*Union {
	return q.unions
}

// GROUP BY ===
func (q *Query) GroupByUntil(untilPos int) *Query {
	if q.err != nil {
		return q
	}

	q.groupBy = make([]int, untilPos)
	for i := 0; i < untilPos; i++ {
		q.groupBy[i] = i + 1
	}

	q.rawSQL = nil

	return q
}

func (q *Query) GroupByPos(pos ...int) *Query {
	if q.err != nil {
		return q
	}

	q.groupBy = pos

	q.rawSQL = nil

	return q
}

func (q *Query) GetGroupBy() []int {
	return q.groupBy
}

func (q *Query) GetGroupByTokens() []Group {
	var groups []Group
	length := len(q.groupBy)
	if length > 0 {
		groups = make([]Group, length)
		for k, idx := range q.groupBy {
			groups[k].Position = idx - 1
			groups[k].Token = q.Columns[idx-1]
		}
	}
	return groups
}

func (q *Query) GroupBy(cols ...*Column) *Query {
	if q.err != nil {
		return q
	}

	q.rawSQL = nil

	length := len(cols)
	if length == 0 {
		q.groupBy = nil
		return q
	}

	q.groupBy = make([]int, length)

	pos := 1
	for i := 0; i < length; i++ {
		for _, token := range q.Columns {
			if ch, ok := token.(*ColumnHolder); ok {
				if ch.GetColumn().Equals(cols[i]) {
					q.groupBy[i] = pos
					break
				}
			}
		}
		pos++

		if q.groupBy[i] == 0 {
			return &Query{
				err: faults.Errorf("column alias '%s' was not found", cols[i]),
			}
		}
	}

	return q
}

func (q *Query) GroupByAs(aliases ...string) *Query {
	if q.err != nil {
		return q
	}

	q.rawSQL = nil

	length := len(aliases)
	if length == 0 {
		q.groupBy = nil
		return q
	}

	q.groupBy = make([]int, length)

	pos := 1
	for i := 0; i < length; i++ {
		for _, token := range q.Columns {
			if aliases[i] == token.GetAlias() {
				q.groupBy[i] = pos
				break
			}
		}
		pos++

		if q.groupBy[i] == 0 {
			return &Query{
				err: faults.Errorf("column alias '%s' was not found", aliases[i]),
			}
		}
	}

	return q
}

// Adds a Having clause to the query.
// The tokens are not processed. You will have to explicitly set all table alias.
func (q *Query) Having(having ...*Criteria) *Query {
	if q.err != nil {
		return q
	}

	if len(having) > 0 {
		q.having = And(having...)
		q.replaceAlias(q.having)
	}

	return q
}

func (q *Query) GetHaving() *Criteria {
	return q.having
}

// replaces ALIAS with the respective select parcel
func (q *Query) replaceAlias(token Tokener) {
	members := token.GetMembers()
	if token.GetOperator() == TOKEN_ALIAS {
		alias := token.GetValue().(string)
		for _, v := range q.Columns {
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
		for _, t := range members {
			if t != nil {
				q.replaceAlias(t)
			}
		}
	}
}

// ======== RETRIEVE ==============

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
func (q *Query) ListSimple(closure func(), instances ...interface{}) error {
	if q.err != nil {
		return q.err
	}

	return q.listClosure(func(rows *sql.Rows) error {
		err := rows.Scan(instances...)
		if err != nil {
			return err
		}
		closure()
		return nil
	})
}

// List using the closure arguments.
//
// A function is used to build the result list.
//
// The types for scanning are supplied by the function arguments. Arguments can be pointers or not.
//
// Reflection is used to determine the arguments types.
//
// The argument can also be a function with the signature func(*struct).
//
// The results will not be assembled in as a tree.
//
//
// ex:
//   roles = make([]string, 0)
//   var role string
//   q.ListInto(func(role *string) {
//	   roles = append(roles, *role)
//   })
func (q *Query) ListInto(closure interface{}) ([]interface{}, error) {
	if q.err != nil {
		return nil, q.err
	}

	// determine types and instanciate them
	ftype := reflect.TypeOf(closure)
	if ftype.Kind() != reflect.Func {
		return nil, faults.Errorf("expected a function with the signature func(*struct) [*struct] or func(primitive1, ..., primitiveN) [anything]. Got %s.", ftype)
	}

	caller, typ, ok := checkCollector(closure)
	if ok {
		coll, err := q.list(NewEntityFactoryTransformer(q, typ, caller))
		if err != nil {
			return nil, err
		}
		return coll.Elements(), nil
	} else {
		return q.listIntoClosure(closure)
	}
}

// the transformer will be responsible for creating  the result list
func (q *Query) listIntoClosure(transformer interface{}) ([]interface{}, error) {
	// if no columns were added, add all columns of the driving table
	if len(q.Columns) == 0 {
		q.All()
	}

	rsql := q.getCachedSql()
	q.debugSQL(rsql.OriSql, 2)

	params, err := rsql.BuildValues(q.DmlBase.parameters)
	if err != nil {
		return nil, err
	}
	r, e := q.DmlBase.dba.QueryInto(rsql.Sql, transformer, params...)
	if e != nil {
		return nil, e
	}
	return r, nil
}

// the transformer will be responsible for creating  the result list
func (q *Query) listClosure(transformer func(rows *sql.Rows) error) error {
	// if no columns were added, add all columns of the driving table
	if len(q.Columns) == 0 {
		q.All()
	}

	rsql := q.getCachedSql()
	q.debugSQL(rsql.OriSql, 2)

	params, err := rsql.BuildValues(q.DmlBase.parameters)
	if err != nil {
		return err
	}
	e := q.DmlBase.dba.QueryClosure(rsql.Sql, transformer, params...)
	if e != nil {
		return e
	}
	return nil
}

// Executes a query and transform the results according to the transformer
// Accepts a row transformer and returns a collection of transformed results
func (q *Query) list(rowMapper dbx.IRowTransformer) (coll.Collection, error) {
	// if no columns were added, add all columns of the driving table
	if len(q.Columns) == 0 {
		q.All()
	}

	rsql := q.getCachedSql()
	q.debugSQL(rsql.OriSql, 2)

	params, err := rsql.BuildValues(q.DmlBase.parameters)
	if err != nil {
		return nil, err
	}
	list, e := q.DmlBase.dba.QueryCollection(rsql.Sql, rowMapper, params...)
	if e != nil {
		return nil, e
	}
	return list, nil
}

//Executes a query and transform the results to the struct type passed as parameter,
//matching the alias with struct property name. If no alias is supplied, it is used the default column alias.
//
//Accepts as parameter the struct type and returns a collection of structs (needs cast)
func (q *Query) ListOf(template interface{}) (coll.Collection, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.list(NewEntityTransformer(q, template))
}

// Executes a query and transform the results into a tree with the passed struct type as the head.
// It matches the alias with struct property name, building a struct tree.
// If the transformed data matches a previous converted entity the previous one is reused.
//
// Receives a template instance and returns a collection of structs.
func (q *Query) ListTreeOf(template tk.Hasher) (coll.Collection, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.list(NewEntityTreeTransformer(q, true, template))
}

//Executes a query, putting the result in a slice, passed as an argument.
// The slice element slice can be a struct or a primitive (ex: string).
//
//This method does not create a tree of related instances.
func (q *Query) List(target interface{}) error {
	if q.err != nil {
		return q.err
	}

	caller, typ, isStruct, ok := checkSlice(target)
	if !ok {
		return faults.Errorf("goSQL: Expected a slice of type *[]*struct. Got %s", typ.String())
	}

	if isStruct {
		_, err := q.list(NewEntityFactoryTransformer(q, typ, caller))
		return err
	} else {
		holder := reflect.New(typ).Interface()
		return q.listClosure(func(rows *sql.Rows) error {
			if err := rows.Scan(holder); err != nil {
				return err
			}
			caller(reflect.ValueOf(holder).Elem())
			return nil
		})
	}
}

func checkSlice(i interface{}) (func(val reflect.Value) reflect.Value, reflect.Type, bool, bool) {
	arr := reflect.ValueOf(i)
	// pointer to the slice
	if arr.Kind() == reflect.Ptr {
		arr = arr.Elem()
	} else {
		return nil, nil, false, false
	}

	// slice element
	var typ reflect.Type
	if arr.Kind() == reflect.Slice {
		typ = arr.Type().Elem()
	} else {
		return nil, nil, false, false
	}

	isStruct := typ.Kind() == reflect.Struct || typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Struct
	ptrElem := (typ.Kind() == reflect.Ptr)
	if !ptrElem {
		typ = reflect.PtrTo(typ) // get the pointer
	}

	// A non initialized array is the same as nil,
	// so a slice is created so that it can be different from nil.
	slice := reflect.MakeSlice(arr.Type(), 0, 20)
	arr.Set(slice)
	// slice := reflect.New(arr.Type()).Elem()
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

	return slicer, typ, isStruct, true
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
func (q *Query) ListFlatTreeOf(template interface{}) (coll.Collection, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.list(NewEntityTreeTransformer(q, false, template))
}

// Executes a query, putting the result in a slice, passed as an argument or
// delegating the responsability of building the result to a processor function.
// The argument must be a function with the signature func(<<*>struct>) or a slice like *[]<*>struct.
// See also List.
func (q *Query) ListFlatTree(target interface{}) error {
	if q.err != nil {
		return q.err
	}

	caller, typ, isStruct, ok := checkSlice(target)
	if !ok || !isStruct {
		caller, typ, ok = checkCollector(target)
		if !ok {
			return faults.Errorf("goSQL: Expected a slice of type *[]<*>struct or a function with the signature func(<<*>struct>). got %s", typ.String())
		}
	}

	_, err := q.list(NewEntityTreeFactoryTransformer(q, typ, caller))
	return err
}

// the result of the query is put in the passed interface array.
// returns true if a result was found, false if no result
func (q *Query) SelectInto(dest ...interface{}) (bool, error) {
	if q.err != nil {
		return false, q.err
	}

	// if no columns were added, add all columns of the driving table
	if len(q.Columns) == 0 {
		q.All()
	}

	rsql := q.getCachedSql()
	q.debugSQL(rsql.OriSql, 1)

	params, err := rsql.BuildValues(q.DmlBase.parameters)
	if err != nil {
		return false, err
	}
	found, e := q.dba.QueryRow(rsql.Sql, params, dest...)
	if e != nil {
		return false, e
	}
	return found, nil
}

//Returns a struct tree. When reuse is true the supplied template instance must implement
//the toolkit.Hasher interface.
//
//This is pretty much the same as SelectTreeTo.
func (q *Query) selectTree(typ interface{}, reuse bool) (interface{}, error) {
	if q.err != nil {
		return nil, q.err
	}

	if reuse {
		_, ok := typ.(tk.Hasher)
		if !ok {
			return nil, faults.Errorf("When reuse is true, the type %T must implement toolkit.Hasher", typ)
		}

		list, err := q.list(NewEntityTreeTransformer(q, true, typ))
		if err != nil {
			return nil, err
		}

		if list.Size() == 0 {
			return nil, nil
		} else {
			return list.Enumerator().Next(), nil // first one
		}
	}

	return q.selectTransformer(NewEntityTreeTransformer(q, false, typ))
}

// The first result of the query is put in the passed struct.
// Returns true if a result was found, false if no result
func (q *Query) SelectTo(instance interface{}) (bool, error) {
	if q.err != nil {
		return false, q.err
	}

	res, err := q.selectTransformer(NewEntityTransformer(q, instance))
	if err != nil {
		return false, err
	}
	if res != nil {
		tk.Set(instance, res)
		return true, nil
	}
	// remove previous marks
	if t, ok := instance.(Markable); ok {
		t.Unmark()
	}
	return false, nil
}

// Executes the query and builds a struct tree, reusing previously obtained entities,
// putting the first element in the supplied struct pointer.
// Since the struct instances are going to be reused it is mandatory that all the structs
// participating in the result tree implement the toolkit.Hasher interface.
// Returns true if a result was found, false if no result.
// See also SelectFlatTree.
func (q *Query) SelectTree(instance tk.Hasher) (bool, error) {
	if q.err != nil {
		return false, q.err
	}

	return q.selectTreeTo(instance, true)
}

// Executes the query and builds a flat struct tree putting the first element in the supplied struct pointer.
// Since the struct instances are not going to be reused it is not mandatory that the structs implement the toolkit.Hasher interface.
// Returns true if a result was found, false if no result.
// See also SelectTree.
func (q *Query) SelectFlatTree(instance interface{}) (bool, error) {
	if q.err != nil {
		return false, q.err
	}

	return q.selectTreeTo(instance, false)
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
func (q *Query) selectTreeTo(instance interface{}, reuse bool) (bool, error) {
	res, err := q.selectTree(instance, reuse)
	if err != nil {
		return false, err
	}
	if res != nil {
		tk.Set(instance, res)
		return true, nil
	}
	// remove previous marks
	if t, ok := instance.(Markable); ok {
		t.Unmark()
	}
	return false, nil
}

func (q *Query) selectTransformer(rowMapper dbx.IRowTransformer) (interface{}, error) {
	oldMax := q.limit
	q.Limit(1)
	defer q.Limit(oldMax)

	list, err := q.list(rowMapper)
	if err != nil {
		return nil, err
	}

	if list.Size() == 0 {
		return nil, nil
	}
	return list.Enumerator().Next(), nil // first one
}

// SQL String. It is cached for multiple access
func (q *Query) getCachedSql() *RawSql {
	if q.rawSQL == nil {
		// if the discriminator conditions have not yet been processed, apply them now
		if q.discriminatorCriterias != nil && q.criteria == nil {
			q.DmlBase.where(nil)
		}

		sql := q.db.GetTranslator().GetSqlForQuery(q)
		q.rawSQL = ToRawSql(sql, q.db.GetTranslator())
	}

	return q.rawSQL
}
