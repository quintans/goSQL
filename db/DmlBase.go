package db

import (
	"fmt"
	"github.com/quintans/goSQL/dbx"
	tk "github.com/quintans/toolkit"
	"github.com/quintans/toolkit/log"
	"reflect"
	"strconv"
	"time"
)

var lgr = log.LoggerFor("github.com/quintans/goSQL/db")

func init() {
	// activates output of program file line
	lgr.CallDepth(2)
}

type RawSql struct {
	// original sql
	OriSql string
	// the converted SQL with the Database specific placeholders
	Sql string
	// the parameters values
	Names []string
}

// Convert a Map of named parameter values to a corresponding array.
//
// return the array of values
func (this *RawSql) BuildValues(paramMap map[string]interface{}) []interface{} {
	paramArray := make([]interface{}, len(this.Names))
	var ok bool
	for i, name := range this.Names {
		paramArray[i], ok = paramMap[name]
		if !ok {
			panic(fmt.Sprintf("[%s] No value supplied for the SQL parameter '%s' for the SQL %s",
				dbx.FAULT_VALUES_STATEMENT, name, this.OriSql))
		}
	}
	return paramArray
}

func (this *RawSql) Clone() interface{} {
	other := new(RawSql)
	other.OriSql = this.OriSql
	other.Sql = this.Sql
	if this.Names != nil {
		other.Names = make([]string, len(this.Names))
		copy(other.Names, this.Names)
	}
	return other
}

type PathCriteria struct {
	Path      []*PathElement
	Criterias []*Criteria
}

const JOIN_PREFIX = "j"
const PREFIX = "t"

type DmlBase struct {
	db IDb

	table                  *Table
	tableAlias             string
	joins                  []*Join
	criteria               *Criteria
	parameters             map[string]interface{}
	joinBag                *AliasBag
	lastFkAlias            string
	lastJoin               *Join
	discriminatorCriterias []*Criteria
	rawIndex               int
	// stores the paths (associations) already traveled
	cachedAssociation [][]*PathElement
	// list with the associations of the current path
	path []*PathElement

	rawSQL *RawSql
	dba    *dbx.SimpleDBA
}

func NewDmlBase(DB IDb, table *Table) *DmlBase {
	this := new(DmlBase)
	this.Super(DB, table)
	return this
}

func (this *DmlBase) Super(DB IDb, table *Table) {
	this.db = DB
	this.table = table
	this.alias(PREFIX + "0")

	if table != nil {
		criterias := table.GetCriterias()
		if criterias != nil {
			this.discriminatorCriterias = make([]*Criteria, len(criterias))
			copy(this.discriminatorCriterias, criterias)
		}
	}
	this.parameters = make(map[string]interface{})

	this.dba = dbx.NewSimpleDBA(DB.GetConnection())
}

func (this *DmlBase) NextRawIndex() int {
	this.rawIndex++
	return this.rawIndex
}

func (this *DmlBase) GetDb() IDb {
	return this.db
}

func (this *DmlBase) GetDba() *dbx.SimpleDBA {
	return this.dba
}

func (this *DmlBase) GetTable() *Table {
	return this.table
}

func (this *DmlBase) GetTableAlias() string {
	return this.tableAlias
}

func (this *DmlBase) SetTableAlias(alias string) {
	this.alias(alias)
}

func (this *DmlBase) alias(a string) {
	if a != "" {
		this.joinBag = NewAliasBag(a + "_" + JOIN_PREFIX)
		this.tableAlias = a
		this.rawSQL = nil
	}
}

func (this *DmlBase) GetJoins() []*Join {
	return this.joins
}

func (this *DmlBase) SetParameter(key string, parameter interface{}) {
	this.parameters[key] = parameter
}

func (this *DmlBase) GetParameters() map[string]interface{} {
	return this.parameters
}

func (this *DmlBase) GetParameter(column *Column) interface{} {
	return this.parameters[column.GetAlias()]
}

func (this *DmlBase) GetCriteria() *Criteria {
	return this.criteria
}

// Sets the value of parameter to the column
// param col: The column
// param parameter: The value to set
func (this *DmlBase) SetParameterFor(col *Column, parameter interface{}) {
	this.SetParameter(col.GetAlias(), parameter)
}

func (this *DmlBase) GetAliasForAssociation(association *Association) string {
	if this.joinBag != nil {
		return this.joinBag.GetAlias(association)
	}
	return ""
}

// includes the associations as inner joins to the current path
// param associations
func (this *DmlBase) inner(associations ...*Association) {
	for _, association := range associations {
		this.path = append(this.path, &PathElement{association, nil, true})
	}

	this.rawSQL = nil
}

func (this *DmlBase) join() {
	// resets path
	this.joinAs("")
}

// indicates that the path should be used to join only
// param endAlias
//  @return
func (this *DmlBase) joinAs(endAlias string) {
	if this.path != nil {
		cache := this.buildPathCriterias(this.path)
		// process the acumulated criterias
		var firstCriterias []*Criteria
		for index, pathCriteria := range cache {
			if pathCriteria != nil {
				conds := pathCriteria.Criterias
				// adjustTableAlias()
				if conds != nil {
					// index == 0 applies to the starting table
					if index == 0 {
						// already with the alias applied
						firstCriterias = conds
					} else {
						this.addJoin("", pathCriteria.Path)
						if firstCriterias != nil {
							tmp := make([]*Criteria, len(conds))
							copy(tmp, conds)
							conds = append(tmp, firstCriterias...)
							firstCriterias = nil
						}
						this.applyOn(And(conds...))
					}
				}
			}
		}

		// if the last one was not processed
		if cache[len(cache)-1] == nil {
			this.addJoin(endAlias, this.path)
		}
		if firstCriterias != nil {
			this.applyOn(And(firstCriterias...))
		}
	}
	this.path = nil

	this.rawSQL = nil
}

func (this *DmlBase) buildPathCriterias(paths []*PathElement) []*PathCriteria {
	// see if any targeted table has discriminator columns
	index := 0
	var tableCriterias []*Criteria
	length := len(paths) + 1
	cache := make([]*PathCriteria, length, length)

	// the path criteria on position 0 refers the criteria on the FROM table
	// both ends of Discriminator criterias (association origin and destination tables) are treated in this block
	for _, pe := range paths {
		index++

		tableCriterias = pe.Base.GetTableTo().GetCriterias()
		if tableCriterias != nil {
			pc := new(PathCriteria)
			pc.Path = paths[:index]
			pc.Criterias = append(pc.Criterias, tableCriterias...)
			cache[index] = pc
		}
	}

	index = 0
	for _, pe := range paths {
		associationCriterias := pe.Base.GetCriterias()
		if associationCriterias != nil {
			if pe.Base.GetDiscriminatorTable().Equals(pe.Base.GetTableTo()) {
				pc := cache[index+1]
				if pc == nil {
					pc = new(PathCriteria)
					pc.Path = paths[:index+1]
					cache[index+1] = pc
				}
				pc.Criterias = append(pc.Criterias, associationCriterias...)
			} else {
				pc := cache[index]
				if pc == nil {
					pc = new(PathCriteria)
					cache[index] = pc
					if index > 0 {
						pc.Path = paths[:index]
					}
				}
				// force table alias for the first criteria
				if index == 0 {
					firstCriterias := make([]*Criteria, 0)
					for _, c := range associationCriterias {
						c2 := c.Clone().(*Criteria)
						c2.SetTableAlias(this.GetTableAlias())
						firstCriterias = append(firstCriterias, c2)
					}
					associationCriterias = firstCriterias
				}
				pc.Criterias = append(pc.Criterias, associationCriterias...)
			}
		}
		index++
	}

	return cache
}

// Executa um inner join com as várias associações
// param associations
// return
func (this *DmlBase) innerJoin(associations ...*Association) {
	this.inner(associations...)
	this.join()
}

func (this *DmlBase) addJoin(lastAlias string, associations []*PathElement) []*PathElement {
	var local []*PathElement

	common := DeepestCommonPath(this.cachedAssociation, associations)

	// creates a copy, since the table alias are going to be defined
	fks := make([]*Association, len(associations))
	var lastFk *Association
	matches := true
	f := 0
	for _, pe := range associations {
		association := pe.Base
		var lastCachedFk *Association
		if matches && f < len(common) {
			if common[f].Base.Equals(association) {
				lastCachedFk = common[f].Derived
			} else {
				matches = false
			}
		} else {
			matches = false
		}

		if lastCachedFk == nil {
			// copies to assign the alias to this query
			fks[f], _ = association.Clone().(*Association)

			/*
				processes the associations
				The alias of the initial side (from) of the first associations
				is assigned the value 'firstAlias' (main table value)
				The alias of the final side of the last association is assigned the
				value 'lastAlias', if it is not null
			*/
			var fkAlias string
			if f == 0 {
				fkAlias = this.tableAlias
			} else {
				fkAlias = this.joinBag.GetAlias(lastFk)
			}
			if fks[f].IsMany2Many() {
				fromFk := fks[f].FromM2M
				toFk := fks[f].ToM2M

				this.prepareAssociation(
					fkAlias,
					this.joinBag.GetAlias(fromFk),
					fromFk,
					true)

				if lastAlias == "" || f < len(fks)-1 {
					fkAlias = this.joinBag.GetAlias(toFk)
				} else {
					fkAlias = lastAlias
				}
				this.prepareAssociation(
					this.joinBag.GetAlias(fromFk),
					fkAlias,
					toFk,
					false)
				lastFk = toFk
			} else {
				var fkAlias2 string
				if fkAlias2 == "" || f < len(fks)-1 {
					fkAlias2 = this.joinBag.GetAlias(fks[f])
				} else {
					fkAlias2 = lastAlias
				}
				this.prepareAssociation(
					fkAlias,
					fkAlias2,
					fks[f],
					false)
				lastFk = fks[f]
			}

		} else {
			// the main list allways with association many-to-many
			fks[f] = lastCachedFk
			// defines the previous fk
			if fks[f].IsMany2Many() {
				lastFk = fks[f].ToM2M
			} else {
				lastFk = lastCachedFk
			}
		}
		pe.Derived = fks[f]
		local = append(local, pe) // cache it

		f++
	}

	// only caches if the path was different
	if !matches {
		this.cachedAssociation = append(this.cachedAssociation, local)
		// gets the alias of the last join
		if lastAlias != "" {
			this.lastFkAlias = lastAlias
			this.joinBag.SetAlias(lastFk, lastAlias)
		}
	}

	// gets the alias of the last join
	if lastAlias == "" {
		this.lastFkAlias = this.joinBag.GetAlias(lastFk)
	}

	this.lastJoin = NewJoin(nil, local)
	this.joins = append(this.joins, this.lastJoin)

	return local
}

func (this *DmlBase) prepareAssociation(aliasFrom string, aliasTo string, currentFk *Association, invert bool) {
	var aFrom string
	var aTo string
	if invert {
		aFrom = aliasTo
		aTo = aliasFrom
	} else {
		aFrom = aliasFrom
		aTo = aliasTo
	}
	currentFk.SetAliasFrom(aFrom)
	currentFk.SetAliasTo(aTo)
	for _, rel := range currentFk.GetRelations() {
		rel.From.SetTableAlias(aFrom)
		rel.To.SetTableAlias(aTo)
	}
}

func (this *DmlBase) where(restrictions ...*Criteria) {
	if restrictions != nil {
		var criterias []*Criteria
		if this.discriminatorCriterias != nil {
			criterias = append(criterias, this.discriminatorCriterias...)
		}

		criterias = append(criterias, restrictions...)
		if len(criterias) != 0 {
			this.applyWhere(And(criterias...))
		}
	}
}

// condição a usar na associação imediatamente anterior
// param criteria: restrição
func (this *DmlBase) on(criterias ...*Criteria) {
	if this.lastJoin != nil {
		associations := this.lastJoin.GetAssociations()
		discriminators := associations[len(associations)-1].GetTableTo().GetCriterias()
		if discriminators != nil {
			// apply on
			var constraints []*Criteria
			constraints = append(constraints, discriminators...)
			criterias = append(constraints, criterias...)
		}

		this.applyOn(And(criterias...))
	}
}

//  condição a usar na associação imediatamente anterior
//	 param criteria: restrição
func (this *DmlBase) applyOn(criteria *Criteria) {
	if this.lastJoin != nil {
		cpy, _ := criteria.Clone().(*Criteria)

		this.joinVirtualColumns(cpy, this.lastJoin.GetPathElements())
		cpy.SetTableAlias(this.lastFkAlias)

		this.replaceRaw(cpy)
		this.lastJoin.Criteria = cpy

		this.rawSQL = nil
	}
}

func (this *DmlBase) joinVirtualColumns(token Tokener, previous []*PathElement) {
	members := token.GetMembers()
	if ch, ok := token.(*ColumnHolder); ok {
		if ch.GetColumn().IsVirtual() {
			// sets the VIRTUAL table alias (it's the alias of the current table)
			if len(previous) > 0 {
				ch.SetVirtualTableAlias(this.lastFkAlias)
			} else {
				ch.SetVirtualTableAlias(this.tableAlias)
			}
			// temp
			join := this.lastJoin
			fkAlias := this.lastFkAlias

			discriminator := ch.GetColumn().GetVirtual().Association
			var associations []*PathElement
			associations = append(associations, previous...)
			associations = append(associations, &PathElement{discriminator, nil, false})

			this.addJoin("", associations)

			ch.SetTableAlias(this.lastFkAlias)

			// reset
			this.lastFkAlias = fkAlias
			this.lastJoin = join
		}
	} else {
		if members != nil {
			for _, o := range members {
				if t, ok := o.(*Token); ok {
					this.joinVirtualColumns(t, previous)
				}
			}
		}
	}
}

// WHERE ===
func (this *DmlBase) applyWhere(restriction *Criteria) {
	// hunt for the virtual column
	this.joinVirtualColumns(restriction, nil)

	token, _ := restriction.Clone().(*Criteria)
	this.replaceRaw(token)
	token.SetTableAlias(this.tableAlias)

	this.criteria = token

	this.rawSQL = nil
}

func (this *DmlBase) dumpParameters(params map[string]interface{}) string {
	str := tk.NewStrBuffer()
	for name, v := range params {
		if v != nil {
			typ := reflect.ValueOf(v)
			k := typ.Kind()
			if k == reflect.Slice || k == reflect.Array {
				str.Add(fmt.Sprintf("[%s=<BLOB>]", name))
			} else if k == reflect.Ptr {
				str.Add(fmt.Sprintf("[%s=(*)%v]", name, typ.Elem().Interface()))
			} else {
				str.Add(fmt.Sprintf("[%s=%v]", name, typ.Interface()))
			}
		} else {
			str.Add(fmt.Sprintf("[%s=NULL]", name))
		}
	}

	return str.String()
}

func (this *DmlBase) debugTime(when time.Time) {
	elapsed := time.Since(when)
	if lgr.IsActive(log.DEBUG) {
		lgr.DebugF(func() string {
			return fmt.Sprintf("executed in: %f secs", elapsed.Seconds())
		})
	}
}

func (this *DmlBase) debugSQL(sql string) {
	if lgr.IsActive(log.DEBUG) {
		dump := this.dumpParameters(this.parameters)
		lgr.DebugF(func() string {
			return fmt.Sprintf("\n\t%T SQL: %s\n\tparameters: %s",
				this, sql, dump)
		})
	}
}

// replaces RAW with PARAM
//
// param baseDml: the instance DmlBase were to put the created parameters
// param token
// return
func (this *DmlBase) replaceRaw(token Tokener) {
	/*
		if tk.IsNil(token) {
			return
		}
	*/

	members := token.GetMembers()
	if token.GetOperator() == TOKEN_RAW {
		this.rawIndex++
		parameter := this.tableAlias + "_R" + strconv.Itoa(this.rawIndex)
		this.SetParameter(parameter, token.GetValue())
		token.SetOperator(TOKEN_PARAM)
		token.SetValue(parameter)
		return
	} else if token.GetOperator() == TOKEN_SUBQUERY {
		subquery := token.GetValue().(*Query)
		// copy the parameters of the subquery to the main query
		for k, v := range subquery.GetParameters() {
			this.SetParameter(k, v)
		}
		return
	} else {
		if members != nil {
			for _, t := range members {
				if t != nil {
					this.replaceRaw(t)
				}
			}
		}
	}
}
