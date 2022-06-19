package db

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/quintans/faults"
	"github.com/quintans/goSQL/dbx"
	tk "github.com/quintans/toolkit"
	"github.com/quintans/toolkit/log"
)

var lgr = log.LoggerFor("github.com/quintans/goSQL/db")

// instead of setting globaly the logger with the caller at 1,
// it is defined locally with method CallerAt()
//func init() {
//	lgr.SetCallerAt(1)
//}

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
func (r *RawSql) BuildValues(paramMap map[string]interface{}) ([]interface{}, error) {
	paramArray := make([]interface{}, len(r.Names))
	var ok bool
	for i, name := range r.Names {
		paramArray[i], ok = paramMap[name]
		if !ok {
			return nil, faults.Errorf("[%s] No value supplied for the SQL parameter '%s' for the SQL %s",
				dbx.FAULT_VALUES_STATEMENT, name, r.OriSql)
		}
	}
	return paramArray, nil
}

func (r *RawSql) Clone() interface{} {
	other := new(RawSql)
	other.OriSql = r.OriSql
	other.Sql = r.Sql
	if r.Names != nil {
		other.Names = make([]string, len(r.Names))
		copy(other.Names, r.Names)
	}
	return other
}

type PathCriteria struct {
	Criterias []*Criteria
	Columns   []Tokener
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
	d := new(DmlBase)
	d.init(DB, table)
	return d
}

func (d *DmlBase) init(DB IDb, table *Table) {
	d.db = DB
	d.table = table
	d.alias(PREFIX + "0")

	if table != nil {
		d.discriminatorCriterias = table.GetCriterias()
	}
	d.parameters = make(map[string]interface{})

	d.dba = dbx.NewSimpleDBA(DB.GetConnection())
}

func (d *DmlBase) NextRawIndex() int {
	d.rawIndex++
	return d.rawIndex
}

func (d *DmlBase) GetDb() IDb {
	return d.db
}

func (d *DmlBase) GetDba() *dbx.SimpleDBA {
	return d.dba
}

func (d *DmlBase) GetTable() *Table {
	return d.table
}

func (d *DmlBase) GetTableAlias() string {
	return d.tableAlias
}

func (d *DmlBase) SetTableAlias(alias string) {
	d.alias(alias)
}

func (d *DmlBase) alias(a string) {
	if a != "" {
		d.joinBag = NewAliasBag(a + "_" + JOIN_PREFIX)
		d.tableAlias = a
		d.rawSQL = nil
	}
}

func (d *DmlBase) GetJoins() []*Join {
	return d.joins
}

func (d *DmlBase) SetParameter(key string, parameter interface{}) {
	d.parameters[key] = parameter
}

func (d *DmlBase) GetParameters() map[string]interface{} {
	return d.parameters
}

func (d *DmlBase) GetParameter(column *Column) interface{} {
	return d.parameters[column.GetAlias()]
}

func (d *DmlBase) GetCriteria() *Criteria {
	return d.criteria
}

// Sets the value of parameter to the column
// param col: The column
// param parameter: The value to set
func (d *DmlBase) SetParameterFor(col *Column, parameter interface{}) {
	d.SetParameter(col.GetAlias(), parameter)
}

func (d *DmlBase) GetAliasForAssociation(association *Association) string {
	if d.joinBag != nil {
		return d.joinBag.GetAlias(association)
	}
	return ""
}

// includes the associations as inner joins to the current path
// param associations
func (d *DmlBase) inner(inner bool, associations ...*Association) {
	for _, association := range associations {
		pe := new(PathElement)
		pe.Base = association
		pe.Inner = inner
		d.path = append(d.path, pe)
	}

	d.rawSQL = nil
}

/*
Indicates that the current association chain should be used to join only.
A table end alias can also be supplied.
This
*/
func (d *DmlBase) joinTo(path []*PathElement, fetch bool) {
	if len(path) > 0 {
		d.addJoin(path, nil, fetch)

		// the first position refers to constraints applied to the table, due to a association discriminator
		pathCriterias := d.buildPathCriterias(path)
		var firstCriterias []*Criteria
		// process the acumulated criterias
		for index, pathCriteria := range pathCriterias {
			if pathCriteria != nil {
				conds := pathCriteria.Criterias
				// adjustTableAlias()
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
						d.applyOn(path[:index], And(conds...))
					}
				}

				if pathCriteria.Columns != nil {
					d.applyInclude(path[:index], pathCriteria.Columns...)
				}
			}
		}
	}
}

/*
The path criteria on position 0 refers the criteria on the FROM table.
The Association can have a constraint(discriminator) refering a column in the source table.
Both ends of Discriminator criterias (association origin and destination tables) are treated in this block
*/
func (d *DmlBase) buildPathCriterias(paths []*PathElement) []*PathCriteria {
	// see if any targeted table has discriminator columns
	index := 0
	var tableCriterias []*Criteria
	length := len(paths) + 1
	pathCriterias := make([]*PathCriteria, length)

	// processes normal criterias
	for _, pe := range paths {
		index++

		var pc *PathCriteria
		if pe.Criteria != nil {
			pc = new(PathCriteria)
			pc.Criterias = append(pc.Criterias, pe.Criteria)
			pathCriterias[index] = pc
		}

		// table discriminator on target
		tableCriterias = pe.Base.GetTableTo().GetCriterias()
		if tableCriterias != nil {
			if pc == nil {
				pc = new(PathCriteria)
				pathCriterias[index] = pc
			}
			pc.Criterias = append(pc.Criterias, tableCriterias...)
		}

		// references column Includes
		if pe.Columns != nil {
			if pc == nil {
				pc = new(PathCriteria)
				pathCriterias[index] = pc
			}
			pc.Columns = pe.Columns
		}
	}

	// process criterias from the association discriminators
	var lastFkAlias = d.GetTableAlias()
	index = 0
	for _, pe := range paths {
		index++
		discriminators := pe.Base.GetDiscriminators()
		if discriminators != nil {
			pc := pathCriterias[index]
			if pc == nil {
				pc = new(PathCriteria)
				pathCriterias[index] = pc
			}

			if pe.Base.GetDiscriminatorTable().Equals(pe.Base.GetTableTo()) {
				for _, v := range discriminators {
					pc.Criterias = append(pc.Criterias, v.Criteria())
				}
			} else {
				// force table alias for the first criteria
				for _, v := range discriminators {
					crit := v.Criteria()
					crit.SetTableAlias(lastFkAlias)
					pc.Criterias = append(pc.Criterias, crit)
				}
			}
		}
		lastFkAlias = d.joinBag.GetAlias(pe.Derived)
	}

	return pathCriterias
}

func (d *DmlBase) addJoin(associations []*PathElement, common []*PathElement, fetch bool) []*PathElement {
	var local []*PathElement

	if common == nil {
		common = DeepestCommonPath(d.cachedAssociation, associations)
	}

	deriveds := make([]*Association, len(associations))
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
			deriveds[f], _ = association.Clone().(*Association)

			/*
				Processes the associations.
				The alias of the initial side (from) of the first associations
				is assigned the value 'firstAlias' (main table value)
				The alias of the final side of the last association is assigned the
				value 'pe.Alias', if it is not empty
			*/
			var fkAlias string
			if f == 0 {
				fkAlias = d.tableAlias
			} else {
				fkAlias = d.joinBag.GetAlias(lastFk)
			}

			if deriveds[f].IsMany2Many() {
				fromFk := deriveds[f].FromM2M
				toFk := deriveds[f].ToM2M

				d.prepareAssociation(
					fkAlias,
					d.joinBag.GetAlias(fromFk),
					fromFk)

				if pe.PreferredAlias == "" {
					fkAlias = d.joinBag.GetAlias(toFk)
				} else {
					fkAlias = pe.PreferredAlias
					d.joinBag.SetAlias(toFk, pe.PreferredAlias)
				}
				d.prepareAssociation(
					d.joinBag.GetAlias(fromFk),
					fkAlias,
					toFk)
				lastFk = toFk
			} else {
				var fkAlias2 string
				if pe.PreferredAlias == "" {
					fkAlias2 = d.joinBag.GetAlias(deriveds[f])
				} else {
					fkAlias2 = pe.PreferredAlias
					d.joinBag.SetAlias(deriveds[f], pe.PreferredAlias)
				}
				d.prepareAssociation(
					fkAlias,
					fkAlias2,
					deriveds[f])
				lastFk = deriveds[f]
			}

		} else {
			// the main list allways with association many-to-many
			deriveds[f] = lastCachedFk
			// defines the previous fk
			if deriveds[f].IsMany2Many() {
				lastFk = deriveds[f].ToM2M
			} else {
				lastFk = lastCachedFk
			}
		}
		pe.Derived = deriveds[f]
		local = append(local, pe) // cache it

		f++
	}

	// only caches if the path was different
	if !matches {
		d.cachedAssociation = append(d.cachedAssociation, local)
	}

	// gets the alias of the last join
	d.lastFkAlias = d.joinBag.GetAlias(lastFk)

	d.lastJoin = NewJoin(local, fetch)
	d.joins = append(d.joins, d.lastJoin)

	return local
}

func (d *DmlBase) prepareAssociation(aliasFrom string, aliasTo string, currentFk *Association) {
	currentFk.SetAliasFrom(aliasFrom)
	currentFk.SetAliasTo(aliasTo)
	for _, rel := range currentFk.GetRelations() {
		rel.From.SetTableAlias(aliasFrom)
		rel.To.SetTableAlias(aliasTo)
	}
}

func (d *DmlBase) where(restrictions []*Criteria) {
	var criterias []*Criteria
	if len(restrictions) > 0 {
		criterias = append(criterias, restrictions...)
	}

	if len(d.discriminatorCriterias) > 0 {
		criterias = append(criterias, d.discriminatorCriterias...)
	}

	if len(criterias) > 0 {
		d.applyWhere(And(criterias...))
	}
}

func (d *DmlBase) applyOn(chain []*PathElement, criteria *Criteria) {
	if len(chain) > 0 {
		pe := chain[len(chain)-1]
		cpy, _ := criteria.Clone().(*Criteria)

		fk := pe.Derived
		var fkAlias string
		if fk.IsMany2Many() {
			fkAlias = d.joinBag.GetAlias(fk.ToM2M)
		} else {
			fkAlias = d.joinBag.GetAlias(pe.Derived)
		}
		cpy.SetTableAlias(fkAlias)

		d.replaceRaw(cpy)
		pe.Criteria = cpy

		d.rawSQL = nil
	}
}

func (d *DmlBase) applyInclude(chain []*PathElement, tokens ...Tokener) {
	if len(chain) > 0 {
		pe := chain[len(chain)-1]
		fk := pe.Derived
		var fkAlias string
		if fk.IsMany2Many() {
			fkAlias = d.joinBag.GetAlias(fk.ToM2M)
		} else {
			fkAlias = d.joinBag.GetAlias(pe.Derived)
		}
		for _, token := range tokens {
			token.SetTableAlias(fkAlias)
		}

		d.rawSQL = nil
	}
}

// WHERE ===
func (d *DmlBase) applyWhere(restriction *Criteria) {
	token, _ := restriction.Clone().(*Criteria)
	d.replaceRaw(token)
	token.SetTableAlias(d.tableAlias)

	d.criteria = token

	d.rawSQL = nil
}

func (d *DmlBase) dumpParameters(params map[string]interface{}) string {
	str := tk.NewStrBuffer()
	for name, v := range params {
		if strings.HasSuffix(name, "$") {
			// secret
			str.Add(fmt.Sprintf("[%s=****]", name))
		} else if v != nil {
			typ := reflect.ValueOf(v)
			k := typ.Kind()
			if k == reflect.Slice || k == reflect.Array {
				str.Add(fmt.Sprintf("[%s=<BLOB>]", name))
			} else if k == reflect.Ptr {
				if typ.IsNil() {
					str.Add(fmt.Sprintf("[%s=NULL]", name))
				} else {
					str.Add(fmt.Sprintf("[%s=(*)%v]", name, typ.Elem().Interface()))
				}
			} else {
				str.Add(fmt.Sprintf("[%s=%v]", name, typ.Interface()))
			}
		} else {
			str.Add(fmt.Sprintf("[%s=NULL]", name))
		}
	}

	return str.String()
}

func (d *DmlBase) debugSQL(sql string, depth int) {
	if lgr.IsActive(log.DEBUG) {
		dump := d.dumpParameters(d.parameters)
		lgr.CallerAt(depth+1).Debugf("%s", func() string {
			return fmt.Sprintf("\n\t%T SQL: %s\n\tparameters: %s",
				d, sql, dump)
		})
	}
}

// replaces RAW with PARAM
//
// param baseDml: the instance DmlBase were to put the created parameters
// param token
// return
func (d *DmlBase) replaceRaw(token Tokener) {
	/*
		if tk.IsNil(token) {
			return
		}
	*/

	members := token.GetMembers()
	if token.GetOperator() == TOKEN_RAW {
		d.rawIndex++
		parameter := d.tableAlias + "_R" + strconv.Itoa(d.rawIndex)
		d.SetParameter(parameter, token.GetValue())
		token.SetOperator(TOKEN_PARAM)
		token.SetValue(parameter)
		return
	} else if token.GetOperator() == TOKEN_SUBQUERY {
		subquery := token.GetValue().(*Query)
		// copy the parameters of the subquery to the main query
		for k, v := range subquery.GetParameters() {
			d.SetParameter(k, v)
		}
		return
	} else {
		for _, t := range members {
			if t != nil {
				d.replaceRaw(t)
			}
		}
	}
}
