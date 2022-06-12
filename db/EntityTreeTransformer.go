package db

import (
	"github.com/quintans/faults"
	tk "github.com/quintans/toolkit"
	coll "github.com/quintans/toolkit/collections"
	"github.com/quintans/toolkit/ext"

	"database/sql"
	"reflect"
)

//extends EntityTransformer
type EntityTreeTransformer struct {
	EntityTransformer

	reuse                bool
	crawler              *Crawler
	cachedEntityMappings map[string]map[string]*EntityProperty
	// entity -> entity
	entities coll.Map
}

func NewEntityTreeTransformer(query *Query, reuse bool, instance interface{}) *EntityTreeTransformer {
	this := NewEntityTreeFactoryTransformer(query, reflect.TypeOf(instance), nil)
	this.reuse = reuse
	if reuse {
		this.entities = coll.NewHashMap()
	}

	return this
}

// since the creation of the list is managed outside the reue flag is set to false
func NewEntityTreeFactoryTransformer(query *Query, typ reflect.Type, returner func(val reflect.Value) reflect.Value) *EntityTreeTransformer {
	this := new(EntityTreeTransformer)
	this.Overrider = this

	this.Super(query, createFactory(typ), returner)
	this.cachedEntityMappings = make(map[string]map[string]*EntityProperty)

	return this
}

func (e *EntityTreeTransformer) BeforeAll() coll.Collection {
	e.crawler = new(Crawler)
	e.crawler.Prepare(e.Query)

	if e.reuse {
		return coll.NewLinkedHashSet()
	}
	return coll.NewArrayList()
}

func (e *EntityTreeTransformer) OnTransformation(result coll.Collection, instance interface{}) {
	e.crawler.Rewind()
	if instance != nil && (!e.reuse || !result.Contains(instance)) {
		result.Add(instance)
	}
}

func (e *EntityTreeTransformer) AfterAll(result coll.Collection) {
	e.crawler.Dispose()
	e.crawler = nil
}

func (e *EntityTreeTransformer) Transform(rows *sql.Rows) (interface{}, error) {
	val := e.Factory()

	alias := e.Query.GetTableAlias()
	if e.TemplateData == nil {
		// creates the array with all the types returned by the query
		// using the entity properties as reference for instantiating the types
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		e.TemplateData = make([]interface{}, len(cols))
		// set default for unused columns, in case of a projection of a result
		// with more columns than the attributes of the destination struct
		for i := 0; i < len(e.TemplateData); i++ {
			e.TemplateData[i] = &ext.Any{}
		}

		// instanciate all target types
		e.InitFullRowData(e.TemplateData, val.Type(), alias)
		e.crawler.Rewind()
	}
	// makes a copy
	rowData := make([]interface{}, len(e.TemplateData), cap(e.TemplateData))
	copy(rowData, e.TemplateData)

	// Scan result set
	if err := rows.Scan(rowData...); err != nil {
		return nil, err
	}

	instance, err := e.transformEntity(rowData, val, alias)
	if err != nil {
		return nil, err
	}

	if e.Returner == nil {
		if H, isH := instance.(tk.Hasher); isH {
			return H, nil
		}
	} else {
		e.Returner(val)
	}

	return nil, nil
}

func (e *EntityTreeTransformer) InitFullRowData(
	row []interface{},
	typ reflect.Type,
	alias string,
) error {
	lastProps, err := e.getCachedProperties(alias, typ)
	if err != nil {
		return err
	}
	// instanciate all target types for the driving entity
	e.Overrider.InitRowData(row, lastProps)

	fks := e.ForwardBranches()
	if fks != nil {
		var subType reflect.Type
		for _, fk := range fks {
			bp := lastProps[alias+"."+fk.Alias]
			if bp != nil {
				if bp.IsMany() { // Collection
					subType = bp.InnerType
				} else {
					subType = bp.Type
				}

				var fkAlias string
				if fk.IsMany2Many() {
					fkAlias = fk.ToM2M.GetAliasTo()
				} else {
					fkAlias = fk.GetAliasTo()
				}

				e.InitFullRowData(row, subType, fkAlias)
			}
		}
	}
	return nil
}

func (e *EntityTreeTransformer) transformEntity(
	row []interface{},
	parent reflect.Value,
	alias string,
) (interface{}, error) {
	var valid bool
	lastProps, err := e.getCachedProperties(alias, parent.Type())
	if err != nil {
		return nil, err
	}
	entity := parent.Interface()
	hasher, isHasher := entity.(tk.Hasher)
	emptyBean := true
	if isHasher && e.reuse {
		// for performance, loads only key, because it's sufficient for searching the cache
		valid, err = e.LoadInstanceKeys(row, parent, lastProps, true)
		if err != nil {
			return nil, err
		} else if valid {
			// searches the cache
			b, _ := e.entities.Get(hasher)
			// if found, use it
			if b != nil {
				hasher = b.(tk.Hasher)
				parent = reflect.ValueOf(hasher)
			} else {
				valid, err = e.LoadInstanceKeys(row, parent, lastProps, false)
				if err != nil {
					return nil, err
				} else if valid {
					e.entities.Put(hasher, hasher)
				} else {
					hasher = nil
				}
			}
			entity = hasher
		} else {
			/*
			 * When reusing entities, the transformation needs all key columns defined.
			 * A exception is thrown if there is NO key column.
			 */
			// TODO: This is weak. there should be a way to check if ALL the key columns where used
			noKey := true
			for _, bp := range lastProps {
				if bp.Key {
					noKey = false
					break
				}
			}
			if noKey {
				return nil, faults.Errorf(
					"Key columns not found for %s."+
						" When transforming to a object tree and reusing previous entities, "+
						"the key columns must be declared in the select.",
					parent.Type(),
				)
			}
		}
		emptyBean = false
	} else {
		valid, err = e.Overrider.ToEntity(row, parent, lastProps, &emptyBean)
		if err != nil {
			return nil, err
		}
	}

	if !valid {
		e.ignoreRemaningBranch()
		return nil, nil
	}

	emptyAssoc := true
	fks := e.ForwardBranches()
	if fks != nil {
		var subType reflect.Type
		for _, fk := range fks {
			bp := lastProps[alias+"."+fk.Alias]
			if bp != nil {
				if bp.IsMany() { // Collection
					subType = bp.InnerType
				} else {
					subType = bp.Type
				}
				var fkAlias string
				if fk.IsMany2Many() {
					fkAlias = fk.ToM2M.GetAliasTo()
				} else {
					fkAlias = fk.GetAliasTo()
				}

				var childVal reflect.Value
				if subType.Kind() == reflect.Ptr {
					childVal = reflect.New(subType.Elem())
				} else {
					childVal = reflect.Zero(subType)
				}
				child, err := e.transformEntity(row, childVal, fkAlias)
				if err != nil {
					return nil, err
				} else if child != nil {
					childVal = reflect.ValueOf(child)
					if bp.IsMany() { // Collection
						sliceV := bp.Get(parent)
						// ensures initialization
						if sliceV.IsNil() {
							sliceV = reflect.MakeSlice(bp.Type, 0, 10)
						}

						if !e.reuse || !tk.SliceContains(sliceV.Interface(), child) {
							sliceV = reflect.Append(sliceV, childVal)
						}

						bp.Set(parent, sliceV)
					} else {
						// passing pointer
						bp.Set(parent, childVal)
					}
					emptyAssoc = false
				}
			}
		}
	}

	/*
	 * if the bean and all of its associations are null then we can safely ignore this bean.
	 * No include() columns were found.
	 */
	if emptyBean && emptyAssoc {
		return nil, nil
	} else {
		return entity, nil
	}
}

func (e *EntityTreeTransformer) DiscardIfKeyIsNull() bool {
	return true
}

/*
func (this *EntityTreeTransformer) LoadEntityKeys(
	row []interface{},
	typ reflect.Type,
	properties map[string]*EntityProperty,
	onlyKeys bool,
) (reflect.Value, error) {
	ptr := reflect.New(typ)
	return this.LoadInstanceKeys(row, ptr, properties, onlyKeys)
}
*/

// return true if the entity is valid
func (e *EntityTreeTransformer) LoadInstanceKeys(
	row []interface{},
	ptr reflect.Value,
	properties map[string]*EntityProperty,
	onlyKeys bool,
) (bool, error) {
	invalid := true
	instance := ptr.Elem()
	for _, bp := range properties {
		if bp.Position != 0 && bp.Key == onlyKeys {
			invalid = false
			position := bp.Position
			value, err := bp.ConvertFromDb(row[position-1])
			if err != nil {
				return false, err
			}

			v := reflect.ValueOf(value)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			ok := bp.Set(instance, v)
			if !ok && onlyKeys && bp.Key {
				// if any key is nil, the entity is invalid. ex: a entity coming from a outer join
				return false, nil
			}
		}
	}

	return !invalid, nil
}

func (e *EntityTreeTransformer) getCachedProperties(alias string, typ reflect.Type) (map[string]*EntityProperty, error) {
	properties, ok := e.cachedEntityMappings[alias]
	if !ok {
		var err error
		properties, err = e.Overrider.PopulateMapping(alias, typ)
		if err != nil {
			return nil, err
		}
		e.cachedEntityMappings[alias] = properties
	}

	return properties, nil
}

// return the list o current branches and moves forward to the next list
// return: the current list of branches
func (e *EntityTreeTransformer) ForwardBranches() []*Association {
	assocs := e.crawler.GetBranches()
	var list []*Association
	for _, assoc := range assocs {
		list = append(list, assoc.ForeignKey)
	}
	e.crawler.Forward() // move to next branches
	return list
}

func (e *EntityTreeTransformer) ignoreRemaningBranch() {
	assocs := e.crawler.GetBranches()
	e.crawler.Forward() // move to next branches
	if assocs != nil {
		e.ignoreRemaningBranch()
	}
}
