package db

import (
	tk "github.com/quintans/toolkit"
	coll "github.com/quintans/toolkit/collections"
	. "github.com/quintans/toolkit/ext"

	"database/sql"
	"reflect"

	"github.com/pkg/errors"
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

func (this *EntityTreeTransformer) BeforeAll() coll.Collection {
	this.crawler = new(Crawler)
	this.crawler.Prepare(this.Query)

	if this.reuse {
		return coll.NewLinkedHashSet()
	}
	return coll.NewArrayList()
}

func (this *EntityTreeTransformer) OnTransformation(result coll.Collection, instance interface{}) {
	this.crawler.Rewind()
	if instance != nil && (!this.reuse || !result.Contains(instance)) {
		result.Add(instance)
	}
}

func (this *EntityTreeTransformer) AfterAll(result coll.Collection) {
	this.crawler.Dispose()
	this.crawler = nil
}

func (this *EntityTreeTransformer) Transform(rows *sql.Rows) (interface{}, error) {
	val := this.Factory()
	instance := val.Interface()

	alias := this.Query.GetTableAlias()
	if this.TemplateData == nil {
		// creates the array with all the types returned by the query
		// using the entity properties as reference for instantiating the types
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		length := len(cols)
		this.TemplateData = make([]interface{}, length, length)
		// set default for unused columns, in case of a projection of a result
		// with more columns than the attributes of the destination struct
		for i := 0; i < len(this.TemplateData); i++ {
			this.TemplateData[i] = &Any{}
		}

		// instanciate all target types
		this.InitFullRowData(this.TemplateData, val.Type(), alias)
		this.crawler.Rewind()
	}
	// makes a copy
	rowData := make([]interface{}, len(this.TemplateData), cap(this.TemplateData))
	copy(rowData, this.TemplateData)

	// Scan result set
	if err := rows.Scan(rowData...); err != nil {
		return nil, err
	}

	instance, err := this.transformEntity(rowData, val, alias)
	if err != nil {
		return nil, err
	}

	if this.Returner == nil {
		if H, isH := instance.(tk.Hasher); isH {
			return H, nil
		}
	} else {
		this.Returner(val)
	}

	return nil, nil
}

func (this *EntityTreeTransformer) InitFullRowData(
	row []interface{},
	typ reflect.Type,
	alias string,
) error {
	lastProps, err := this.getCachedProperties(alias, typ)
	if err != nil {
		return err
	}
	// instanciate all target types for the driving entity
	this.Overrider.InitRowData(row, lastProps)

	fks := this.ForwardBranches()
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

				this.InitFullRowData(row, subType, fkAlias)
			}
		}
	}
	return nil
}

func (this *EntityTreeTransformer) transformEntity(
	row []interface{},
	parent reflect.Value,
	alias string,
) (interface{}, error) {
	var valid bool
	lastProps, err := this.getCachedProperties(alias, parent.Type())
	if err != nil {
		return nil, err
	}
	entity := parent.Interface()
	hasher, isHasher := entity.(tk.Hasher)
	emptyBean := true
	if isHasher && this.reuse {
		// for performance, loads only key, because it's sufficient for searching the cache
		valid, err = this.LoadInstanceKeys(row, parent, lastProps, true)
		if err != nil {
			return nil, err
		} else if valid {
			// searches the cache
			b, _ := this.entities.Get(hasher)
			// if found, use it
			if b != nil {
				hasher = b.(tk.Hasher)
				parent = reflect.ValueOf(hasher)
			} else {
				valid, err = this.LoadInstanceKeys(row, parent, lastProps, false)
				if err != nil {
					return nil, err
				} else if valid {
					this.entities.Put(hasher, hasher)
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
				return nil, errors.Errorf(
					"Key columns not found for %s."+
						" When transforming to a object tree and reusing previous entities, "+
						"the key columns must be declared in the select.",
					parent.Type(),
				)
			}
		}
		emptyBean = false
	} else {
		valid, err = this.Overrider.ToEntity(row, parent, lastProps, &emptyBean)
		if err != nil {
			return nil, err
		}
	}

	if !valid {
		this.ignoreRemaningBranch()
		return nil, nil
	}

	emptyAssoc := true
	fks := this.ForwardBranches()
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
				child, err := this.transformEntity(row, childVal, fkAlias)
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

						if !this.reuse || !tk.SliceContains(sliceV.Interface(), child) {
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

func (this *EntityTreeTransformer) DiscardIfKeyIsNull() bool {
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
func (this *EntityTreeTransformer) LoadInstanceKeys(
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

func (this *EntityTreeTransformer) getCachedProperties(alias string, typ reflect.Type) (map[string]*EntityProperty, error) {
	properties, ok := this.cachedEntityMappings[alias]
	if !ok {
		var err error
		properties, err = this.Overrider.PopulateMapping(alias, typ)
		if err != nil {
			return nil, err
		}
		this.cachedEntityMappings[alias] = properties
	}

	return properties, nil
}

// return the list o current branches and moves forward to the next list
// return: the current list of branches
func (this *EntityTreeTransformer) ForwardBranches() []*Association {
	assocs := this.crawler.GetBranches()
	var list []*Association
	if assocs != nil {
		for _, assoc := range assocs {
			list = append(list, assoc.ForeignKey)
		}
	}
	this.crawler.Forward() // move to next branches
	return list
}

func (this *EntityTreeTransformer) ignoreRemaningBranch() {
	assocs := this.crawler.GetBranches()
	this.crawler.Forward() // move to next branches
	if assocs != nil {
		this.ignoreRemaningBranch()
	}
}
