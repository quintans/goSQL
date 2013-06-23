package db

import (
	coll "github.com/quintans/toolkit/collection"
	. "github.com/quintans/toolkit/ext"

	"strings"
)

const FK_NAV_SEP = "."

// entity mapping
var Tables = coll.NewLinkedHashMap()

func AddEntity(table *Table) {
	Tables.Put(Str(table.Alias), table)
}

func GetLink(chain string) *LinkNav {
	idx := strings.Index(chain, FK_NAV_SEP)
	var link Str
	if idx > 0 {
		link = Str(chain[:idx])
	} else {
		link = Str(chain)
	}
	o, _ := Tables.Get(link)
	table, _ := o.(*Table)

	if idx < 0 {
		return NewLinkNav(nil, table)
	} else if table != nil {
		foreignKeys := coll.NewArrayList()
		return table.GetLink(chain[idx+1:], foreignKeys)
	}

	return nil
}

type LinkNav struct {
	// Association List
	foreignKey coll.Collection
	object     interface{}
}

func NewLinkNav(fks coll.Collection, o interface{}) *LinkNav {
	this := new(LinkNav)
	if fks != nil && fks.Size() != 0 {
		this.foreignKey = fks
	}
	this.object = o
	return this
}

func (this *LinkNav) GetForeignKey() coll.Collection {
	return this.foreignKey
}

func (this *LinkNav) GetObject() interface{} {
	return this.object
}
