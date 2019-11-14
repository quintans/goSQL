package db

import (
	coll "github.com/quintans/toolkit/collections"
	. "github.com/quintans/toolkit/ext"
)

const FK_NAV_SEP = "."

// entity mapping
var Tables = coll.NewLinkedHashMap()

func AddEntity(table *Table) {
	Tables.Put(Str(table.Alias), table)
}
