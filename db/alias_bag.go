package db

import (
	coll "github.com/quintans/toolkit/collections"

	"strconv"
)

/*
This struct gives the SAME alias when traversing the JOINS

Note: two lists of joins (a path), they have diferent FK alias from the moment they differ
*/
type AliasBag struct {
	prefix  string
	counter int
	bag     coll.Map
}

func NewAliasBag(prefix string) *AliasBag {
	ab := new(AliasBag)
	ab.prefix = prefix
	ab.counter = 0
	ab.bag = coll.NewLinkedHashMap()
	return ab
}

func (a *AliasBag) SetAlias(fk *Association, alias string) {
	a.bag.Put(fk, alias)
}

func (a *AliasBag) GetAlias(fk *Association) string {
	alias, ok := a.bag.Get(fk)
	if !ok {
		a.counter++
		s := a.prefix + strconv.Itoa(a.counter)
		a.bag.Put(fk, s)
		return s
	}

	return alias.(string)
}

func (a *AliasBag) Has(fk *Association) bool {
	_, ok := a.bag.Get(fk)
	return ok
}
