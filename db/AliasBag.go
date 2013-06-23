package db

import (
	coll "github.com/quintans/toolkit/collection"

	"strconv"
)

// This struct gives the SAME alias when traversing the JOINS
// Note: two lists of joins (a path), they have diferent FK alias from the moment they differ
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

func (this *AliasBag) SetAlias(fk *Association, alias string) {
	this.bag.Put(fk, alias)
}

func (this *AliasBag) GetAlias(fk *Association) string {
	alias, ok := this.bag.Get(fk)
	if !ok {
		this.counter++
		a := this.prefix + strconv.Itoa(this.counter)
		this.bag.Put(fk, a)
		return a
	}

	return alias.(string)
}

func (this *AliasBag) Has(fk *Association) bool {
	_, ok := this.bag.Get(fk)
	return ok
}
