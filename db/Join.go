package db

import ()

type PathElement struct {
	Base     *Association
	Derived  *Association
	Inner    bool
	Criteria *Criteria
	Columns  []Tokener
	Orders   []*Order
}

type Join struct {
	associations []*PathElement
	fetch        bool
}

func NewJoin(associations []*PathElement, fetch bool) *Join {
	this := new(Join)
	length := len(associations)
	this.associations = make([]*PathElement, length, length)
	copy(this.associations, associations)
	this.fetch = fetch
	return this
}

func (this Join) IsFetch() bool {
	return this.fetch
}

func (this Join) GetPathElements() []*PathElement {
	return this.associations
}

func (this Join) GetAssociations() []*Association {
	length := len(this.associations)
	derived := make([]*Association, length, length)
	for i, pe := range this.associations {
		derived[i] = pe.Derived
	}
	return derived
}

// From the lists of Foreign Keys (paths) groups, gets the Foreign Keys
// matching the longest common path that is possible to traverse with the
// supplied Foreign Keys
// param cachedAssociation: lists of Foreign Keys (paths) groups
// param associations: matching Foreign Keys (paths) groups
// return Foreign Keys:  matching the longest common path that is possible to traverse
func DeepestCommonPath(cachedAssociation [][]*PathElement, associations []*PathElement) []*PathElement {
	var common []*PathElement

	if associations != nil {
		for _, path := range cachedAssociation {
			// finds the common start portion in this path
			var temp []*PathElement
			for depth := 0; depth < len(path); depth++ {
				pe := path[depth]
				if depth < len(associations) {
					pe2 := associations[depth]
					if pe2.Inner == pe.Inner && pe2.Base != nil && pe2.Base.Equals(pe.Base) {
						temp = append(temp, pe)
					} else {
						break
					}
				} else {
					break
				}
			}
			// if common portion is larger than the previous one, use it
			if len(temp) > len(common) {
				common = temp
			}
		}
	}

	return common
}
